// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! A process-wide audit of object store requests, for benchmarking only.
//!
//! When the `COMET_IO_AUDIT` environment variable is set, every store built by
//! `prepare_object_store_with_configs` is wrapped so that each request is
//! tallied by operation, range size and table. A background thread prints the
//! tallies accumulated in the last second to stderr as `IOAUDIT` lines, and
//! every request for a fixed sample of paths is printed as an `IOAUDIT_REQ`
//! line. Without the variable no store is wrapped and nothing here runs.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result as ObjectStoreResult,
};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Default, Clone)]
struct Tally {
    calls: u64,
    errors: u64,
    bytes: u64,
    items: u64,
    first_byte_micros: u64,
    total_micros: u64,
}

struct Audit {
    tallies: Mutex<HashMap<(String, String), Tally>>,
}

fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("COMET_IO_AUDIT")
            .map(|v| !v.is_empty() && v != "0")
            .unwrap_or(false)
    })
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn audit() -> &'static Audit {
    static AUDIT: OnceLock<Audit> = OnceLock::new();
    AUDIT.get_or_init(|| {
        eprintln!(
            "IOAUDIT_START ts_ms={} pid={}",
            now_millis(),
            std::process::id()
        );
        let _ = std::thread::Builder::new()
            .name("comet-io-audit".to_string())
            .spawn(|| loop {
                std::thread::sleep(Duration::from_secs(1));
                let drained: Vec<((String, String), Tally)> = {
                    let mut tallies = audit()
                        .tallies
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    tallies.drain().collect()
                };
                if drained.is_empty() {
                    continue;
                }
                let ts = now_millis();
                let pid = std::process::id();
                let mut out = String::new();
                for ((op, key), t) in drained {
                    out.push_str(&format!(
                        "IOAUDIT ts_ms={ts} pid={pid} op={op} key={key} calls={} errs={} bytes={} items={} fb_us={} tot_us={}\n",
                        t.calls, t.errors, t.bytes, t.items, t.first_byte_micros, t.total_micros
                    ));
                }
                eprint!("{out}");
            });
        Audit {
            tallies: Mutex::new(HashMap::new()),
        }
    })
}

/// Wraps `store` when the audit is enabled, and returns it unchanged otherwise.
pub(crate) fn wrap(store: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    if enabled() {
        Arc::new(IoAuditStore { inner: store })
    } else {
        store
    }
}

/// `<table>/<class>` for a path: the table is the directory above the first partition
/// directory, the `.hoodie` directory or the file name, and the class is `hoodie`, `parquet`
/// or `other`.
fn classify(path: &str) -> String {
    let parts: Vec<&str> = path.split('/').collect();
    let mut index = parts.len().saturating_sub(1);
    for (i, part) in parts.iter().enumerate() {
        if *part == ".hoodie" || part.contains('=') {
            index = i;
            break;
        }
    }
    let table = if index > 0 { parts[index - 1] } else { "" };
    let class = if path.contains("/.hoodie/") || path.ends_with("/.hoodie") {
        "hoodie"
    } else if path.ends_with(".parquet") {
        "parquet"
    } else {
        "other"
    };
    format!("{table}/{class}")
}

fn size_bucket(bytes: u64) -> &'static str {
    if bytes <= 64 * 1024 {
        "le64k"
    } else if bytes <= 1024 * 1024 {
        "le1m"
    } else if bytes <= 8 * 1024 * 1024 {
        "le8m"
    } else {
        "gt8m"
    }
}

/// Whether every request for `path` is printed. The path hash is the same in every process,
/// so all executors and all runs sample the same data files.
fn sampled(path: &str, key: &str) -> bool {
    if key.ends_with("/hoodie") {
        static LEFT: AtomicI64 = AtomicI64::new(400);
        return LEFT.fetch_sub(1, Ordering::Relaxed) > 0;
    }
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    hasher.finish() % 50 == 0
}

/// One request in flight. It is recorded when its response stream ends or is dropped.
struct Pending {
    op: String,
    key: String,
    path: Option<String>,
    range_start: u64,
    range_end: u64,
    started: Instant,
    first_byte: Duration,
    bytes: u64,
    items: u64,
}

impl Pending {
    fn new(op: String, path: &str) -> Self {
        let key = classify(path);
        let detail = if sampled(path, &key) {
            Some(path.to_string())
        } else {
            None
        };
        Self {
            op,
            key,
            path: detail,
            range_start: 0,
            range_end: 0,
            started: Instant::now(),
            first_byte: Duration::ZERO,
            bytes: 0,
            items: 0,
        }
    }

    fn finish(self, ok: bool) {
        let total = self.started.elapsed();
        {
            let mut tallies = audit()
                .tallies
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let tally = tallies.entry((self.op.clone(), self.key.clone())).or_default();
            tally.calls += 1;
            if !ok {
                tally.errors += 1;
            }
            tally.bytes += self.bytes;
            tally.items += self.items;
            tally.first_byte_micros += self.first_byte.as_micros() as u64;
            tally.total_micros += total.as_micros() as u64;
        }
        if let Some(path) = self.path {
            eprintln!(
                "IOAUDIT_REQ ts_ms={} pid={} op={} path={} start={} end={} bytes={} items={} fb_us={} tot_us={} ok={}",
                now_millis(),
                std::process::id(),
                self.op,
                path,
                self.range_start,
                self.range_end,
                self.bytes,
                self.items,
                self.first_byte.as_micros(),
                total.as_micros(),
                ok
            );
        }
    }
}

fn measure_bytes(bytes: &Bytes) -> (u64, u64) {
    (bytes.len() as u64, 0)
}

fn measure_meta(_meta: &ObjectMeta) -> (u64, u64) {
    (0, 1)
}

/// A response stream that records its request when it ends, fails or is dropped.
struct Timed<T> {
    inner: BoxStream<'static, ObjectStoreResult<T>>,
    measure: fn(&T) -> (u64, u64),
    pending: Option<Pending>,
}

impl<T> Stream for Timed<T> {
    type Item = ObjectStoreResult<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let polled = this.inner.poll_next_unpin(cx);
        match &polled {
            Poll::Ready(Some(Ok(item))) => {
                let (bytes, items) = (this.measure)(item);
                if let Some(pending) = this.pending.as_mut() {
                    pending.bytes += bytes;
                    pending.items += items;
                }
            }
            Poll::Ready(Some(Err(_))) => {
                if let Some(pending) = this.pending.take() {
                    pending.finish(false);
                }
            }
            Poll::Ready(None) => {
                if let Some(pending) = this.pending.take() {
                    pending.finish(true);
                }
            }
            Poll::Pending => {}
        }
        polled
    }
}

impl<T> Drop for Timed<T> {
    fn drop(&mut self) {
        if let Some(pending) = self.pending.take() {
            pending.finish(true);
        }
    }
}

#[derive(Debug)]
struct IoAuditStore {
    inner: Arc<dyn ObjectStore>,
}

impl Display for IoAuditStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "io-audit({})", self.inner)
    }
}

fn prefix_str(prefix: Option<&Path>) -> String {
    prefix
        .map(|p| {
            let raw: &str = p.as_ref();
            raw.to_string()
        })
        .unwrap_or_default()
}

#[async_trait]
impl ObjectStore for IoAuditStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let pending = Pending::new("PUT".to_string(), location.as_ref());
        let result = self.inner.put_opts(location, payload, options).await;
        pending.finish(result.is_ok());
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    // `get_ranges` keeps the trait's default, which coalesces the ranges and fetches each
    // through `get_opts` here, as the S3 store does, so every HTTP GET is seen.
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        let head = options.head;
        let kind = if head {
            "HEAD"
        } else {
            match &options.range {
                Some(GetRange::Bounded(_)) => "GET_RANGE",
                Some(GetRange::Suffix(_)) => "GET_SUFFIX",
                Some(GetRange::Offset(_)) => "GET_OFFSET",
                None => "GET_FULL",
            }
        };
        let mut pending = Pending::new(kind.to_string(), location.as_ref());
        let result = self.inner.get_opts(location, options).await;
        pending.first_byte = pending.started.elapsed();
        let result = match result {
            Ok(result) => result,
            Err(e) => {
                pending.finish(false);
                return Err(e);
            }
        };
        pending.range_start = result.range.start;
        pending.range_end = result.range.end;
        if head {
            pending.finish(true);
            return Ok(result);
        }
        let size = result.range.end - result.range.start;
        pending.op = format!("{kind}:{}", size_bucket(size));
        let GetResult {
            payload,
            meta,
            range,
            attributes,
        } = result;
        let payload = match payload {
            GetResultPayload::Stream(stream) => GetResultPayload::Stream(Box::pin(Timed {
                inner: stream,
                measure: measure_bytes,
                pending: Some(pending),
            })),
            other => {
                pending.bytes = size;
                pending.finish(true);
                other
            }
        };
        Ok(GetResult {
            payload,
            meta,
            range,
            attributes,
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let pending = Pending::new("LIST".to_string(), &prefix_str(prefix));
        Box::pin(Timed {
            inner: self.inner.list(prefix),
            measure: measure_meta,
            pending: Some(pending),
        })
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let pending = Pending::new("LIST_OFFSET".to_string(), &prefix_str(prefix));
        Box::pin(Timed {
            inner: self.inner.list_with_offset(prefix, offset),
            measure: measure_meta,
            pending: Some(pending),
        })
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        let mut pending = Pending::new("LIST_DELIM".to_string(), &prefix_str(prefix));
        let result = self.inner.list_with_delimiter(prefix).await;
        pending.first_byte = pending.started.elapsed();
        if let Ok(listing) = &result {
            pending.items = (listing.objects.len() + listing.common_prefixes.len()) as u64;
        }
        pending.finish(result.is_ok());
        result
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
