/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/**
 * The local file system with a recorder for accesses to paths under a table's {@code .hoodie}
 * folder, split by whether the access ran inside an engine task (an executor) or anywhere else
 * (the driver, or a test thread).
 *
 * <p>Reading a table should resolve everything that lives under {@code .hoodie} (table config,
 * timeline, schema history) once on the driver. An executor that lists or opens {@code .hoodie}
 * files does so once per task, which on a cloud store is a request per task per file group. This
 * file system lets a functional test assert that no such access happens.
 *
 * <p>Usage: {@link #register} it on the Hadoop configuration the engine hands to its readers, set
 * how to tell an engine task apart with {@link #setTaskScope}, then {@link #reset}, run the read,
 * and inspect {@link #getTaskAccesses()}. {@link #getNonTaskAccessCount()} being non-zero after a
 * read proves the hook is live: every read opens {@code hoodie.properties} on the driver.
 *
 * <p>A test that expects legitimate task reads under {@code .hoodie}, such as distributed reads of
 * metadata table data files, names them with {@link #setAllowedTaskAccess}; those are kept apart in
 * {@link #getAllowedTaskAccesses()} instead of counting as violations.
 *
 * <p>The recorder state is static because Hadoop creates a new instance per
 * {@code FileSystem.get} call once caching is disabled, which {@link #register} does so that the
 * implementation is picked up regardless of what the file system cache already holds.
 */
public class MetaFolderAccessRecordingFileSystem extends RawLocalFileSystem {

  private static final String META_FOLDER = "/.hoodie";
  private static final String METADATA_TABLE_FOLDER = META_FOLDER + "/metadata";
  private static final int MAX_RECORDED_STACKS = 3;
  private static final String[] REGISTERED_KEYS = {"fs.file.impl", "fs.file.impl.disable.cache"};

  private static final Map<Configuration, Map<String, String>> PRIOR_VALUES = new WeakHashMap<>();
  private static final Queue<Access> TASK_ACCESSES = new ConcurrentLinkedQueue<>();
  private static final Queue<Access> ALLOWED_TASK_ACCESSES = new ConcurrentLinkedQueue<>();
  private static final AtomicLong NON_TASK_ACCESS_COUNT = new AtomicLong();
  private static final AtomicInteger RECORDED_STACKS = new AtomicInteger();
  private static final ThreadLocal<Boolean> IN_RECORDED_CALL = ThreadLocal.withInitial(() -> false);
  private static volatile BooleanSupplier taskScope = () -> false;
  private static volatile Predicate<String> allowedTaskAccess = path -> false;

  /**
   * One access to a {@code .hoodie} path from inside an engine task.
   */
  public static final class Access {
    private final String operation;
    private final String path;
    private final String threadName;
    private final Throwable stack;

    private Access(String operation, String path, String threadName, Throwable stack) {
      this.operation = operation;
      this.path = path;
      this.threadName = threadName;
      this.stack = stack;
    }

    public String getOperation() {
      return operation;
    }

    public String getPath() {
      return path;
    }

    public String getThreadName() {
      return threadName;
    }

    /**
     * The call stack of the access, kept for the first few accesses only.
     */
    public Throwable getStack() {
      return stack;
    }

    @Override
    public String toString() {
      return operation + " " + path + " [" + threadName + "]";
    }
  }

  /**
   * Makes the {@code file} scheme resolve to this file system on the given configuration, keeping
   * the values it replaces for {@link #unregister}.
   */
  public static void register(Configuration conf) {
    synchronized (PRIOR_VALUES) {
      if (!PRIOR_VALUES.containsKey(conf)) {
        Map<String, String> prior = new HashMap<>();
        for (String key : REGISTERED_KEYS) {
          prior.put(key, conf.getRaw(key));
        }
        PRIOR_VALUES.put(conf, prior);
      }
    }
    conf.setClass("fs.file.impl", MetaFolderAccessRecordingFileSystem.class, FileSystem.class);
    conf.setBoolean("fs.file.impl.disable.cache", true);
  }

  /**
   * Reverts {@link #register}, restoring the values it replaced.
   */
  public static void unregister(Configuration conf) {
    Map<String, String> prior;
    synchronized (PRIOR_VALUES) {
      prior = PRIOR_VALUES.remove(conf);
    }
    for (String key : REGISTERED_KEYS) {
      String value = prior == null ? null : prior.get(key);
      if (value == null) {
        conf.unset(key);
      } else {
        conf.set(key, value);
      }
    }
  }

  /**
   * Sets how to tell whether the current thread runs inside an engine task.
   */
  public static void setTaskScope(BooleanSupplier inTask) {
    taskScope = inTask;
  }

  /**
   * Sets which task accesses under {@code .hoodie} are legitimate, by path. They are recorded in
   * {@link #getAllowedTaskAccesses()} rather than {@link #getTaskAccesses()}.
   */
  public static void setAllowedTaskAccess(Predicate<String> isAllowedPath) {
    allowedTaskAccess = isAllowedPath;
  }

  /**
   * Whether the path is a data file or partition folder of a metadata table, which executors may
   * read in distributed metadata table lookups, as opposed to the metadata table's own
   * {@code .hoodie} folder.
   */
  public static boolean isMetadataTableDataPath(String path) {
    String str = new Path(path).toUri().getPath();
    int index = str.indexOf(METADATA_TABLE_FOLDER + "/");
    return index >= 0 && !isMetaFolderPath(new Path(str.substring(index + METADATA_TABLE_FOLDER.length())));
  }

  public static void reset() {
    TASK_ACCESSES.clear();
    ALLOWED_TASK_ACCESSES.clear();
    NON_TASK_ACCESS_COUNT.set(0);
    RECORDED_STACKS.set(0);
  }

  public static List<Access> getTaskAccesses() {
    return new ArrayList<>(TASK_ACCESSES);
  }

  public static List<Access> getAllowedTaskAccesses() {
    return new ArrayList<>(ALLOWED_TASK_ACCESSES);
  }

  public static long getNonTaskAccessCount() {
    return NON_TASK_ACCESS_COUNT.get();
  }

  /**
   * Describes the recorded task accesses: every distinct operation and path, then the stacks kept.
   */
  public static String describeTaskAccesses() {
    List<Access> accesses = getTaskAccesses();
    StringBuilder sb = new StringBuilder();
    sb.append(accesses.size()).append(" access(es) to .hoodie from inside engine tasks:\n");
    accesses.stream()
        .map(a -> a.getOperation() + " " + a.getPath())
        .distinct()
        .forEach(line -> sb.append("  ").append(line).append('\n'));
    for (Access access : accesses) {
      if (access.getStack() != null) {
        StringWriter writer = new StringWriter();
        access.getStack().printStackTrace(new PrintWriter(writer));
        sb.append("Stack of ").append(access).append(":\n").append(writer);
      }
    }
    return sb.toString();
  }

  static boolean isMetaFolderPath(Path path) {
    if (path == null) {
      return false;
    }
    String str = path.toUri().getPath();
    return str.contains(META_FOLDER + "/") || str.endsWith(META_FOLDER);
  }

  /**
   * Runs {@code call}, recording it when it is the outermost recorded call on this thread and
   * targets a {@code .hoodie} path. Nested calls (for example {@code exists} delegating to
   * {@code getFileStatus}) are not recorded twice.
   */
  private static <T> T record(String operation, Path path, IOCall<T> call) throws IOException {
    if (IN_RECORDED_CALL.get()) {
      return call.run();
    }
    if (isMetaFolderPath(path)) {
      if (taskScope.getAsBoolean()) {
        if (allowedTaskAccess.test(path.toString())) {
          ALLOWED_TASK_ACCESSES.add(new Access(operation, path.toString(), Thread.currentThread().getName(), null));
        } else {
          Throwable stack = RECORDED_STACKS.getAndIncrement() < MAX_RECORDED_STACKS
              ? new Throwable(operation + " " + path) : null;
          TASK_ACCESSES.add(new Access(operation, path.toString(), Thread.currentThread().getName(), stack));
        }
      } else {
        NON_TASK_ACCESS_COUNT.incrementAndGet();
      }
    }
    IN_RECORDED_CALL.set(true);
    try {
      return call.run();
    } finally {
      IN_RECORDED_CALL.set(false);
    }
  }

  @FunctionalInterface
  private interface IOCall<T> {
    T run() throws IOException;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return record("open", f, () -> super.open(f, bufferSize));
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    return record("listStatus", f, () -> super.listStatus(f));
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return record("getFileStatus", f, () -> super.getFileStatus(f));
  }

  @Override
  public boolean exists(Path f) throws IOException {
    return record("exists", f, () -> super.exists(f));
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws IOException {
    return record("listLocatedStatus", f, () -> super.listLocatedStatus(f));
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path f) throws IOException {
    return record("listStatusIterator", f, () -> super.listStatusIterator(f));
  }
}
