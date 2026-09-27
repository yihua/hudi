# Hudi 0.14 vs 1.2 read-path probe kit

Three pieces, all version-neutral (run the same thing against the 0.14 build and the 1.2 build):

1. `probe.scala` - a `spark-shell` script that scans one Hudi table on the cluster and writes, per task and per
   query: the driver-stamped turnaround overhead (`ovh`), run/deserialize/CPU/result-serialization time,
   accumulators and result bytes per task, HDFS operation counts per file (from the executors' Hadoop
   `StorageStatistics`), the driver's CPU by scheduler thread role, driver GC, SQL metrics per plan node, the
   physical plan (with `ReadSchema`, for the column-count question), and the serialized scan partition size.
2. `analyze_eventlog.py` - the same `ovh` definition applied to a whole application's event log, per stage, plus
   stage launch throughput (tasks/s), accumulators and result bytes per task, off-CPU time, and the driver's GC
   counters from the stage executor metrics.
3. Driver metrics recipe (below) - zero-code confs that make the driver report its own scheduler / listener-bus /
   GC health over time, so the two runs can be compared where the event log is blind.

## 1. probe.scala

```bash
# same command for both builds; only the bundle jar and -Dprobe.label change
spark-shell --master yarn --deploy-mode client \
  --jars /path/hudi-spark3.3-bundle_2.12-<version>.jar \
  --num-executors 20 --executor-cores 4 --executor-memory 8g --driver-memory 8g \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --driver-java-options "-Dprobe.label=hudi-0.14 -Dprobe.table=hdfs://nn/warehouse/T -Dprobe.query=snapshot -Dprobe.iters=4" \
  -i probe.scala 2>&1 | tee /tmp/hudi-probe-hudi-0.14.driver.log
```

Knobs (`-Dprobe.*`):

| knob | meaning | default |
|---|---|---|
| `label` | output label; results go to `/tmp/hudi-probe-<label>/` | `run` |
| `table` | table base path | required |
| `query` | `snapshot`, `incremental`, `read_optimized` | `snapshot` |
| `begin`, `end` | incremental window (`hoodie.datasource.read.begin/end.instanttime`) | none |
| `cols` | comma list of columns to project (e.g. the 60 the ETL reads); default all | all |
| `where` | SQL predicate to push down | none |
| `iters` | iterations; iteration 0 is the warm-up | 3 |
| `onefile` | one file per task (`maxPartitionBytes=1g`), so per-task = per-file | `true` |
| `opt.<key>=<v>` | any extra Hudi read option, e.g. `-Dprobe.opt.hoodie.metadata.enable=true` | none |
| `nohash` | aggregate `count(1)` only instead of `sum(hash(*))` (no decode cost) | off |
| `aqe` | `spark.sql.adaptive.enabled` for the probe session; off by default so each iteration is one plain scan stage and the plan is inspectable | `false` |

Checked end to end on a local Spark 3.2.2 with the 0.14.1 Spark 3.2 bundle and compiled against Spark 3.4; the
Spark APIs used are the same on 3.3. On Spark 3.2 (log4j 1) the `Starting task` INFO lines are not switched on
automatically; add `log4j.logger.org.apache.spark.scheduler.TaskSetManager=INFO` to the driver log config instead.

Use the same executor count and cores for both runs, and a table that is not being written to. Run the two
builds back to back on the same queue (the queue and driver JVM settings are confounders for `ovh`).

Outputs in `/tmp/hudi-probe-<label>/`:

- `summary.tsv` - one row per iteration: `ntasks`, `stage_wall_ms`, `tasks_per_s` (driver launch throughput),
  `ovh_med/p90/p99/sum`, `run_med`, `deser_med`, `cpu_med`, `result_bytes_med`, `accums_med`,
  `drv_cpu_{sched,dag,resultgetter,listener,rpc,main,other}_ms` (driver thread CPU during the query),
  `drv_gc_ms`, `drv_gc_count`.
- `tasks.tsv` - every scan task with its `ovh_ms` decomposition inputs.
- `fsops.tsv` - HDFS client op deltas for the query (`hdfs:op_open`, `hdfs:op_get_file_status`,
  `hdfs:op_list_status`, `hdfs:readOps`, `hdfs:bytesRead`, ...) with per-task and per-file rates. Sampled from the
  executors reached by a small collect job; `executors_sampled` says how many of them were seen in both samples.
- `planmetrics.tsv` - SQL metrics (accumulators) registered by each physical node, and the total.
- `plan.txt` - full `queryExecution` string: compare `ReadSchema` between builds for the column-pruning question.
- `partsize.tsv` - serialized bytes of one scan partition (the Hudi-controlled part of every task's payload).
- The driver log (`tee`) has one `Starting task ... (..., N bytes)` line per task from `TaskSetManager` at INFO,
  i.e. the whole serialized task the driver ships. `grep -o '[0-9]* bytes)' driver.log | sort -n | uniq -c`.

What to compare: `ovh_med` / `ovh_sum` and `tasks_per_s` at equal `ntasks` (lead 1); `accums_med`,
`result_bytes_med`, `planmetrics.tsv` (lead 2); `fsops.tsv` per file and `run_med - cpu_med` (lead 3);
`deser_med` (lead 4); `ReadSchema` in `plan.txt` and `numOutputRows` on the scan node (lead 7).

## 2. analyze_eventlog.py

```bash
python3 analyze_eventlog.py /path/to/eventlog-0.14 --csv stages-0.14.csv
python3 analyze_eventlog.py /path/to/eventlog-1.2  --csv stages-1.2.csv
```

Plain or `.gz` event logs (decompress `.lz4` / `.zstd` first, or run with `spark.eventLog.compress=false`).
Prints app totals (`ovh` sum / median / p99, accumulables and result bytes per task, peak launch throughput,
driver GC and heap peak) and the per-stage table sorted by `ovh_sum_s`, tagged with the scan RDD scope
(`Scan parquet`, `Scan HudiFileGroup`, `IncrementalRelation`, ...). Pair stages across the two CSVs by
`ntasks` + `scan`.

The interesting driver-side signal is `tasks_per_s` on the largest stages: if the 1.2 run's peak launch
throughput is a fraction of 0.14's while `run_med` is equal or lower, the driver (or its RPC / listener path) is
the ceiling, not the executors.

## 3. Driver metrics recipe (add to both runs)

```bash
--conf spark.metrics.conf.driver.sink.csv.class=org.apache.spark.metrics.sink.CsvSink \
--conf spark.metrics.conf.driver.sink.csv.period=10 \
--conf spark.metrics.conf.driver.sink.csv.unit=seconds \
--conf spark.metrics.conf.driver.sink.csv.directory=/tmp/spark-driver-metrics-<label> \
--conf spark.metrics.conf.driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.scheduler.listenerbus.metrics.maxListenerClassesTimed=128 \
--driver-java-options "-Xlog:gc*:file=/tmp/driver-gc-<label>.log:time,uptime"   # JDK 11+; JDK 8: -XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:/tmp/driver-gc-<label>.log
```

Files worth diffing between the two runs (10 s samples, driver only):

- `<app>.driver.DAGScheduler.messageProcessingTime.csv` - time the DAG scheduler event loop spends per event.
- `<app>.driver.LiveListenerBus.queue.appStatus.size.csv`, `...queue.executorManagement.size.csv`,
  `...queue.eventLog.size.csv`, `...numDroppedEvents.csv` - listener bus backlog and drops.
- `<app>.driver.LiveListenerBus.listenerProcessingTime.org.apache.spark.sql.execution.ui.SQLAppStatusListener.csv`
  and the other listeners - who is slow on the bus.
- `<app>.driver.jvm.G1-Young-Generation.time.csv`, `...G1-Old-Generation.time.csv`, `...heap.used.csv` - driver GC.
- Driver log: `grep -c 'Dropped .* events' driver.log` (listener-bus overflow), `grep -c 'Starting task' driver.log`.

Also record the two job configs side by side: `spark.driver.memory`, `spark.driver.cores`,
`spark.driver.extraJavaOptions`, `spark.scheduler.listenerbus.eventqueue.capacity`, `spark.rpc.*`,
`spark.locality.wait`, `spark.sql.shuffle.partitions`, `spark.speculation`, and the YARN queue.
