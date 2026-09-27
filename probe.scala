// Hudi read-path probe: per-task turnaround, accumulators, HDFS ops, driver CPU. Same script for every Hudi version.
//
//   spark-shell --master yarn --deploy-mode client --jars <hudi-spark-bundle.jar> \
//     --conf spark.dynamicAllocation.enabled=false --num-executors N --executor-cores C \
//     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
//     --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
//     --driver-java-options "-Dprobe.label=hudi-0.14 -Dprobe.table=hdfs://.../T -Dprobe.query=snapshot -Dprobe.iters=3" \
//     -i probe.scala 2>&1 | tee /tmp/hudi-probe-hudi-0.14.driver.log
//
// Knobs (-Dprobe.*): label, table, query=snapshot|incremental|read_optimized, begin/end (incremental instant range),
// cols (comma list; default all), where (SQL predicate), iters (default 3, first is warmup), onefile=true (one file
// per task, default true), opt.<hudi option>=<value> (extra read options), out (default /tmp/hudi-probe-<label>).
// Output: TSV files under <out>; the driver log has one "Starting task ... N bytes" line per task (serialized task size).

import java.lang.management.ManagementFactory
import java.io.{File, FileWriter}
import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.scheduler._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.SparkPlan
import scala.collection.mutable
import scala.collection.JavaConverters._

object Probe {
  def prop(k: String, d: String): String = sys.props.getOrElse("probe." + k, d)
  val label = prop("label", "run")
  val table = prop("table", "")
  val query = prop("query", "snapshot")
  val iters = prop("iters", "3").toInt
  val out = prop("out", s"/tmp/hudi-probe-$label")
  new File(out).mkdirs()

  def append(f: String, header: String, line: String): Unit = {
    val p = new File(out, f); val fresh = !p.exists()
    val w = new FileWriter(p, true)
    try { if (fresh) w.write(header + "\n"); w.write(line + "\n") } finally w.close()
  }
  def med(xs: Seq[Double]): Double = if (xs.isEmpty) Double.NaN else { val s = xs.sorted; val n = s.size; if (n % 2 == 1) s(n / 2) else (s(n / 2 - 1) + s(n / 2)) / 2 }
  def pct(xs: Seq[Double], p: Double): Double = if (xs.isEmpty) Double.NaN else { val s = xs.sorted; s(math.min(s.size - 1, math.floor(p * (s.size - 1) + 0.5).toInt)) }
  def f(d: Double): String = if (d.isNaN) "NaN" else f"$d%.2f"

  case class T(stage: Int, idx: Int, exec: String, launch: Long, finish: Long, gettingResult: Long, run: Long, deser: Long,
               cpuNs: Long, gc: Long, resultSer: Long, resultBytes: Long, accums: Int, shuffleRecs: Long, ok: Boolean) {
    def dur: Long = finish - launch
    def gettingResultMs: Long = if (gettingResult > 0) finish - gettingResult else 0L
    def ovh: Long = dur - run - deser - resultSer - gettingResultMs
  }

  class L extends SparkListener {
    val tasks = mutable.HashMap[Int, mutable.ArrayBuffer[T]]()
    val jobStages = mutable.HashMap[Int, Seq[Int]](); val jobTag = mutable.HashMap[Int, String]()
    val stageWall = mutable.HashMap[Int, (Long, Long)]()
    override def onJobStart(e: SparkListenerJobStart): Unit = synchronized {
      jobTag(e.jobId) = Option(e.properties).flatMap(p => Option(p.getProperty("probe.tag"))).getOrElse("")
      jobStages(e.jobId) = e.stageIds
    }
    override def onStageCompleted(e: SparkListenerStageCompleted): Unit = synchronized {
      stageWall(e.stageInfo.stageId) = (e.stageInfo.submissionTime.getOrElse(-1L), e.stageInfo.completionTime.getOrElse(-1L))
    }
    override def onTaskEnd(e: SparkListenerTaskEnd): Unit = synchronized {
      val i = e.taskInfo; val m = e.taskMetrics
      val t = if (m == null) T(e.stageId, i.index, i.executorId, i.launchTime, i.finishTime, i.gettingResultTime, 0, 0, 0, 0, 0, 0, i.accumulables.size, 0, i.successful)
      else T(e.stageId, i.index, i.executorId, i.launchTime, i.finishTime, i.gettingResultTime, m.executorRunTime, m.executorDeserializeTime,
        m.executorCpuTime, m.jvmGCTime, m.resultSerializationTime, m.resultSize, i.accumulables.size, m.shuffleWriteMetrics.recordsWritten, i.successful)
      tasks.getOrElseUpdate(e.stageId, mutable.ArrayBuffer()) += t
    }
    def stagesFor(tag: String): Seq[Int] = synchronized { jobTag.filter(_._2 == tag).keys.toSeq.flatMap(jobStages).distinct.sorted }
  }

  // driver thread CPU by role
  val tmx = ManagementFactory.getThreadMXBean
  val groups = Seq("sched", "dag", "resultgetter", "listener", "rpc", "main", "exec", "other")
  def group(n: String): String =
    if (n.startsWith("dispatcher-")) "sched" else if (n.startsWith("dag-scheduler")) "dag"
    else if (n.startsWith("task-result-getter")) "resultgetter" else if (n.startsWith("spark-listener-group")) "listener"
    else if (n.startsWith("rpc-") || n.startsWith("netty-") || n.startsWith("shuffle-")) "rpc"
    else if (n == "main") "main" else if (n.startsWith("Executor task launch worker")) "exec" else "other"
  def cpuSnap(): Map[Long, (String, Long)] = {
    val ids = tmx.getAllThreadIds; val infos = tmx.getThreadInfo(ids)
    ids.zip(infos).flatMap { case (id, ti) => if (ti == null) None else { val c = tmx.getThreadCpuTime(id); if (c < 0) None else Some(id -> (ti.getThreadName, c)) } }.toMap
  }
  def cpuDiff(a: Map[Long, (String, Long)], b: Map[Long, (String, Long)]): Map[String, Double] = {
    val acc = mutable.HashMap[String, Double]().withDefaultValue(0.0)
    b.foreach { case (id, (n, c1)) => acc(group(n)) += (c1 - a.get(id).map(_._2).getOrElse(0L)) / 1e6 }
    groups.map(g => g -> acc(g)).toMap
  }
  def gcNow(): (Long, Long) = { val bs = ManagementFactory.getGarbageCollectorMXBeans.asScala; (bs.map(_.getCollectionTime).sum, bs.map(_.getCollectionCount).sum) }

  // per-executor FileSystem op counters (Hadoop global storage statistics: hdfs op_open, op_get_file_status, readOps ...)
  def fsOps(sc: SparkContext, parts: Int): Map[String, Map[String, Long]] =
    sc.parallelize(1 to parts, parts).mapPartitions { _ =>
      val m = mutable.Map[String, Long]()
      val it = org.apache.hadoop.fs.FileSystem.getGlobalStorageStatistics.iterator()
      while (it.hasNext) { val ss = it.next(); val li = ss.getLongStatistics; while (li.hasNext) { val e = li.next(); m(ss.getScheme + ":" + e.getName) = e.getValue } }
      Iterator(SparkEnv.get.executorId -> m.toMap)
    }.collect().toMap

  // listenerBus is private[spark]; reach it reflectively (public at the bytecode level), else settle for a sleep
  def drainBus(sc: SparkContext): Unit = try {
    val bus = sc.getClass.getMethod("listenerBus").invoke(sc)
    bus.getClass.getMethod("waitUntilEmpty", classOf[Long]).invoke(bus, java.lang.Long.valueOf(120000L))
  } catch { case _: Throwable => Thread.sleep(3000) }

  def read(spark: org.apache.spark.sql.SparkSession): DataFrame = {
    var r = spark.read.format("hudi").option("hoodie.datasource.query.type", query)
    sys.props.get("probe.begin").foreach(b => r = r.option("hoodie.datasource.read.begin.instanttime", b))
    sys.props.get("probe.end").foreach(e => r = r.option("hoodie.datasource.read.end.instanttime", e))
    sys.props.filterKeys(_.startsWith("probe.opt.")).foreach { case (k, v) => r = r.option(k.stripPrefix("probe.opt."), v) }
    var df = r.load(table)
    sys.props.get("probe.where").foreach(w => df = df.where(w))
    sys.props.get("probe.cols").foreach(c => df = df.selectExpr(c.split(","): _*))
    df
  }

  def run(spark: org.apache.spark.sql.SparkSession): Unit = {
    val sc = spark.sparkContext
    val l = new L; sc.addSparkListener(l)
    // AQE would materialize the scan stage inside executedPlan/execute() before the timers start; the probe wants one
    // plain scan stage per iteration (set -Dprobe.aqe=true to keep the session default)
    spark.conf.set("spark.sql.adaptive.enabled", prop("aqe", "false"))
    if (prop("onefile", "true").toBoolean) {
      spark.conf.set("spark.sql.files.maxPartitionBytes", (1L << 30).toString); spark.conf.set("spark.sql.files.openCostInBytes", (1L << 30).toString)
    }
    // per-task serialized size shows up in the driver log at INFO on this logger (log4j2 in Spark 3.3+)
    try {
      val lvl = Class.forName("org.apache.logging.log4j.Level"); val cfg = Class.forName("org.apache.logging.log4j.core.config.Configurator")
      cfg.getMethod("setLevel", classOf[String], lvl).invoke(null, "org.apache.spark.scheduler.TaskSetManager", lvl.getField("INFO").get(null))
    } catch { case e: Throwable => println("TaskSetManager INFO not enabled: " + e) }
    val execs = math.max(1, sc.getExecutorMemoryStatus.size - 1)
    val fsParts = execs * math.max(2, sc.getConf.getInt("spark.executor.cores", 2)) * 3
    println(s"PROBE label=$label table=$table query=$query iters=$iters executors=$execs spark=${sc.version} out=$out")
    for (i <- 0 until iters) {
      val tag = s"$label-$i"; sc.setLocalProperty("probe.tag", tag)
      val df = read(spark).selectExpr("count(1)", "sum(hash(*))".replace("hash(*)", if (sys.props.contains("probe.nohash")) "1" else "hash(*)"))
      val plan = df.queryExecution.executedPlan
      if (i == 0) {
        val w = new FileWriter(new File(out, "plan.txt")); try w.write(df.queryExecution.toString) finally w.close()
        plan.collect { case p => p }.foreach(p => append("planmetrics.tsv", "label\tnode\tn_metrics\tmetrics", Seq(label, p.nodeName, p.metrics.size, p.metrics.keys.toSeq.sorted.mkString(",")).mkString("\t")))
        append("planmetrics.tsv", "", Seq(label, "TOTAL", plan.collect { case p => p.metrics.size }.sum, "").mkString("\t"))
        // Hudi-controlled part of every serialized task: the scan RDD's partitions (file split payload), found by
        // walking the lineage of the query's RDD down to the file scan
        try {
          val seen = mutable.LinkedHashSet[org.apache.spark.rdd.RDD[_]]()
          def walk(r: org.apache.spark.rdd.RDD[_]): Unit = if (seen.add(r)) r.dependencies.foreach(d => walk(d.rdd))
          walk(plan.execute())
          val scanRdd = seen.find(r => r.getClass.getSimpleName.contains("FileScanRDD") || r.getClass.getName.contains("Hoodie")).getOrElse(seen.last)
          val ser = SparkEnv.get.closureSerializer.newInstance()
          val sizes = scanRdd.partitions.map(p => ser.serialize(p).limit().toDouble)
          append("partsize.tsv", "label\tscan_rdd\tpartitions\tpart_bytes_med\tpart_bytes_max", Seq(label, scanRdd.getClass.getSimpleName, sizes.length, f(med(sizes)), sizes.max).mkString("\t"))
        } catch { case e: Throwable => println("partition size: " + e) }
      }
      val fs0 = fsOps(sc, fsParts); val c0 = cpuSnap(); val g0 = gcNow(); val w0 = System.currentTimeMillis()
      val res = df.collect()
      val w1 = System.currentTimeMillis(); drainBus(sc); val c1 = cpuSnap(); val g1 = gcNow()
      val fs1 = fsOps(sc, fsParts)
      sc.setLocalProperty("probe.tag", null)
      val stages = l.stagesFor(tag)
      val all = l.synchronized { stages.flatMap(s => l.tasks.getOrElse(s, Nil)).filter(_.ok) }
      val scan = l.synchronized { stages.find(s => l.tasks.get(s).exists(_.exists(_.shuffleRecs > 0))).getOrElse(-1) }
      val ts = all.filter(_.stage == scan)
      ts.foreach(t => append("tasks.tsv", "label\titer\tstage\tindex\texecutor\tdur_ms\trun_ms\tdeser_ms\tcpu_ms\tgc_ms\tresult_ser_ms\tgetting_result_ms\tovh_ms\tresult_bytes\taccumulables",
        Seq(label, i, t.stage, t.idx, t.exec, t.dur, t.run, t.deser, f(t.cpuNs / 1e6), t.gc, t.resultSer, t.gettingResultMs, t.ovh, t.resultBytes, t.accums).mkString("\t")))
      val cpu = cpuDiff(c0, c1)
      val (ss, se) = l.stageWall.getOrElse(scan, (-1L, -1L))
      val ops = mutable.LinkedHashMap[String, Long]()
      for ((e, m1) <- fs1; m0 <- fs0.get(e); (k, v1) <- m1) ops(k) = ops.getOrElse(k, 0L) + (v1 - m0.getOrElse(k, 0L))
      val nfiles = plan.collectFirst { case s: org.apache.spark.sql.execution.FileSourceScanExec => s.metrics.get("numFiles").map(_.value) }.flatten.getOrElse(-1L)
      ops.filter(_._2 != 0).foreach { case (k, v) => append("fsops.tsv", "label\titer\tcounter\tdelta\tper_task\tper_file\texecutors_sampled",
        Seq(label, i, k, v, f(v.toDouble / math.max(1, ts.size)), if (nfiles > 0) f(v.toDouble / nfiles) else "NaN", fs1.keySet.intersect(fs0.keySet).size).mkString("\t")) }
      def m(g: T => Double) = f(med(ts.map(g))); def s(g: T => Double) = f(ts.map(g).sum)
      append("summary.tsv", ("label\titer\twall_ms\tscan_stage\tntasks\tstage_wall_ms\ttasks_per_s\tnum_files\tdur_med\trun_med\tdeser_med\tcpu_med\tresult_ser_med\tgetting_result_med\tovh_med\tovh_p90\tovh_p99\tovh_sum\trun_sum\tdeser_sum\tresult_bytes_med\taccums_med\t" +
        groups.map(g => s"drv_cpu_${g}_ms").mkString("\t") + "\tdrv_gc_ms\tdrv_gc_count\tresult"),
        (Seq(label, i, w1 - w0, scan, ts.size, se - ss, f(ts.size * 1000.0 / math.max(1, se - ss)), nfiles, m(_.dur), m(_.run), m(_.deser), m(_.cpuNs / 1e6), m(_.resultSer), m(_.gettingResultMs),
          m(_.ovh), f(pct(ts.map(_.ovh.toDouble), 0.9)), f(pct(ts.map(_.ovh.toDouble), 0.99)), s(_.ovh), s(_.run), s(_.deser), m(_.resultBytes), m(_.accums)) ++
          groups.map(g => f(cpu(g))) ++ Seq(g1._1 - g0._1, g1._2 - g0._2, res.mkString(";"))).mkString("\t"))
      println(s"ITER $i wall=${w1 - w0} ms tasks=${ts.size} ovh_med=${m(_.ovh)} run_med=${m(_.run)} deser_med=${m(_.deser)} accums=${m(_.accums)} drvCpu=${groups.map(g => g + "=" + f(cpu(g))).mkString(",")}")
      l.synchronized { stages.foreach(l.tasks.remove) }
    }
    println(s"PROBE done: $out")
  }
}
Probe.run(spark)
System.exit(0)
