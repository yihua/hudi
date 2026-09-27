#!/usr/bin/env python3
"""Per-stage task turnaround analysis of a Spark event log (plain or .gz; decompress lz4/zstd first).

ovh = task duration - executorRunTime - executorDeserializeTime - resultSerializationTime - gettingResultTime,
i.e. the time a task holds a slot with nothing the executor accounts for (driver launch / completion handling,
RPC, result deserialization). Also per-stage launch throughput, accumulators and result bytes per task, the scan
node names (RDD scopes), and the driver's GC counters from stage executor metrics.

  python3 analyze_eventlog.py <eventlog> [--top 40] [--min-tasks 100] [--csv out.csv]
Run it on the 0.14 and the 1.2 event logs and diff the tables (stage names / task counts pair the stages).
"""
import argparse, gzip, json, statistics, sys
from collections import defaultdict


def opn(path):
    return gzip.open(path, "rt") if path.endswith(".gz") else open(path, "r")


def pctile(xs, p):
    if not xs:
        return float("nan")
    s = sorted(xs)
    return s[min(len(s) - 1, int(round(p * (len(s) - 1))))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("log")
    ap.add_argument("--top", type=int, default=40, help="stages to print, by ovh sum")
    ap.add_argument("--min-tasks", type=int, default=1)
    ap.add_argument("--csv", help="write the per-stage table here")
    a = ap.parse_args()

    stages = defaultdict(lambda: {"tasks": [], "name": "", "scopes": set(), "sub": None, "done": None, "ntasks": 0})
    driver_gc = []  # (stage, minorGCTime, majorGCTime, heap) peaks of the driver per stage
    exec_cores = {}
    dropped = 0
    app_start = app_end = None
    for line in opn(a.log):
        try:
            e = json.loads(line)
        except Exception:
            continue
        ev = e.get("Event")
        if ev == "SparkListenerApplicationStart":
            app_start = e.get("Timestamp")
        elif ev == "SparkListenerApplicationEnd":
            app_end = e.get("Timestamp")
        elif ev == "SparkListenerExecutorAdded":
            exec_cores[e["Executor ID"]] = e["Executor Info"].get("Total Cores", 0)
        elif ev == "SparkListenerStageSubmitted":
            si = e["Stage Info"]
            st = stages[(si["Stage ID"], si["Stage Attempt ID"])]
            st["name"] = si.get("Stage Name", "")
            st["ntasks"] = si.get("Number of Tasks", 0)
            for r in si.get("RDD Info", []):
                sc = r.get("Scope")
                if sc:
                    try:
                        st["scopes"].add(json.loads(sc).get("name", ""))
                    except Exception:
                        pass
        elif ev == "SparkListenerStageCompleted":
            si = e["Stage Info"]
            st = stages[(si["Stage ID"], si["Stage Attempt ID"])]
            st["sub"] = si.get("Submission Time")
            st["done"] = si.get("Completion Time")
        elif ev == "SparkListenerStageExecutorMetrics":
            if e.get("Executor ID") == "driver":
                m = e.get("Executor Metrics", {})
                driver_gc.append((e["Stage ID"], m.get("MinorGCTime", 0), m.get("MajorGCTime", 0), m.get("JVMHeapMemory", 0)))
        elif ev == "SparkListenerTaskEnd":
            ti = e["Task Info"]
            if ti.get("Failed") or ti.get("Killed"):
                continue
            tm = e.get("Task Metrics") or {}
            launch, finish = ti["Launch Time"], ti["Finish Time"]
            gr = ti.get("Getting Result Time", 0)
            gr_ms = finish - gr if gr and gr > 0 else 0
            run = tm.get("Executor Run Time", 0)
            deser = tm.get("Executor Deserialize Time", 0)
            rser = tm.get("Result Serialization Time", 0)
            dur = finish - launch
            rec = {
                "dur": dur, "run": run, "deser": deser, "rser": rser, "gr": gr_ms,
                "ovh": dur - run - deser - rser - gr_ms,
                "cpu": tm.get("Executor CPU Time", 0) / 1e6, "gc": tm.get("JVM GC Time", 0),
                "rsize": tm.get("Result Size", 0), "acc": len(ti.get("Accumulables", [])),
                "launch": launch, "finish": finish,
            }
            stages[(e["Stage ID"], e["Stage Attempt ID"])]["tasks"].append(rec)

    rows = []
    for (sid, att), st in stages.items():
        ts = st["tasks"]
        if len(ts) < a.min_tasks or not ts:
            continue
        wall = (st["done"] - st["sub"]) if st["sub"] and st["done"] else (max(t["finish"] for t in ts) - min(t["launch"] for t in ts))
        scan = ",".join(sorted(s for s in st["scopes"] if s.startswith("Scan") or "Relation" in s)) or ",".join(sorted(st["scopes"]))[:60]
        def med(k): return statistics.median(t[k] for t in ts)
        def tot(k): return sum(t[k] for t in ts)
        rows.append({
            "stage": sid, "att": att, "scan": scan[:60], "name": st["name"].split("\n")[0][:40], "ntasks": len(ts),
            "wall_s": wall / 1000.0, "tasks_per_s": len(ts) * 1000.0 / max(1, wall),
            "dur_med": med("dur"), "run_med": med("run"), "deser_med": med("deser"), "ovh_med": med("ovh"),
            "ovh_p99": pctile([t["ovh"] for t in ts], 0.99),
            "ovh_sum_s": tot("ovh") / 1000.0, "run_sum_s": tot("run") / 1000.0, "deser_sum_s": tot("deser") / 1000.0,
            "cpu_sum_s": tot("cpu") / 1000.0, "offcpu_sum_s": (tot("run") - tot("cpu")) / 1000.0,
            "acc_mean": tot("acc") / len(ts), "rsize_mean": tot("rsize") / len(ts),
        })
    rows.sort(key=lambda r: -r["ovh_sum_s"])

    cols = ["stage", "att", "ntasks", "wall_s", "tasks_per_s", "dur_med", "run_med", "deser_med", "ovh_med", "ovh_p99",
            "ovh_sum_s", "run_sum_s", "deser_sum_s", "cpu_sum_s", "offcpu_sum_s", "acc_mean", "rsize_mean", "scan"]
    fmt = {"wall_s": "{:.1f}", "tasks_per_s": "{:.1f}", "ovh_sum_s": "{:.0f}", "run_sum_s": "{:.0f}", "deser_sum_s": "{:.0f}",
           "cpu_sum_s": "{:.0f}", "offcpu_sum_s": "{:.0f}", "acc_mean": "{:.1f}", "rsize_mean": "{:.0f}"}
    all_t = [t for st in stages.values() for t in st["tasks"]]
    n = len(all_t)
    print(f"app: {a.log}  tasks={n}  stages={len(rows)}  executors={len(exec_cores)} cores={sum(exec_cores.values())}"
          f"  app_wall_s={((app_end or 0) - (app_start or 0)) / 1000.0:.0f}")
    if n:
        for k in ("dur", "run", "deser", "ovh", "rser", "gr"):
            xs = [t[k] for t in all_t]
            print(f"  {k:6s} sum_s={sum(xs) / 1000.0:10.0f}  med={statistics.median(xs):8.0f}  p99={pctile(xs, 0.99):8.0f} ms")
        print(f"  accumulables/task mean={sum(t['acc'] for t in all_t) / n:.2f}  result bytes/task mean={sum(t['rsize'] for t in all_t) / n:.0f}")
        peak = max(rows, key=lambda r: r["tasks_per_s"]) if rows else None
        if peak:
            print(f"  peak launch throughput: {peak['tasks_per_s']:.1f} tasks/s at stage {peak['stage']} ({peak['ntasks']} tasks)")
    if driver_gc:
        driver_gc.sort()
        print(f"  driver GC (from stage executor metrics, cumulative peaks): minor={driver_gc[-1][1]} ms major={driver_gc[-1][2]} ms"
              f" heap_peak={max(g[3] for g in driver_gc) / 1048576:.0f} MB over {len(driver_gc)} stages")
    print()
    print("\t".join(cols))
    for r in rows[: a.top]:
        print("\t".join(fmt.get(c, "{}").format(r[c]) for c in cols))
    if a.csv:
        import csv
        with open(a.csv, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols + ["name"])
            w.writeheader()
            for r in rows:
                w.writerow({c: r[c] for c in cols + ["name"]})
        print(f"\nwrote {a.csv}")


if __name__ == "__main__":
    main()
