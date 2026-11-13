# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psutil>=6"]
# ///
"""Run one mapped Cypher workload per process and sample its resident memory from the parent."""

import argparse
import ctypes
import json
import os
import resource
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent


def queries(uid):
    anchor = f"u.uid={uid}"
    pattern = "(u:User)-[a:Purchased]->(p:Product)"
    two = pattern + "<-[b:Purchased]-(v:User)"
    three = two + "-[c:Purchased]->(q:Product)"
    four = three + "<-[d:Purchased]-(w:User)"
    return {
        "one_count": f"MATCH {pattern} WHERE {anchor} RETURN count(*)",
        "one_topk": f"MATCH {pattern} WHERE {anchor} AND a.amt<4900 RETURN a.oid, a.amt ORDER BY a.oid LIMIT 3",
        "two_count": f"MATCH {two} WHERE {anchor} AND id(v)<>id(u) RETURN count(*)",
        "two_buyers": f"MATCH {two} WHERE {anchor} AND id(v)<>id(u) RETURN v.uid AS buyer, count(*) AS orders ORDER BY orders DESC, buyer LIMIT 10",
        "three_count": f"MATCH {three} WHERE {anchor} AND id(v)<>id(u) AND id(q)<>id(p) RETURN count(*)",
        "four_count": f"MATCH {four} WHERE {anchor} AND id(v)<>id(u) AND id(q)<>id(p) AND id(w)<>id(v) RETURN count(*)",
    }


def memory_reader():
    if sys.platform == "darwin":
        lib = ctypes.CDLL("/usr/lib/libproc.dylib")

        class Usage(ctypes.Structure):
            _fields_ = [("uuid", ctypes.c_ubyte * 16)] + [
                (field, ctypes.c_uint64)
                for field in (
                    "user_time",
                    "system_time",
                    "wakes",
                    "idle_wakes",
                    "pageins",
                    "wired",
                    "resident",
                    "footprint",
                    "start",
                    "exit",
                )
            ]

        def sample(pid):
            usage = Usage()
            if lib.proc_pid_rusage(pid, 0, ctypes.byref(usage)) != 0:
                return None
            return usage.resident, usage.footprint

        return sample
    import psutil

    def sample(pid):
        try:
            return psutil.Process(pid).memory_info().rss, None
        except psutil.NoSuchProcess:
            return None

    return sample


def emit(**values):
    print(json.dumps(dict(t=time.monotonic(), **values)), flush=True)


def worker(args):
    sys.path.insert(0, str(HERE.parents[1]))
    os.environ["LBUG_PYTHON_BACKEND"] = "capi"
    from tools.python_api.src_py import Connection, Database

    def run(c, query):
        with c.execute(query) as r:
            return list(r)

    lake, dbfile = args.root / "lake", args.graph
    if args.query_file:
        query = args.query_file.read_text().strip()
    else:
        query = queries(json.loads((lake / "fixture.json").read_text())["uid"])[
            args.case
        ]
    with (
        Database(
            str(dbfile),
            buffer_pool_size=args.main_buffer_mib << 20,
            max_num_threads=1,
        ) as db,
        Connection(db) as c,
    ):
        run(c, "CALL threads=1")
        if args.scan_threads is not None:
            run(c, f"CALL iceberg_scan_threads={args.scan_threads}")
        if args.disable_key_pushdown:
            run(c, "CALL iceberg_poc_key_pushdown=false")
        run(
            c,
            "CALL iceberg_poc_catalog=" + json.dumps(str(lake / "catalog.sqlite")),
        )
        if not run(c, "CALL show_tables() RETURN name"):
            for statement in (
                (HERE / "schema-sidecar.cypher").read_text().split(";")
            ):
                if statement.strip():
                    run(c, statement)
        if args.mode == "setup":
            emit(event="schema")
            return
        if args.mode in {"plan", "operators"}:
            statement = "PROFILE " if args.mode == "operators" else "EXPLAIN "
            print(run(c, statement + query)[0][0])
            return
        sample = memory_reader()
        emit(event="ready", baseline=sample(os.getpid()), cypher=query)
        if args.mode == "txn":
            run(c, "BEGIN TRANSACTION READ ONLY")
        anchors = (
            json.loads(args.anchors.read_text())
            if args.anchors
            else [None] * args.repeats
        )
        for i, anchor in enumerate(anchors):
            if anchor is not None:
                query = queries(int(anchor))[args.case]
            emit(event="start", i=i)
            start = time.perf_counter_ns()
            rows = run(c, query)
            elapsed = (time.perf_counter_ns() - start) / 1e6
            emit(
                event="result",
                i=i,
                anchor=anchor,
                ms=elapsed,
                rows=rows,
                memory=sample(os.getpid()),
            )
        if args.mode == "txn":
            run(c, "COMMIT")
        emit(event="before_close", memory=sample(os.getpid()))
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    emit(
        event="closed", peak_rss_bytes=peak if sys.platform == "darwin" else peak * 1024
    )


def profile(command, prefix, cap_gib):
    sample = memory_reader()
    samples, peak_rss, peak_footprint = [], 0, 0
    stopped = False
    t0 = time.monotonic()
    with (
        prefix.with_suffix(".jsonl").open("w") as out,
        prefix.with_suffix(".log").open("w") as err,
    ):
        process = subprocess.Popen(command, stdout=out, stderr=err)
        while process.poll() is None:
            m = sample(process.pid)
            if m:
                rss, fp = m
                peak_rss = max(peak_rss, rss)
                peak_footprint = max(peak_footprint, fp or 0)
                samples.append([round(time.monotonic(), 4), rss, fp])
                if cap_gib and max(rss, fp or 0) > cap_gib * (1 << 30):
                    stopped = True
                    process.kill()
            time.sleep(0.02)
        rc = process.wait()
    prefix.with_suffix(".memory.json").write_text(
        json.dumps(
            dict(
                elapsed_s=time.monotonic() - t0,
                peak_rss=peak_rss,
                peak_footprint=peak_footprint,
                limit_gib=cap_gib,
                stopped=stopped,
                returncode=rc,
                sample_interval_ms=20,
                samples=samples,
            )
        )
        + "\n"
    )
    return rc


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("root", type=Path)
    p.add_argument("--graph", type=Path, required=True)
    p.add_argument("--case", default="one_count", choices=queries(0))
    p.add_argument(
        "--anchors", type=Path, help="JSON list of starting user keys, one per query"
    )
    p.add_argument("--query-file", type=Path, help="measure a custom Cypher query")
    p.add_argument(
        "--disable-key-pushdown",
        action="store_true",
        help="measure the original mapped scan",
    )
    p.add_argument(
        "--mode", default="auto", choices=["setup", "auto", "txn", "plan", "operators"]
    )
    p.add_argument("--repeats", type=int, default=6)
    p.add_argument("--profile", type=Path)
    p.add_argument("--limit-gib", type=float, default=0)
    p.add_argument("--main-buffer-mib", type=int, default=2048)
    p.add_argument("--scan-threads", type=int, help="DuckDB threads per mapped Iceberg reader")
    args = p.parse_args()
    args.root = args.root.resolve()
    args.graph = args.graph.resolve()
    if args.profile:
        cmd = [
            sys.executable,
            __file__,
            str(args.root),
            "--graph",
            str(args.graph),
            "--case",
            args.case,
            "--mode",
            args.mode,
            "--repeats",
            str(args.repeats),
        ]
        cmd.extend(["--main-buffer-mib", str(args.main_buffer_mib)])
        if args.scan_threads is not None:
            cmd.extend(["--scan-threads", str(args.scan_threads)])
        if args.anchors:
            cmd.extend(["--anchors", str(args.anchors.resolve(strict=True))])
        if args.query_file:
            cmd.extend(["--query-file", str(args.query_file.resolve(strict=True))])
        if args.disable_key_pushdown:
            cmd.append("--disable-key-pushdown")
        sys.exit(profile(cmd, args.profile, args.limit_gib))
    worker(args)
