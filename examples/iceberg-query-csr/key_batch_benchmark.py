# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psutil>=6"]
# ///
"""Compare the two-hop DuckDB key lookup with JOINs on the original pinned Iceberg tables."""

import argparse
from collections import Counter
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys

import duckdb

from large_benchmark import HERE, profile
from large_build import iceberg
from large_suite import check_pins, log_rows, read, write


def main(root, out, graph, repeats, cap_gib):
    out.mkdir(parents=True, exist_ok=True)
    fixture = read(root / "lake/fixture.json")
    pins, uid = fixture["pins"], int(fixture["uid"])
    check_pins(root, pins)
    assert read(root / "topology/manifest.json")["pins"] == pins
    base = (
        "MATCH (u:User)-[a:Purchased]->(p:Product)<-[b:Purchased]-(v:User)\n"
        f"WHERE u.uid={uid} AND id(v)<>id(u) AND b.amt<4900\n"
    )
    graph_queries = {
        "count": base + "RETURN count(*)",
        "rows": base + "RETURN b.oid,v.uid,v.uname,b.amt",
    }
    graph_queries["names"] = (
        f"MATCH (u:User)-[:Purchased]->(p:Product)<-[:Purchased]-(v:User) "
        f"WHERE u.uid={uid} AND v.uid<>u.uid RETURN DISTINCT v.uid,v.uname"
    )
    original = root / "diagnostics/two_amount.cypher"
    if original.exists():
        assert original.read_text().strip() == graph_queries["count"]
    worker = [
        sys.executable,
        str(HERE / "large_benchmark.py"),
        str(root),
        "--graph",
        str(graph),
        "--main-buffer-mib",
        "2048",
    ]
    os.environ["LBUG_ICEBERG_POC_PROFILE"] = "1"
    expected = {}
    with duckdb.connect() as db:
        db.execute("SET threads=1")
        db.execute("LOAD iceberg")
        for name in ("users", "products", "orders"):
            db.execute(f"CREATE VIEW {name} AS SELECT * FROM {iceberg(pins, name)}")
        joins = f"""FROM users u JOIN orders a ON a.uid=u.uid JOIN products p ON p.pid=a.pid
                  JOIN orders b ON b.pid=p.pid JOIN users v ON v.uid=b.uid
                  WHERE u.uid={uid} AND v.uid<>u.uid"""
        count_sql = "SELECT count(*) " + joins + " AND b.amt<4900"
        expected["count"] = db.execute(count_sql).fetchall()
        expected["rows"] = db.execute(
            "SELECT b.oid,v.uid,v.uname,b.amt " + joins + " AND b.amt<4900"
        ).fetchall()
        expected["names"] = db.execute(
            "SELECT DISTINCT v.uid,v.uname " + joins
        ).fetchall()
        candidates = db.execute(
            "SELECT count(*),count(DISTINCT b.oid),count(DISTINCT v.uid) " + joins
        ).fetchone()
    write(
        out / "snapshot-join.json",
        {"pins": pins, "count_sql": count_sql, "candidates": candidates, **expected},
    )
    print(
        f"Pinned JOIN: {candidates} (paths, orders, users), {expected['count'][0][0]} matches",
        flush=True,
    )
    report = {
        "counts": fixture["counts"],
        "snapshots": {k: v["snapshot"] for k, v in pins.items()},
        "origin_uid": uid,
        "candidate_paths_orders_buyers": candidates,
        "runs": {},
    }
    trace_sql = {}
    for case, query in graph_queries.items():
        path = out / (case + ".cypher")
        path.write_text(query + "\n")
        cmd = worker + ["--query-file", str(path)]
        plan = subprocess.check_output(cmd + ["--mode", "plan"], text=True)
        assert "ICEBERG_CSR_KEY_LOOKUP" in plan, plan
        (out / (case + ".plan.txt")).write_text(plan)
        for mode in ["auto", "txn"] if case == "count" else ["auto"]:
            prefix = out / (case + "-" + mode)
            n = repeats + 1 if case == "count" else 1
            rc = profile(cmd + ["--mode", mode, "--repeats", str(n)], prefix, cap_gib)
            if rc:
                raise RuntimeError(f"{case}/{mode} exited {rc}, see {prefix}.log")
            results = [
                r
                for r in log_rows(prefix.with_suffix(".jsonl"))
                if r["event"] == "result"
            ]
            assert len(results) == n
            for result in results:
                assert Counter(map(tuple, result["rows"])) == Counter(
                    expected[case]
                ), case
            traces = []
            log = prefix.with_suffix(".log").read_text()
            assert "[iceberg_csr] reused " in log and "[iceberg_csr] built " not in log
            for table, keys, rows, sql in re.findall(
                r"\[iceberg_key_batch\] table=(\S+) keys=(\d+) rows=(\d+) sql=([^\n]+)",
                log,
            ):
                assert " IN (" in sql and "SELECT *" not in sql
                traces.append({"table": table, "keys": int(keys), "rows": int(rows)})
                trace_sql.setdefault((case, table), sql)
            assert traces
            times = [r["ms"] for r in results]
            mem = read(prefix.with_suffix(".memory.json"))
            record = dict(
                first_ms=times[0],
                repeat_ms=times[1:],
                median_ms=statistics.median(times[1:]) if n > 1 else None,
                peak_rss=mem["peak_rss"],
                peak_footprint=mem["peak_footprint"],
                kernel_peak_rss=log_rows(prefix.with_suffix(".jsonl"))[-1][
                    "peak_rss_bytes"
                ],
                result_rows=len(expected[case]),
                traces=traces,
            )
            report["runs"][prefix.name] = record
            print(
                prefix.name,
                "first ms",
                round(times[0], 1),
                "median ms",
                record["median_ms"],
                "peak RSS MiB",
                round(mem["peak_rss"] / 2**20, 1),
                flush=True,
            )
    # Profile the first logged lookup for each table and query shape outside the timed runs.
    with duckdb.connect() as db:
        db.execute("SET threads=1")
        db.execute("LOAD iceberg")
        report["duckdb_scans"] = {}
        for (case, table), sql in trace_sql.items():
            plan = json.loads(
                db.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + sql).fetchone()[1]
            )
            write(out / f"{case}-{table}.duckdb.json", plan)
            nodes = [plan]
            while nodes:
                node = nodes.pop()
                if node.get("operator_name") == "ICEBERG_SCAN":
                    extra = node["extra_info"]
                    report["duckdb_scans"][f"{case}-{table}"] = {
                        "rows_scanned": node["operator_rows_scanned"],
                        "scan_output_rows": node["operator_cardinality"],
                        "columns": extra.get("Projections"),
                        "files_read": extra.get("Total Files Read"),
                    }
                nodes.extend(node.get("children", []))
    check_pins(root, pins)
    write(out / "result.json", report)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("out", type=Path)
    parser.add_argument(
        "--graph",
        type=Path,
        help="optional copy of the mapped database and its sidecar",
    )
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--limit-gib", type=float, default=6)
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("at least one timed repetition is required")
    main(
        args.root.resolve(),
        args.out.resolve(),
        (args.graph or args.root / "mapped/graph.db").resolve(strict=True),
        args.repeats,
        args.limit_gib,
    )
