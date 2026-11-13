# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psutil>=6"]
# ///
"""Compare an attached DuckDB graph and an existing stable CSR on the same ecommerce snapshots."""

import argparse
import csv
import ctypes
import json
import os
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import time

from large_benchmark import HERE
from large_build import iceberg, quote
from large_suite import check_pins


CHAINS = {
    2: "(u:User)-[a:Purchased]->(p:Product)<-[b:Purchased]-(v:User)",
    # This equivalent spelling avoids a u._ID binding error in the legacy foreign optimizer.
    3: "(q:Product)<-[c:Purchased]-(v:User)-[b:Purchased]->(p:Product)<-[a:Purchased]-(u:User)",
    4: "(u:User)-[a:Purchased]->(p:Product)<-[b:Purchased]-(v:User)"
    "-[c:Purchased]->(q:Product)<-[d:Purchased]-(w:User)",
}


def detail_query(hops, uid, mode):
    chain = CHAINS[hops]
    if mode == "join":
        chain = chain.replace(":User", ":g.users").replace(":Product", ":g.products")
    conditions = [f"u.uid={uid}", "v.uid<>u.uid"]
    if hops >= 3:
        conditions.append("q.pid<>p.pid")
    if hops == 4:
        conditions.append("w.uid<>v.uid")
    order, endpoint, name = {
        2: ("b", "v.uid", "v.uname"),
        3: ("c", "q.pid", "q.pname"),
        4: ("d", "w.uid", "w.uname"),
    }[hops]
    conditions.append(f"{order}.amt<4900")
    return f"MATCH {chain} WHERE {' AND '.join(conditions)} RETURN {order}.oid,{endpoint},{name},{order}.amt"


def prepare(args, fixture):
    import duckdb

    with duckdb.connect(str(args.output / "foreign.duckdb")) as db:
        db.execute("LOAD iceberg")
        db.execute(f"SET threads={args.threads}")
        for name in ("users", "products", "orders"):
            view = "order_links" if name == "orders" else name
            projection = (
                "uid AS src_id,pid AS dst_id,oid,amt" if name == "orders" else "*"
            )
            db.execute(
                f"CREATE VIEW {view} AS SELECT {projection} FROM {iceberg(fixture['pins'], name)}"
            )
        db.execute(
            """CREATE TEMP TABLE h1 AS SELECT u.uid AS origin,p.pid AS shared
            FROM users u JOIN order_links a ON a.src_id=u.uid JOIN products p ON p.pid=a.dst_id
            WHERE u.uid=?""",
            [fixture["uid"]],
        )
        db.execute("""CREATE TEMP TABLE h2 AS SELECT h1.shared,v.uid AS buyer,b.oid,v.uid,v.uname,b.amt
            FROM h1 JOIN order_links b ON b.dst_id=h1.shared JOIN users v ON v.uid=b.src_id
            WHERE v.uid<>h1.origin""")
        db.execute("""CREATE TEMP TABLE h3 AS SELECT h2.buyer,c.oid,q.pid,q.pname,c.amt
            FROM h2 JOIN order_links c ON c.src_id=h2.buyer JOIN products q ON q.pid=c.dst_id
            WHERE q.pid<>h2.shared""")
        db.execute("""CREATE TEMP TABLE h4 AS SELECT d.oid,w.uid,w.uname,d.amt
            FROM h3 JOIN order_links d ON d.dst_id=h3.pid JOIN users w ON w.uid=d.src_id
            WHERE w.uid<>h3.buyer""")
        for hops in CHAINS:
            cols = "oid,pid,pname,amt" if hops == 3 else "oid,uid,uname,amt"
            db.execute(
                f"COPY (SELECT {cols} FROM h{hops} WHERE amt<4900 ORDER BY ALL) "
                f"TO {quote(args.output / f'expected-{hops}.csv')} (FORMAT CSV, HEADER FALSE)"
            )


def worker(args, fixture):
    sys.path.insert(0, str(HERE.parents[1]))
    os.environ["LBUG_PYTHON_BACKEND"] = "capi"
    from tools.python_api.src_py import Connection, Database

    mode, hops = args.worker[0], int(args.worker[1])
    q = detail_query(hops, fixture["uid"], mode)
    tag = args.output / f"{mode}-{hops}"
    tag.with_suffix(".cypher").write_text(q + ";\n")

    def execute(conn, stmt):
        with conn.execute(stmt) as result:
            return list(result)

    with (
        Database(
            ":memory:" if mode == "join" else str(args.graph),
            buffer_pool_size=2 << 30,
            max_num_threads=1,
        ) as db,
        Connection(db) as conn,
    ):
        execute(conn, "CALL threads=1")
        if mode == "join":
            execute(conn, "LOAD EXTENSION " + json.dumps(str(args.extension)))
            execute(
                conn,
                "ATTACH "
                + json.dumps(str(args.output / "foreign.duckdb"))
                + " AS g (DBTYPE DUCKDB)",
            )
            execute(
                conn,
                "CREATE REL TABLE Purchased(FROM g.users TO g.products) WITH (storage='g.order_links')",
            )
            set_threads = ctypes.CDLL(str(args.reader_library)).attached_reader_threads
            set_threads.argtypes = [ctypes.c_void_p, ctypes.c_int]
            set_threads.restype = ctypes.c_int
            assert (
                set_threads(conn._connection._connection._connection, args.threads)
                == args.threads
            )
        else:
            execute(
                conn,
                "CALL iceberg_poc_catalog="
                + json.dumps(str(args.fixture / "lake/catalog.sqlite")),
            )
            execute(conn, f"CALL iceberg_scan_threads={args.threads}")
            assert execute(
                conn, "CALL current_setting('iceberg_scan_threads') RETURN *"
            ) == [[str(args.threads)]]
        if args.worker[2] == "plan":
            plan = execute(conn, "EXPLAIN " + q)[0][0]
            tag.with_suffix(".plan.txt").write_text(plan)
            if mode == "join":
                assert (
                    "TABLE_FUNCTION_CALL" in plan and plan.count(" JOIN ") == 2 * hops
                )
            else:
                assert (
                    "ICEBERG_CSR_KEY_LOOKUP"
                    if hops == 2
                    else f"ICEBERG_CSR_PATH_LOOKUP ({hops} hops"
                ) in plan
            return
        samples = []
        for _ in range(6):
            start = time.perf_counter_ns()
            with conn.execute(q) as result:
                rows = list(result)
            elapsed_ms = (time.perf_counter_ns() - start) / 1e6
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            rss_bytes = rss if sys.platform == "darwin" else rss * 1024
            samples.append(
                dict(ms=elapsed_ms, peak_rss_bytes=rss_bytes, rows=len(rows))
            )
            rows.sort()
            with (args.output / f"expected-{hops}.csv").open(newline="") as f:
                for row, (oid, endpoint, name, amount) in zip(
                    rows, csv.reader(f), strict=True
                ):
                    assert row == [int(oid), int(endpoint), name, int(amount)], row
            del rows
        tag.with_suffix(".json").write_text(
            json.dumps(
                dict(
                    first_ms=samples[0]["ms"],
                    median_next_five_ms=statistics.median(s["ms"] for s in samples[1:]),
                    peak_rss_bytes=max(s["peak_rss_bytes"] for s in samples),
                    samples=samples,
                ),
                indent=2,
            )
            + "\n"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "fixture",
        type=Path,
        help="large_fixture.py output parent with lake/fixture.json",
    )
    parser.add_argument(
        "graph",
        type=Path,
        help="existing ecom stable mapping graph with a prebuilt CSR",
    )
    parser.add_argument("output", type=Path, help="new result directory")
    parser.add_argument(
        "--extension", type=Path, required=True, help="lbug foreign DuckDB extension"
    )
    parser.add_argument(
        "--reader-library", type=Path, required=True, help="iceberg_bench_reader module"
    )
    parser.add_argument("--threads", type=int, default=10)
    parser.add_argument("--worker", nargs=3, help=argparse.SUPPRESS)
    args = parser.parse_args()
    for attr in ("fixture", "graph", "output", "extension", "reader_library"):
        setattr(args, attr, getattr(args, attr).resolve())
    if args.threads < 1 or not all(
        p.is_file()
        for p in (
            args.graph,
            Path(str(args.graph) + ".csr.db"),
            args.extension,
            args.reader_library,
        )
    ):
        parser.error(
            "Supply a positive thread count, existing graph and sidecar, and both reader libraries"
        )
    fixture = json.loads((args.fixture / "lake/fixture.json").read_text())
    check_pins(args.fixture, fixture["pins"])
    if args.worker:
        worker(args, fixture)
    else:
        args.output.mkdir(parents=True, exist_ok=False)
        prepare(args, fixture)
        for hops in CHAINS:
            for mode in ("join", "csr"):
                for stage in ("plan", "time"):
                    subprocess.run(
                        [sys.executable, *sys.argv, "--worker", mode, str(hops), stage],
                        check=True,
                    )
                print(
                    mode,
                    hops,
                    (args.output / f"{mode}-{hops}.json").read_text(),
                    flush=True,
                )
        check_pins(args.fixture, fixture["pins"])
