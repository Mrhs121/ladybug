# /// script
# requires-python = ">=3.11"
# dependencies = ["pyiceberg[sql-sqlite]==0.11.1", "pyarrow==23.0.1", "numpy>=1.26", "duckdb==1.5.5"]
# ///
"""Check automatic MATCH translation against snapshot-pinned Iceberg joins, and time both routes."""

import argparse
import json
import os
import statistics
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter_ns


from benchmark import fixture
from run import HERE, batch, create_fixture, execute, join_sql, lake_versions
from sidecar import open_graph

MATCH = """MATCH (u:User)-[o:Purchased]->(p:Product)
WHERE u.uid = {uid} AND o.amt < {threshold}
RETURN o.oid, o.amt ORDER BY o.oid LIMIT 3"""
MARKER = "DATAFUSION_ICEBERG_MATCH"


def reference_sql(pins, where, direction="ASC", limit=3):
    join = join_sql(pins, ordered=False)
    return f"SELECT oid, amt FROM ({join}) j WHERE {where} ORDER BY oid {direction} LIMIT {limit}"


def oracle(pins, where, direction="ASC", limit=3):
    sql = reference_sql(pins, where, direction, limit)
    raw = subprocess.check_output(
        ["duckdb", "-no-init", "-json", "-c", "LOAD iceberg; " + sql], text=True
    )
    return [[r["oid"], r["amt"]] for r in json.loads(raw.strip() or "[]")]


def setup(conn, path):
    execute(conn, "CALL iceberg_poc_catalog=" + json.dumps(str(path)))
    for statement in (HERE / "schema-sidecar.cypher").read_text().split(";"):
        if statement.strip():
            execute(conn, statement)


@contextmanager
def trace_plan(path):
    # Rust writes plans to the process stderr, including the physical Iceberg predicates.
    saved_fd = os.dup(2)
    old_trace = os.environ.get("LBUG_ICEBERG_DF_TRACE")
    try:
        with path.open("w") as trace:
            os.dup2(trace.fileno(), 2)
            os.environ["LBUG_ICEBERG_DF_TRACE"] = "1"
            yield
    finally:
        os.dup2(saved_fd, 2)
        os.close(saved_fd)
        if old_trace is None:
            os.environ.pop("LBUG_ICEBERG_DF_TRACE")
        else:
            os.environ["LBUG_ICEBERG_DF_TRACE"] = old_trace


def check(root):
    path, users, products, orders = create_fixture(root)
    query = MATCH.format(uid=11, threshold=50)
    checks = []
    with open_graph(root / "graph.db") as (_, c):
        setup(c, path)
        assert MARKER in str(execute(c, "EXPLAIN " + query))

        def compare(stage, pins=None, q=query, where="uid=11 AND amt<50", **kwargs):
            pins = pins or lake_versions(path)
            rows = execute(c, q)
            expected = oracle(pins, where, **kwargs)
            assert rows == expected, (stage, rows, expected)
            checks.append(dict(stage=stage, rows=rows, join=expected, pins=pins))
            return rows

        pins = lake_versions(path)
        with trace_plan(root / "plan.txt"):
            assert compare("parallel orders", pins) == [[7001, 25], [7002, 26]]
        physical = (root / "plan.txt").read_text().split("[iceberg_df] physical")[1]
        scan = next(
            line for line in physical.splitlines() if "IcebergTableScan" in line
        )
        assert "LbugCsrExpandExec: direction=FWD" in physical
        assert "[iceberg_df] CSR candidate edges=2" in physical
        assert "uid = 11" in scan and "amt < 50" in scan and "pid" not in scan, scan

        execute(c, "BEGIN TRANSACTION READ ONLY")
        compare("pin old transaction", pins)
        orders.append(
            batch("orders", (-4, 11, 101, 7), (0, 11, 101, None), (2, 11, 999, 5))
        )
        compare("old transaction after append", pins)
        execute(c, "COMMIT")
        assert compare("signed key and missing endpoint") == [
            [-4, 7],
            [7001, 25],
            [7002, 26],
        ]
        orders.upsert(batch("orders", (7001, 11, 101, 80)), join_cols=["oid"])
        compare("amount update")
        orders.upsert(batch("orders", (7002, 42, 205, 26)), join_cols=["oid"])
        compare("endpoint moved")
        orders.delete("oid = -4")
        compare("edge deleted")
        products.append(batch("products", (999, "New endpoint")))
        assert compare("new destination") == [[2, 5]]

        reverse = """MATCH (item:Product)<-[line:Purchased]-(customer:User)
            WHERE item.pid=101 RETURN line.oid AS id, line.amt ORDER BY line.oid DESC LIMIT 3"""
        assert MARKER in str(execute(c, "EXPLAIN " + reverse))
        assert compare(
            "reverse CSR, alias, nullable attribute",
            q=reverse,
            where="pid=101",
            direction="DESC",
        ) == [[7001, 80], [0, None]]
        products.delete("pid = 999")
        assert compare("destination deleted") == []
        users.delete("uid = 11")
        compare("anchor deleted")

        prepared_query = MATCH.format(uid="$who", threshold="$ceiling")
        with c.execute("EXPLAIN " + prepared_query, {"who": 42, "ceiling": 200}) as r:
            assert MARKER in str(list(r))
        statement = c.prepare(prepared_query, {"who": 42, "ceiling": 200})
        try:
            for key, ceiling in [(42, 200), (42, 50), (11, 50)]:
                with c.execute(statement, {"who": key, "ceiling": ceiling}) as r:
                    actual = list(r)
                pins = lake_versions(path)
                expected = oracle(pins, f"uid={key} AND amt<{ceiling}")
                assert actual == expected
                checks.append(
                    dict(
                        stage="rebound parameters",
                        rows=actual,
                        join=expected,
                        pins=pins,
                    )
                )
        finally:
            statement.close()

        # One native neighbor list and an Arrow result both cross vector boundaries.
        orders.append(
            batch(
                "orders",
                *((i, 42, 205, i if i % 7 else None) for i in range(20000, 28209)),
            )
        )
        many = """MATCH (a:User)-[e:Purchased]->(:Product) WHERE a.uid=42
            RETURN e.amt, e.oid ORDER BY e.oid DESC LIMIT 9000"""
        assert MARKER in str(execute(c, "EXPLAIN " + many))
        pins = lake_versions(path)
        actual = execute(c, many)
        expected = [[amt, oid] for oid, amt in oracle(pins, "uid=42", "DESC", 9000)]
        assert len(actual) > 8192 and actual == expected
        checks.append(
            dict(
                stage="ordered nullable rows across batches",
                row_count=len(actual),
                pins=pins,
                matched_join=True,
            )
        )

        fallback = "MATCH (u:User)-[:Purchased]->(p:Product)<-[:Purchased]-(v:User) RETURN count(*)"
        assert MARKER not in str(execute(c, "EXPLAIN " + fallback))
        original = execute(c, fallback)
        execute(c, "CALL iceberg_poc_datafusion=false")
        assert MARKER not in str(execute(c, "EXPLAIN " + query))
        assert execute(c, fallback) == original
    return checks


def benchmark(root, edges):
    import duckdb

    root.mkdir()
    path, uid, _, _ = fixture(root, edges)
    query = MATCH.format(uid=uid, threshold=4900)
    pins = lake_versions(path)
    expected = oracle(pins, f"uid={uid} AND amt<4900")
    samples = {"datafusion": [], "existing": [], "duckdb_join": []}
    sql = reference_sql(pins, f"uid={uid} AND amt<4900")
    with open_graph(root / "graph.db") as (_, c), duckdb.connect() as ref:
        setup(c, path)
        execute(c, "CALL threads=1")
        ref.execute("SET threads=1")
        ref.execute("LOAD iceberg")
        for i in range(8):
            for mode in list(samples) if i % 2 == 0 else reversed(samples):
                if mode == "duckdb_join":
                    start = perf_counter_ns()
                    actual = [list(r) for r in ref.execute(sql).fetchall()]
                else:
                    execute(
                        c,
                        "CALL iceberg_poc_datafusion="
                        + str(mode == "datafusion").lower(),
                    )
                    assert (MARKER in str(execute(c, "EXPLAIN " + query))) == (
                        mode == "datafusion"
                    )
                    start = perf_counter_ns()
                    actual = execute(c, query)
                latency = (perf_counter_ns() - start) / 1e6
                assert actual == expected
                if not i and mode == "datafusion":
                    first_build_ms = round(latency, 2)
                if i:  # both routes use the same warmed, read-only sidecar generation
                    samples[mode].append(latency)
        execute(c, "CALL iceberg_poc_datafusion=true")
        with trace_plan(root / "plan.txt"):
            assert execute(c, query) == expected
    assert lake_versions(path) == pins
    return dict(
        edges=edges,
        pins=pins,
        query=query,
        rows=expected,
        samples_ms=samples,
        first_build_ms=first_build_ms,
        median_ms={k: round(statistics.median(v), 2) for k, v in samples.items()},
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--benchmark-edges", type=int)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="df-match-", dir=args.output.resolve()))
    report = {"checks": check(root)}
    if args.benchmark_edges:
        report["benchmark"] = benchmark(root / "bench", args.benchmark_edges)
        print(report["benchmark"]["median_ms"])
    (root / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    print(root, flush=True)
