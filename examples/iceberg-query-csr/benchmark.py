# /// script
# requires-python = ">=3.11"
# dependencies = ["pyiceberg[sql-sqlite]==0.11.1", "pyarrow==23.0.1", "numpy>=1.26"]
# ///
"""Scale the original Iceberg fixture and time materialized MATCH and JOIN results."""

import argparse
import json
import platform
import statistics
import subprocess
import tempfile
from pathlib import Path
from time import perf_counter_ns

import numpy as np
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

from run import (
    HERE,
    MATCH,
    SCHEMAS,
    Connection,
    Database,
    batch,
    execute,
    join_sql,
    lake_versions,
    reference_join,
    register_mapping,
)


def fixture(root, edges):
    path = root / "catalog.sqlite"
    catalog = SqlCatalog(
        "poc", uri=f"sqlite:///{path}", warehouse=(root / "warehouse").as_uri()
    )
    catalog.create_namespace("ecom")
    nusers, nproducts = max(1, edges // 10), max(1, edges // 50)
    users = 1001 + 7 * np.arange(nusers)
    products = 503 + 11 * np.arange(nproducts)
    rng = np.random.default_rng(314159)
    order = rng.permutation(edges)
    data = {
        "users": [users, [f"user-{i}" for i in range(nusers)]],
        "products": [products, [f"product-{i}" for i in range(nproducts)]],
        "orders": [
            100003 + 13 * order,
            users[order % nusers],
            products[rng.integers(nproducts, size=edges)],
            10 + order % 5001,
        ],
    }
    for table, schema in SCHEMAS.items():
        source = catalog.create_table(
            ("ecom", table), schema, properties={"format-version": "2"}
        )
        source.append(pa.Table.from_arrays(data.pop(table), schema=schema))
    return path, int(users[nusers // 2]), nusers, nproducts


def user_query(uid):
    return MATCH.replace("RETURN", f"WHERE u.uid = {uid} RETURN")


def after_commits(root, edges, repeats, schema, sidecar):
    root.mkdir()
    path, uid, users, products = fixture(root, edges)
    orders = SqlCatalog("poc", uri=f"sqlite:///{path}").load_table("ecom.orders")
    cypher = user_query(uid)
    samples = []
    with (
        Database(
            str(root / "graph.db") if sidecar else ":memory:",
            buffer_pool_size=512 << 20,
            max_num_threads=1,
        ) as db,
        Connection(db) as conn,
    ):
        register_mapping(conn, path, schema)
        versions = lake_versions(path)
        baseline = execute(conn, cypher)
        if baseline != reference_join(versions, uid):
            raise AssertionError("Initial MATCH/JOIN differ")

        def check(stage, expected):
            nonlocal versions
            previous = versions
            start = perf_counter_ns()
            with conn.execute(cypher) as result:
                ms = (perf_counter_ns() - start) / 1e6
                rows = list(result)
            # This process owns the only writer, which is idle throughout MATCH and JOIN.
            versions = lake_versions(path)
            if versions["orders"]["snapshot"] == previous["orders"]["snapshot"]:
                raise AssertionError(f"{stage}: no new orders snapshot")
            if rows != expected or rows != reference_join(versions, uid):
                raise AssertionError(f"{stage}: unexpected rows after commit")
            samples.append(
                {"stage": stage, "ms": ms, "pins": versions, "match_and_join": rows}
            )

        for i in range(repeats):
            oid = -(i + 1)  # Sorts before every order in the fixture.
            first, last = 503, 503 + 11 * (products - 1)
            user_name = f"user-{users // 2}"
            orders.append(batch("orders", (oid, uid, first, 99)))
            check("append", [[oid, uid, user_name, first, "product-0", 99], *baseline])
            orders.upsert(batch("orders", (oid, uid, last, 199)), join_cols=["oid"])
            check(
                "update",
                [
                    [oid, uid, user_name, last, f"product-{products - 1}", 199],
                    *baseline,
                ],
            )
            orders.delete(f"oid = {oid}")
            check("delete", baseline)
    medians = {
        stage: statistics.median(s["ms"] for s in samples if s["stage"] == stage)
        for stage in ("append", "update", "delete")
    }
    print(f"{edges:>8} edges  first MATCH after commit  {medians}", flush=True)
    return {
        "base_edges": edges,
        "connections": 1,
        "median_ms": medians,
        "checks": samples,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--build",
        type=Path,
        required=True,
        help="root lbug build directory containing the native mapping benchmark",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--edges", type=int, nargs="+", default=[10000, 100000, 1000000]
    )
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument(
        "--sidecar",
        action="store_true",
        help="measure native persistent CSR and cross-transaction hits",
    )
    parser.add_argument(
        "--after-commits",
        action="store_true",
        help="also time the first MATCH after each insert/update/delete on the largest scale",
    )
    args = parser.parse_args()
    if args.repeats < 1 or min(args.edges) < 1:
        parser.error("edges and repeats must be positive")
    build = args.build.resolve()
    schema = HERE / ("schema-sidecar.cypher" if args.sidecar else "schema.cypher")
    runner = (build / "examples/iceberg-query-csr/iceberg_query_bench").resolve(strict=True)
    args.output.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="bench-", dir=args.output.resolve()))
    report = {
        "platform": platform.platform(),
        "threads_per_engine": 1,
        "repeats": args.repeats,
        "persistent_csr": args.sidecar,
        "cases": [],
    }
    print(root, flush=True)
    for edges in args.edges:
        dataset = root / str(edges)
        dataset.mkdir()
        path, uid, users, products = fixture(dataset, edges)
        pins = lake_versions(path)
        queries = {
            "all": (MATCH, join_sql(pins)),
            "one_user": (user_query(uid), join_sql(pins, uid)),
        }
        for name, (cypher, sql) in queries.items():
            case = dataset / name
            case.mkdir()
            cypher_path, sql_path = case / "match.cypher", case / "join.sql"
            cypher_path.write_text(cypher)
            sql_path.write_text(sql)
            with (case / "trace.log").open("w") as trace:
                proc = subprocess.run(
                    [
                        runner,
                        schema,
                        path,
                        cypher_path,
                        sql_path,
                        str(args.repeats),
                        *([case / "graph.db"] if args.sidecar else []),
                    ],
                    stdout=subprocess.PIPE,
                    stderr=trace,
                    text=True,
                )
            if proc.returncode:
                raise RuntimeError(f"Benchmark failed; see {case / 'trace.log'}")
            samples = {
                mode: []
                for mode in (
                    "sidecar_hit" if args.sidecar else "rebuild",
                    "reuse",
                    "join",
                )
            }
            first_build_ms = None
            expected_rows = (
                edges if name == "all" else (edges - 1 - users // 2) // users + 1
            )
            for line in proc.stdout.splitlines():
                mode, _, ms, rows = line.split(",")
                if int(rows) != expected_rows:
                    raise AssertionError(line)
                if mode == "first_build":
                    first_build_ms = float(ms)
                else:
                    samples[mode].append(float(ms))
            if args.sidecar != (first_build_ms is not None):
                raise AssertionError(proc.stdout)
            if any(len(t) != args.repeats for t in samples.values()):
                raise AssertionError(proc.stdout)
            medians = {
                mode: statistics.median(times) for mode, times in samples.items()
            }
            report["cases"].append(
                {
                    "edges": edges,
                    "users": users,
                    "products": products,
                    "query": name,
                    "result_rows": expected_rows,
                    "pins": pins,
                    "samples_ms": samples,
                    "median_ms": medians,
                    **({"first_build_ms": first_build_ms} if args.sidecar else {}),
                }
            )
            (root / "results.json").write_text(json.dumps(report, indent=2) + "\n")
            print(f"{edges:>8} edges {name:>10}  {medians}", flush=True)
    if args.after_commits:
        report["after_commits"] = after_commits(
            root / "after-commits",
            max(args.edges),
            args.repeats,
            schema,
            args.sidecar,
        )
        (root / "results.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
