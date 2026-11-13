# /// script
# requires-python = ">=3.11"
# dependencies = ["pyiceberg[sql-sqlite]==0.11.1","pyarrow==23.0.1","duckdb==1.5.5","psutil>=6"]
# ///
"""Measure an existing mapping connection across orders -> users -> products Iceberg commits."""

import argparse
import json
from pathlib import Path
import statistics
import re
import sys
import time

import duckdb
from pyiceberg.catalog.sql import SqlCatalog

from append_benchmark import fork_fixture, native_tables, save
from large_benchmark import profile
from large_build import iceberg
from run import HERE, Connection, Database, batch, execute, lake_versions


def ids(x):
    return "[" + ",".join(map(str, x)) + "]"


def run(base, root):
    orders, original = fork_fixture(
        base, root, writable=("users", "orders", "products"), copy_main=False
    )
    cat = SqlCatalog("poc", uri="sqlite:///" + str(root / "catalog.sqlite"))
    users = cat.load_table("ecom.users")
    products = cat.load_table("ecom.products")
    counts = json.loads((base / "lake/fixture.json").read_text())["counts"]
    n, nu, np = counts["orders"], counts["users"], counts["products"]
    assert min(nu, np) > 101
    # Every new key falls strictly between two old keys. Use a permutation across products.
    u = [1001 + 7 * ((i + 1) * (nu // 101)) + 1 for i in range(100)]
    p = [503 + 11 * ((i + 1) * (np // 101)) + 1 for i in range(100)]
    records = [
        (100003 + 13 * (n + i), u[i], p[(i * 37) % 100], i % 9 + 1) for i in range(100)
    ]
    payload = [(x, y, z, a) for x, y, z, a in records]
    orders_batch = batch("orders", *payload)
    user_batch = batch("users", *((x, f"new-user-{x}") for x in reversed(u)))
    product_batch = batch("products", *((x, f"new-product-{x}") for x in reversed(p)))

    def graph_query(col, keys):
        return f"MATCH (u:User)-[o:Purchased]->(p:Product) WHERE {col} IN {ids(keys)} RETURN u.uid,o.oid,p.pid ORDER BY o.oid"

    target = graph_query("u.uid", u)
    reverse = graph_query("p.pid", p)
    controls = sorted({u[0] - 1, u[0] + 6, u[49] + 6, u[-1] + 6})
    old_query = graph_query("u.uid", controls)
    node_queries = [
        (
            label,
            key,
            sorted(
                {
                    fresh[0] - 1,
                    fresh[0] + step - 1,
                    fresh[49] + step - 1,
                    fresh[-1] + step - 1,
                }
            ),
            fresh,
        )
        for label, key, step, fresh in [
            ("User", "uid", 7, u),
            ("Product", "pid", 11, p),
        ]
    ]

    def inspect_ids(c, after):
        for (label, key, control, fresh), visible, total, start, step in zip(
            node_queries, after, (nu, np), (1001, 503), (7, 11)
        ):
            sampled = control + fresh if visible else control
            q = f"MATCH (v:{label}) WHERE v.{key} IN {ids(sampled)} RETURN v.{key},offset(id(v)) ORDER BY v.{key}"
            expected = {k: (k - start) // step for k in control}
            if visible:
                expected.update((k, total + i) for i, k in enumerate(fresh))
            assert execute(c, q) == [list(t) for t in sorted(expected.items())]

    stages = []
    sidecar = root / "graph.db.csr.db"
    base_table = native_tables(sidecar)
    assert len(base_table) == 1
    inode = sidecar.stat().st_ino
    db_size = sidecar.stat().st_size
    setting = "CALL iceberg_poc_catalog=" + json.dumps(str(root / "catalog.sqlite"))
    with duckdb.connect() as sql:
        sql.execute("SET threads=2")
        sql.execute("SET memory_limit='1800MiB'")
        sql.execute("LOAD iceberg")
        with (
            Database(
                str(root / "graph.db"), buffer_pool_size=2 << 30, max_num_threads=1
            ) as db,
            Connection(db) as c,
            Connection(db) as old,
            Connection(db) as late,
        ):
            for connection in (c, old, late):
                execute(connection, setting)
                execute(connection, "CALL threads=1")
            for ddl in (HERE / "schema-stable.cypher").read_text().split(";"):
                if ddl.strip():
                    execute(c, ddl)
            for stage in ("baseline", "orders", "users", "products"):
                if stage == "baseline":
                    init = time.perf_counter()
                    assert execute(c, "MATCH ()-[:Purchased]->() RETURN count(*)") == [
                        [n]
                    ]
                    init_ms = (time.perf_counter() - init) * 1000
                    print("baseline ready", init_ms, flush=True)
                    (root / "plan.txt").write_text(str(execute(c, "EXPLAIN " + target)))
                elif stage == "orders":
                    orders.append(orders_batch)
                elif stage == "users":
                    users.append(user_batch)
                elif stage == "products":
                    products.append(product_batch)
                committed = time.perf_counter()
                commit_unix_ns = time.time_ns()
                # Orders can reference only absent anchors. Explicitly refresh that batch, exactly
                # as a CDC consumer would. The last stage's first call is the timed normal MATCH.
                if stage == "orders":
                    receipt = execute(
                        c,
                        f"CALL iceberg_csr_refresh('Purchased',{orders.current_snapshot().snapshot_id}) RETURN edge_count",
                    )
                    assert receipt == [[n]]
                rows = execute(c, target)
                latency = (time.perf_counter() - committed) * 1000
                cnt = execute(c, "MATCH ()-[:Purchased]->() RETURN count(*)")[0][0]
                pins = lake_versions(root / "catalog.sqlite")
                o, s, d = (
                    iceberg(pins, name) for name in ("orders", "users", "products")
                )
                tables = (
                    f" FROM {o} o JOIN {s} u ON u.uid=o.uid JOIN {d} p ON p.pid=o.pid"
                )
                # One complete three-way join supplies the global count and all comparison rows.
                def selected(column, keys):
                    return f"list([u.uid,o.oid,p.pid] ORDER BY o.oid) FILTER (WHERE {column} IN {ids(keys)})"

                comparison = (
                    "SELECT count(*),"
                    + ",".join(
                        [
                            selected("u.uid", u),
                            selected("p.pid", p),
                            selected("u.uid", controls),
                        ]
                    )
                    + tables
                )
                total, forward_ref, reverse_ref, control_ref = sql.execute(
                    comparison
                ).fetchone()
                expected = forward_ref or []
                assert (
                    rows
                    == expected
                    == (
                        [[r[1], r[0], r[2]] for r in records]
                        if stage == "products"
                        else []
                    )
                ), stage
                back = execute(c, reverse)
                assert back == (reverse_ref or []) == expected
                old_rows = execute(c, old_query)
                assert old_rows == control_ref and len(old_rows) == 10 * len(controls)
                assert cnt == total == n + len(expected)
                inspect_ids(c, (stage in ("users", "products"), stage == "products"))
                recheck = execute(
                    c,
                    f"CALL iceberg_csr_refresh('Purchased',{orders.current_snapshot().snapshot_id}) RETURN edge_count",
                )
                assert recheck == [[total]]
                if stage == "orders":
                    execute(old, "BEGIN TRANSACTION READ ONLY")
                    assert execute(
                        old, "MATCH ()-[:Purchased]->() RETURN count(*)"
                    ) == [[n]]
                if stage == "users":
                    execute(late, "BEGIN TRANSACTION READ ONLY")
                    assert execute(late, "MATCH (p:Product) RETURN count(*)") == [[np]]
                if stage == "products":
                    # The late reader pinned Iceberg but did not acquire an adjacency reader.
                    for watcher in (old, late):
                        assert (
                            execute(watcher, target) == []
                            and execute(watcher, reverse) == []
                        )
                        assert execute(
                            watcher, "MATCH ()-[:Purchased]->() RETURN count(*)"
                        ) == [[n]]
                        execute(watcher, "COMMIT")
                    oid_amount = []
                    execute(c, "BEGIN TRANSACTION READ ONLY")
                    for key in u:
                        q = f"MATCH (u:User)-[o:Purchased]->(:Product) WHERE u.uid={key} AND o.amt<10 RETURN o.oid,o.amt ORDER BY o.oid"
                        oid_amount.extend(execute(c, q))
                    execute(c, "COMMIT")
                    expected_amounts = sql.execute(
                        "SELECT o.oid,o.amt"
                        + tables
                        + f" WHERE u.uid IN {ids(u)} ORDER BY o.oid"
                    ).fetchall()
                    assert oid_amount == [list(t) for t in expected_amounts]
                stages.append(
                    dict(
                        stage=stage,
                        snapshots=pins,
                        total=cnt,
                        new_edges=len(rows),
                        native_rows=rows,
                        joined_rows=expected,
                        reverse_checked=True,
                        control_edges=old_rows,
                        stable_ids_checked=True,
                        first_result_ms=latency,
                        commit_unix_ns=commit_unix_ns,
                    )
                )
                save(root / "stages.json", stages)
                print(stage, len(rows), round(latency, 2), flush=True)
    assert (
        lake_versions(base / "lake/catalog.sqlite") == original
        and sidecar.stat().st_ino == inode
    )
    with Database(
        str(sidecar), read_only=True, buffer_pool_size=256 << 20, max_num_threads=1
    ) as db, Connection(db) as c:
        tables = execute(c, "CALL show_tables() RETURN id,name")
        rels = [t for t in tables if t[1].startswith("StableEdges_")]
        assert len(rels) == 1 and rels[0][0] == base_table[0][0], (rels, base_table)
        pending = next(t[1] for t in tables if t[1].startswith("Pending_"))
        assert execute(c, f"MATCH (o:{pending}) RETURN count(*)") == [[0]]
        versions = execute(
            c,
            "MATCH (v:StableGraphVersions) RETURN v.users,v.orders,v.products,v.edges ORDER BY v.orders,v.users,v.products",
        )
        assert versions == [
            [nu, n, np, n],
            [nu, n + 100, np, n],
            [nu + 100, n + 100, np, n],
            [nu + 100, n + 100, np + 100, n + 100],
        ]
    result = dict(
        orders=n,
        init_ms=init_ms,
        last_commit_to_100_ms=stages[-1]["first_result_ms"],
        query=target,
        base_rel=base_table[0],
        final_rel=rels[0],
        inode_unchanged=True,
        physical_versions=versions,
        pending_after=0,
        closed_sidecar_growth=sidecar.stat().st_size - db_size,
        original_catalog_unchanged=True,
    )
    save(root / "result.json", result)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("base", type=Path)
    ap.add_argument("output", type=Path)
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    a = ap.parse_args()
    base, root = a.base.resolve(), a.output.resolve()
    if a.worker:
        return run(base, root)
    root.mkdir(parents=True, exist_ok=False)
    rows = []
    for i in range(a.trials):
        path = root / f"trial-{i+1}"
        rc = profile(
            [
                sys.executable,
                __file__,
                str(base),
                str(path),
                "--worker",
            ],
            path,
            6,
        )
        if rc:
            raise RuntimeError(f"Trial failed ({rc}), see {path.with_suffix('.log')}")
        report = json.loads((path / "result.json").read_text())
        log = path.with_suffix(".log").read_text()
        assert (
            "checkpoint after a commit failed" not in log
            and "database file may be corrupted" not in log
        )
        assert (
            log.count("[stable_csr] adopted existing CSR") == 1
            and "built baseline" not in log
            and "starting full build" not in log
        )
        assert log.count("[stable_csr] delta") == 3
        for fragment in (
            "users=0 products=0 order_files=1 order_rows=100 queued=100 promoted=0",
            "users=100 products=0 order_files=0 order_rows=0 queued=0 promoted=0",
            "users=0 products=100 order_files=0 order_rows=0 queued=0 promoted=100",
        ):
            assert fragment in log, fragment
        published = re.findall(
            r"\[stable_csr\] graph published ms=([0-9.eE+-]+) unix_ns=(\d+)", log
        )
        assert len(published) == 4
        last = json.loads((path / "stages.json").read_text())[-1]
        ready_ms = (int(published[-1][1]) - last["commit_unix_ns"]) / 1e6
        assert 0 < ready_ms <= report["last_commit_to_100_ms"] + 0.1
        report["commit_to_csr_ready_ms"] = ready_ms
        report["final_graph_publish_ms"] = float(published[-1][0])
        report["peak_rss"] = json.loads(path.with_suffix(".memory.json").read_text())[
            "peak_rss"
        ]
        rows.append(report)
        save(root / "summary.json", rows)
        print("trial", i + 1, report["last_commit_to_100_ms"], flush=True)
    print("median ms", statistics.median(r["last_commit_to_100_ms"] for r in rows))


if __name__ == "__main__":
    main()
