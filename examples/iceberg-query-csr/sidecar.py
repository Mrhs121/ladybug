# /// script
# requires-python = ">=3.11"
# dependencies = ["pyiceberg[sql-sqlite]==0.11.1", "pyarrow==23.0.1"]
# ///
"""Verify mapped and native relationships in one database and persist only the mapped topology next to it."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from urllib.parse import unquote, urlparse

from run import (
    HERE,
    MATCH,
    Connection,
    Database,
    batch,
    create_fixture,
    execute,
    lake_versions,
    reference_join,
    register_mapping,
)

LOCAL = "MATCH (a:Local)-[r:Owns]->(b:Local) RETURN a.id,r.tag,b.id"
LOCAL_ROW = [[1, "main-only", 2]]


@contextmanager
def open_graph(path):
    with (
        Database(str(path), buffer_pool_size=128 << 20, max_num_threads=2) as db,
        Connection(db) as conn,
    ):
        yield db, conn


def check(conn, stage, pins):
    actual = execute(conn, MATCH)
    expected = reference_join(pins)
    if actual != expected:
        raise AssertionError(f"{stage}: MATCH {actual} JOIN {expected}")
    return {"stage": stage, "pins": pins, "rows": actual}


def exercise(root):
    path, users, products, orders = create_fixture(root)
    sidecar = root / "graph.db.csr.db"
    setting = "CALL iceberg_poc_catalog=" + json.dumps(str(path))
    checks = []
    with open_graph(root / "graph.db") as (db, conn):
        for ddl in (HERE / "schema-sidecar.cypher").read_text().split(";"):
            if ddl.strip():
                execute(conn, ddl)
        execute(conn, "CREATE NODE TABLE Local(id INT64 PRIMARY KEY)")
        execute(conn, "CREATE REL TABLE Owns(FROM Local TO Local, tag STRING)")
        execute(
            conn, "CREATE (:Local {id:1})-[:Owns {tag:'main-only'}]->(:Local {id:2})"
        )
        assert execute(conn, LOCAL) == LOCAL_ROW and not sidecar.exists()
        execute(conn, setting)
        pins = lake_versions(path)
        assert execute(conn, "MATCH (u:User) RETURN count(*)") == [[3]]
        assert (
            not sidecar.exists()
        )  # Reading mapped nodes alone does not require adjacency.
        checks.append(check(conn, "first", pins))
        checks.append(check(conn, "same snapshots, next transaction", pins))
        with orders.transaction() as change:
            change.set_properties({"poc-test": "metadata-only"})
        same = lake_versions(path)
        assert pins["orders"]["metadata"] != same["orders"]["metadata"]
        assert pins["orders"]["snapshot"] == same["orders"]["snapshot"]
        checks.append(check(conn, "new metadata, same snapshot", same))
        execute(conn, "BEGIN TRANSACTION READ ONLY")
        checks.append(check(conn, "pin old reader", same))
        users.append(batch("users", (7, "Zoe")))
        products.append(batch("products", (3, "Cable")))
        orders.append(batch("orders", (5002, 7, 3, 49)))
        new = lake_versions(path)

        def race_reader(i):
            with Connection(db) as other:
                execute(other, setting)
                return check(other, f"concurrent reader {i}", new)

        with ThreadPoolExecutor(2) as pool:
            checks.extend(pool.map(race_reader, range(2)))
        checks.append(check(conn, "old reader after publication", same))
        execute(conn, "COMMIT")
        checks.append(check(conn, "same connection after commit", new))
        users.upsert(batch("users", (11, "Ada Lovelace")), join_cols=["uid"])
        checks.append(check(conn, "only users changed", lake_versions(path)))
        orders.upsert(batch("orders", (7002, 42, 3, 26)), join_cols=["oid"])
        checks.append(check(conn, "edge moved", lake_versions(path)))
        edges = checks[-1]["rows"]
        peers = sorted(
            [
                [b[3]]
                for a in edges
                for b in edges
                if a[3] == 3 and b[3] != 3 and a[1] == b[1]
            ]
        )
        assert peers
        for pattern in (
            "(p:Product)<-[:Purchased]-(u:User)-[:Purchased]->(q:Product)",
            "(p:Product)-[:Purchased*1..2]-(q:Product)",
        ):
            assert (
                execute(
                    conn,
                    f"MATCH {pattern} WHERE p.pid=3 AND q.pid<>p.pid RETURN q.pid ORDER BY q.pid",
                )
                == peers
            )
        orders.delete("oid = 7001")
        checks.append(check(conn, "edge deleted", lake_versions(path)))
        products.delete("pid = 205")  # The order stays in the lake as a dangling edge.
        checks.append(check(conn, "only products changed", lake_versions(path)))
        mixed = execute(
            conn,
            """MATCH (a:Local)-[r:Owns]->(b:Local),
                    (u:User)-[o:Purchased]->(p:Product)
                    RETURN r.tag, o.oid, o.amt ORDER BY o.oid""",
        )
        assert mixed == [["main-only", r[0], r[-1]] for r in checks[-1]["rows"]]
        users.append(batch("users", (11, "duplicate key")))
        try:
            execute(conn, MATCH)
        except RuntimeError as error:
            assert "unique, non-null BIGINT key" in str(error)
        else:
            raise AssertionError("Invalid snapshot was accepted")
        assert execute(conn, LOCAL) == LOCAL_ROW
        users.delete("uid = 11")
        users.append(batch("users", (11, "Restored")))
        orders.delete("oid >= 0")
        checks.append(
            check(conn, "empty topology after failed build", lake_versions(path))
        )
        assert not checks[-1]["rows"]
        orders.append(batch("orders", *((10000 + i, 11, 101, i) for i in range(2300))))
        checks.append(
            check(
                conn,
                "one adjacency list spanning multiple batches",
                lake_versions(path),
            )
        )
        assert len(checks[-1]["rows"]) == 2300
        execute(conn, "CHECKPOINT")
    (root / "checks.json").write_text(json.dumps(checks, indent=2) + "\n")


def check_append_history(root):
    path, _, _, orders = create_fixture(root)
    setting = "CALL iceberg_poc_catalog=" + json.dumps(str(path))
    original = lake_versions(path)
    with open_graph(root / "graph.db") as (db, main), Connection(db) as late:
        for ddl in (HERE / "schema-sidecar.cypher").read_text().split(";"):
            if ddl.strip():
                execute(main, ddl)
        for conn in (main, late):
            execute(conn, setting)
            execute(conn, "BEGIN TRANSACTION READ ONLY")
        check(main, "original", original)
        assert execute(late, "MATCH (p:Product) RETURN count(*)") == [[2]]
        old_files = [
            Path(unquote(urlparse(t.file.file_path).path))
            for t in orders.scan().plan_files()
        ]
        orders.append(
            batch("orders", (8000, 11, 205, 10))
        )  # Interleaves old sorted row locators.
        mid = orders.current_snapshot().snapshot_id
        orders.append(batch("orders", (9500, 42, 101, 11)))
        head = orders.current_snapshot().snapshot_id

        def refresh(target):
            with Connection(db) as worker:
                execute(worker, setting)
                return execute(
                    worker,
                    f"CALL iceberg_csr_refresh('Purchased',{target}) RETURN snapshot_id,edge_count",
                )

        # Physically withhold private historical order data. Metadata is available to both workers.
        # A CSR refresh that reads any old order Parquet file cannot pass this check.
        try:
            for f in old_files:
                f.rename(f.with_suffix(".held"))
            with ThreadPoolExecutor(2) as pool:
                assert list(pool.map(refresh, [head, head])) == [
                    [[head, 5]],
                    [[head, 5]],
                ]
            assert refresh(mid) == [[mid, 4]]
        finally:
            for f in old_files:
                if f.with_suffix(".held").exists():
                    f.with_suffix(".held").rename(f)
        for conn in (main, late):
            check(conn, "old snapshot after two appends", original)
            count = execute(conn, "MATCH ()-[:Purchased]->() RETURN count(*)")
            assert count == [[3]], (count, "original" if conn is main else "late")
            execute(conn, "COMMIT")
            check(conn, "interleaved order keys", lake_versions(path))
            assert execute(
                conn,
                "MATCH ()-[r:Purchased]->() RETURN offset(id(r)),r.oid ORDER BY r.oid",
            ) == [[i, k] for i, k in enumerate([7001, 7002, 8000, 9003, 9500])]
        orders.append(batch("orders", (9600, 11, 101, 1)))
        valid = orders.current_snapshot().snapshot_id
        orders.append(batch("orders", (7001, 42, 205, 2)))
        try:
            refresh(orders.current_snapshot().snapshot_id)
        except RuntimeError as err:
            assert "unique, non-null BIGINT key" in str(err)
        else:
            raise AssertionError("Duplicate order ID was accepted")
        assert refresh(valid) == [[valid, 6]]
        orders.delete("oid = 7001")
        try:
            refresh(orders.current_snapshot().snapshot_id)
        except RuntimeError as err:
            assert "Full CSR rebuild required" in str(err)
        else:
            raise AssertionError("Delete was accepted by append-only refresh")
        check(main, "ordinary-query fallback after a delete", lake_versions(path))


def check_attached_refresh(root):
    catalog_path, _, _, orders = create_fixture(root)
    graph = root / "graph.db"
    baseline = lake_versions(catalog_path)
    with open_graph(graph) as (_, conn):
        register_mapping(conn, catalog_path, HERE / "schema-stable.cypher")
        check(conn, "direct open", baseline)

    with open_graph(":memory:") as (db, reader), Connection(db) as worker:
        # Both connections use the attached catalog, with a different table at the same ID in main.
        execute(reader, "CREATE NODE TABLE Other(k INT64 PRIMARY KEY)")
        execute(reader, "CREATE NODE TABLE OtherDest(k INT64 PRIMARY KEY)")
        execute(reader, "CREATE REL TABLE OtherEdges(FROM Other TO OtherDest)")
        execute(reader, "ATTACH " + json.dumps(str(graph)) + " AS demo (DBTYPE lbug)")
        execute(reader, "USE demo")
        for conn in (reader, worker):
            execute(conn, "CALL iceberg_poc_catalog=" + json.dumps(str(catalog_path)))
        execute(reader, "BEGIN TRANSACTION READ ONLY")
        check(reader, "attached reader before append", baseline)
        orders.append(batch("orders", (9100, 42, 205, 55)))
        latest = lake_versions(catalog_path)
        snapshot = latest["orders"]["snapshot"]
        assert execute(
            worker, f"CALL iceberg_csr_refresh('Purchased',{snapshot}) RETURN snapshot_id, edge_count"
        ) == [[snapshot, 4]]
        check(reader, "attached reader still pinned", baseline)
        execute(reader, "COMMIT")
        check(reader, "attached reader next transaction", latest)


def reopen(root, native_only):
    with open_graph(root / "graph.db") as (_, conn):
        assert execute(conn, LOCAL) == LOCAL_ROW
        if native_only:
            assert not (root / "graph.db.csr.db").exists()
            return
        path = root / "catalog.sqlite"
        execute(conn, "CALL iceberg_poc_catalog=" + json.dumps(str(path)))
        check(conn, "fresh process", lake_versions(path))
        main_tables = execute(conn, "CALL show_tables() RETURN name ORDER BY name")
        assert {r[0] for r in main_tables} == {
            "User",
            "Product",
            "Purchased",
            "Local",
            "Owns",
        }
        main_pages = execute(conn, "CALL storage_info('Owns') RETURN column_name")
        assert ["fwd_NBR_ID"] in main_pages
        mapped_main_pages = {
            name: execute(
                conn,
                f"CALL storage_info('{name}') WHERE num_pages > 0 RETURN column_name,num_pages",
            )
            for name in ("User", "Product", "Purchased")
        }
        assert not any(mapped_main_pages.values())
    with (
        Database(
            str(root / "graph.db.csr.db"), read_only=True, buffer_pool_size=64 << 20
        ) as db,
        Connection(db) as conn,
    ):
        names = [
            x[0] for x in execute(conn, "CALL show_tables() RETURN name ORDER BY name")
        ]
        generations = [n for n in names if re.fullmatch("IceEdges_[0-9a-f]{64}", n)]
        assert set(names) == {"CsrSlots", *generations} | (
            {"CsrVersions"} if "CsrVersions" in names else set()
        )
        assert execute(conn, "CALL table_info('CsrSlots') RETURN name") == [["pos"]]
        for name in generations:
            assert execute(conn, f"CALL table_info('{name}') RETURN name") == [
                ["lake_row"],
                ["lake_key"],
            ]
        sample = next(
            n
            for n in generations
            if execute(conn, f"MATCH ()-[e:{n}]->() RETURN count(*)")[0][0] >= 2300
        )
        pages = execute(
            conn,
            f"CALL storage_info('{sample}') RETURN column_name,start_page_idx,num_pages",
        )
        assert {
            "fwd_csr_offset",
            "fwd_csr_length",
            "bwd_csr_offset",
            "bwd_csr_length",
            "fwd_NBR_ID",
            "bwd_NBR_ID",
            "fwd_lake_row",
            "fwd_lake_key",
        } <= {r[0] for r in pages}
    (root / "physical.json").write_text(
        json.dumps(
            {
                "main_tables": main_tables,
                "mapped_data_pages_in_main": mapped_main_pages,
                "sidecar_tables": names,
                "sample_csr_pages": pages,
            },
            indent=2,
        )
        + "\n"
    )


def stable_recovery(root, seed):
    """Lose the process after the pending rows commit, then promote without old Parquet files."""
    from pyiceberg.catalog.sql import SqlCatalog

    if seed:
        path, users, products, orders = create_fixture(root)
    else:
        path = root / "catalog.sqlite"
        cat = SqlCatalog("poc", uri="sqlite:///" + str(path))
        users, products, orders = (
            cat.load_table("ecom." + t) for t in ("users", "products", "orders")
        )
        # Opening the native sidecar also exercises normal WAL recovery after os._exit.
        with Database(
            str(root / "graph.db.csr.db"), buffer_pool_size=256 << 20
        ) as db, Connection(db) as disk:
            name = execute(
                disk, "CALL show_tables() WHERE name STARTS WITH 'Pending_' RETURN name"
            )[0][0]
            assert execute(disk, f"MATCH (o:{name}) RETURN o.oid ORDER BY o.oid") == [
                [10000],
                [10001],
                [10002],
                [10003],
            ]

    def sid():
        return orders.current_snapshot().snapshot_id

    with open_graph(root / "graph.db") as (db, c), Connection(db) as pinned:
        setting = "CALL iceberg_poc_catalog=" + json.dumps(str(path))
        for conn in (c, pinned):
            execute(conn, setting)

        def refresh():
            return execute(
                c, f"CALL iceberg_csr_refresh('Purchased',{sid()}) RETURN edge_count"
            )[0][0]

        if seed:
            for ddl in (HERE / "schema-stable.cypher").read_text().split(";"):
                if ddl.strip():
                    execute(c, ddl)
            assert refresh() == 3
            execute(c, "CHECKPOINT")
            orders.append(
                batch(
                    "orders",
                    (10000, 12, 102, 1),
                    (10001, 12, 103, 2),
                    (10002, 42, 103, 3),
                    (10003, 13, 205, 4),
                    (10004, 11, 101, 5),
                )
            )
            assert refresh() == 4
            os._exit(0)
        initial = lake_versions(path)
        execute(pinned, "BEGIN TRANSACTION READ ONLY")
        assert execute(pinned, "MATCH (u:User) RETURN count(*)") == [[3]]
        old_data = [
            Path(unquote(urlparse(f.file.file_path).path))
            for t in (users, products, orders)
            for f in t.scan().plan_files()
        ]
        checks = []
        for label, table, rows, expected in [
            ("users", users, batch("users", (13, "Lee"), (12, "Jo")), 5),
            ("product_102", products, batch("products", (102, "Cable")), 6),
            ("product_103", products, batch("products", (103, "Pen")), 8),
        ]:
            table.append(rows)
            try:
                for f in old_data:
                    f.rename(f.with_suffix(".held"))
                assert refresh() == refresh() == expected
            finally:
                for f in old_data:
                    if f.with_suffix(".held").exists():
                        f.with_suffix(".held").rename(f)
            checks.append(check(c, label, lake_versions(path)))
            check(pinned, "snapshot pinned before both endpoints arrived", initial)
        execute(pinned, "COMMIT")
        all_orders = checks[-1]["rows"]
        for anchor in (12, 13):
            peers = execute(
                c,
                "MATCH (u:User)-[:Purchased]->(p:Product)<-[:Purchased]-(v:User) "
                f"WHERE u.uid={anchor} AND v.uid<>u.uid RETURN DISTINCT v.uid ORDER BY v.uid",
            )
            bought = {r[3] for r in all_orders if r[1] == anchor}
            peer_ids = sorted(
                {r[1] for r in all_orders if r[3] in bought and r[1] != anchor}
            )
            assert peers == [[k] for k in peer_ids] == [[42]]
        src = execute(c, "MATCH (v:User) RETURN v.uid,offset(id(v)) ORDER BY v.uid")
        assert src == [[11, 0], [12, 3], [13, 4], [42, 1], [99, 2]]
        # A duplicate retry of an invalid Iceberg append cannot advance the graph head.
        execute(pinned, "BEGIN TRANSACTION READ ONLY")
        good = lake_versions(path)
        check(pinned, "before invalid append", good)
        orders.append(batch("orders", (10000, 13, 103, 9)))
        try:
            refresh()
        except RuntimeError as error:
            assert "Duplicate order key" in str(error)
        else:
            raise AssertionError("Accepted an existing order key")
        check(pinned, "after rejected append", good)
        execute(pinned, "COMMIT")
        (root / "checks.json").write_text(json.dumps(checks, indent=2) + "\n")


def check_stable_capacity(root):
    from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
    from pyiceberg.typedef import Record

    path, users, _, orders = create_fixture(root)
    limit = 1 << 62
    count = "MATCH ()-[:Purchased]->() RETURN count(*)"
    with open_graph(root / "graph.db") as (db, conn), Connection(db) as old:
        for c in (conn, old):
            execute(c, "CALL iceberg_poc_catalog=" + json.dumps(str(path)))
        for ddl in (HERE / "schema-stable.cypher").read_text().split(";"):
            if ddl.strip():
                execute(conn, ddl)
        assert execute(conn, count) == [[3]]
        execute(old, "BEGIN TRANSACTION READ ONLY")
        assert execute(old, count) == [[3]]
        for table in (users, orders):
            before = table.current_snapshot()
            # A synthetic manifest count exercises the admission check without allocating rows.
            file = DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=(root / "oversized.parquet").as_uri(),
                file_format=FileFormat.PARQUET,
                partition=Record(),
                record_count=limit + 1 - int(before.summary["total-records"]),
                file_size_in_bytes=1,
            )
            file.spec_id = table.spec().spec_id
            with (
                table.transaction() as txn,
                txn.update_snapshot().fast_append() as append,
            ):
                append.append_data_file(file)
            assert int(table.current_snapshot().summary["total-records"]) == limit + 1
            for _ in range(2):
                try:
                    execute(conn, count)
                except RuntimeError as err:
                    assert "Stable CSR row count outside supported capacity" in str(err)
                else:
                    raise AssertionError("Published an oversized stable CSR")
            assert execute(old, count) == [[3]]
            table.manage_snapshots().rollback_to_snapshot(before.snapshot_id).commit()

        max_key = (1 << 63) - 1
        users.append(batch("users", (max_key, "Max key")))
        orders.append(batch("orders", (10001, max_key, 101, 9)))
        assert execute(conn, count) == [[4]]
        assert execute(
            conn, f"MATCH (u:User {{uid:{max_key}}}) RETURN offset(id(u))"
        ) == [[3]]
        assert execute(old, count) == [[3]]
        execute(old, "COMMIT")
        assert execute(old, count) == [[4]]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--worker",
        choices=["create", "native", "reopen", "stable-seed", "stable-finish"],
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()
    base = args.output.resolve()
    if args.worker:
        if args.worker.startswith("stable-"):
            stable_recovery(
                base / "stable-recovery", args.worker == "stable-seed"
            )
        elif args.worker == "create":
            exercise(base)
        else:
            reopen(base, args.worker == "native")
        return
    base.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="sidecar-", dir=base))
    print(root, flush=True)

    def worker(mode):
        log = root / f"{mode}.log"
        with log.open("w") as stream:
            result = subprocess.run(
                [
                    sys.executable,
                    __file__,
                    "--output",
                    str(root),
                    "--worker",
                    mode,
                ],
                stdout=stream,
                stderr=subprocess.STDOUT,
            )
        if result.returncode:
            raise RuntimeError(f"{mode} failed, see {log}")
        return log.read_text()

    worker("stable-seed")
    worker("stable-finish")
    check_stable_capacity(root / "capacity")
    check_append_history(root / "append-history")
    check_attached_refresh(root / "attached-refresh")
    created = worker("create")
    sidecar = root / "graph.db.csr.db"
    hidden = root / "withheld.db"
    sidecar.rename(hidden)
    try:
        assert "[iceberg_csr]" not in worker(
            "native"
        )  # Fresh process, no lake configuration.
        assert not sidecar.exists()
    finally:
        os.replace(hidden, sidecar)
    restarted = worker("reopen")
    checks = json.loads((root / "checks.json").read_text())
    versions = {
        tuple(
            row["pins"][table]["snapshot"] for table in ("users", "orders", "products")
        )
        for row in checks
    }
    built = re.findall(r"\[iceberg_csr\] built (\w+)", created)
    appended = re.findall(r"\[iceberg_csr\] published append (\w+)", created)
    assert len(built) + len(appended) == len(set(built + appended)) == len(versions)
    assert (
        "[iceberg_csr] reused" in restarted and "[iceberg_csr] built" not in restarted
    )
    physical = json.loads((root / "physical.json").read_text())
    assert set(built) == set(physical["sidecar_tables"]) - {"CsrSlots", "CsrVersions"}
    print(
        f"{len(checks)} snapshot JOIN checks, {len(built)} full builds, {len(appended)} append publications, native-only and warm restart passed"
    )


if __name__ == "__main__":
    main()
