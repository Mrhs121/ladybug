# /// script
# requires-python = ">=3.11"
# dependencies = ["pyiceberg[sql-sqlite]==0.11.1","pyarrow==23.0.1","duckdb==1.5.5","psutil>=6"]
# ///
"""Measure one 100-order commit per independent copy of the original benchmark."""
import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

import duckdb
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

from large_benchmark import profile
from large_build import iceberg
from run import SCHEMAS, Connection, Database, execute, lake_versions


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def fork_fixture(base, root, *, writable=("orders",), copy_main=True):
    """Share immutable old lake files. The private SQL catalog and order writer are independent."""
    root.mkdir(parents=True, exist_ok=False)
    original = lake_versions(base / "lake/catalog.sqlite")
    cat = SqlCatalog(
        "poc",
        uri="sqlite:///" + str(root / "catalog.sqlite"),
        warehouse=(root / "warehouse").as_uri(),
    )
    cat.create_namespace("ecom")
    for name, pin in original.items():
        location = pin["metadata"]
        if name in writable:
            old = Path(unquote(urlparse(location).path))
            meta = json.loads(old.read_text())
            dest = root / "warehouse/ecom" / name
            assert not any(
                k.startswith(
                    (
                        "write.data.path",
                        "write.metadata.path",
                        "write.object-storage.path",
                    )
                )
                for k in meta.get("properties", {})
            )
            meta["location"] = dest.as_uri()
            (dest / "metadata").mkdir(parents=True)
            copy = dest / "metadata" / old.name
            save(copy, meta)
            location = copy.as_uri()
        cat.register_table(("ecom", name), location)
    for name in ("graph.db", "graph.db.csr.db") if copy_main else ("graph.db.csr.db",):
        src = base / "mapped" / name
        if sys.platform == "darwin":
            subprocess.run(["cp", "-c", src, root / name], check=True)
        else:
            shutil.copy2(src, root / name)
    return cat.load_table("ecom.orders"), original


def native_tables(path):
    with Database(
        str(path), read_only=True, buffer_pool_size=256 << 20, max_num_threads=1
    ) as db, Connection(db) as conn:
        return execute(
            conn,
            "CALL show_tables() WHERE name STARTS WITH 'IceEdges_' RETURN id,name ORDER BY id",
        )


def worker(args):
    root, base = args.output, args.base
    table, original = fork_fixture(base, root)
    sidecar = root / "graph.db.csr.db"
    tables = native_tables(sidecar)
    assert len(tables) == 1
    before = sidecar.stat()
    fixture = json.loads((base / "lake/fixture.json").read_text())
    orders = fixture["counts"]["orders"]
    uid = fixture["uid"]
    first = 100003 + 13 * orders
    data = [
        dict(oid=first + 13 * i, uid=uid, pid=503 + 11 * i, amt=1 + i % 9)
        for i in range(100)
    ]
    expected = [[r["oid"], r["amt"]] for r in data]
    payload = pa.Table.from_pylist(data, schema=SCHEMAS["orders"])
    query = f"MATCH (u:User)-[o:Purchased]->(:Product) WHERE u.uid={uid} AND o.amt<10 RETURN o.oid,o.amt ORDER BY o.oid"
    reverse = "MATCH (:User)-[o:Purchased]->(p:Product) WHERE p.pid=503 AND o.amt<10 RETURN o.oid,o.amt ORDER BY o.oid"
    setting = "CALL iceberg_poc_catalog=" + json.dumps(str(root / "catalog.sqlite"))
    now = time.perf_counter

    def ms(start):
        return (now() - start) * 1000

    with (
        Database(
            str(root / "graph.db"), buffer_pool_size=2 << 30, max_num_threads=1
        ) as db,
        Connection(db) as serving,
        Connection(db) as watcher,
        Connection(db) as writer,
    ):
        for c in (serving, watcher, writer):
            execute(c, setting)
            execute(c, "CALL threads=1")
        old_id = original["orders"]["snapshot"]
        old_refresh = f"CALL iceberg_csr_refresh('Purchased',{old_id}) RETURN snapshot_id,edge_count"
        execute(watcher, "BEGIN TRANSACTION READ ONLY")
        if args.cold_keys:
            assert (
                execute(serving, old_refresh)
                == execute(watcher, old_refresh)
                == [[old_id, orders]]
            )
        else:
            assert execute(serving, query) == execute(watcher, reverse) == []
        append_start = now()
        table.append(payload)
        committed = now()
        snapshot = table.current_snapshot()
        sid = snapshot.snapshot_id
        refresh = f"CALL iceberg_csr_refresh('Purchased',{sid}) RETURN snapshot_id,edge_count"
        receipt = [[sid, orders + 100]]
        refresh_ms = None
        if args.mode == "refresh":
            assert execute(writer, refresh) == receipt
            refresh_ms = ms(committed)
        first_query = now()
        rows = execute(serving, query)
        returned = now()
        assert rows == expected
        # Same serving connection across writer commit and both reads, original watcher still pinned.
        assert execute(watcher, reverse) == []
        execute(watcher, "COMMIT")
        assert execute(watcher, reverse) == expected[:1]
        repeat = now()
        assert execute(writer, refresh) == receipt
        replay_ms = ms(repeat)
        assert execute(serving, query) == expected
    new = lake_versions(root / "catalog.sqlite")
    assert all(new[k] == original[k] for k in ("users", "products"))
    assert (
        snapshot.parent_snapshot_id == original["orders"]["snapshot"]
        and sid == new["orders"]["snapshot"]
    )
    assert (
        snapshot.summary.operation.value == "append"
        and int(snapshot.summary["added-records"]) == 100
    )
    with duckdb.connect() as sql:
        sql.execute("LOAD iceberg")
        result = sql.execute(
            f"""SELECT o.oid,o.amt FROM {iceberg(new,'orders')} o
            JOIN {iceberg(new,'users')} u ON u.uid=o.uid JOIN {iceberg(new,'products')} p ON p.pid=o.pid
            WHERE u.uid=? AND o.amt<10 ORDER BY o.oid""",
            [uid],
        ).fetchall()
        assert [list(r) for r in result] == rows
    assert (
        lake_versions(base / "lake/catalog.sqlite") == original
        and sidecar.stat().st_ino == before.st_ino
    )
    assert native_tables(sidecar) == tables
    with Database(
        str(sidecar), read_only=True, buffer_pool_size=256 << 20, max_num_threads=1
    ) as db, Connection(db) as conn:
        versions = execute(
            conn,
            "MATCH (v:CsrVersions) RETURN v.row_end,v.edge_count,v.is_head ORDER BY v.row_end",
        )
        assert versions == [[orders, orders, False], [orders + 100, orders + 100, True]]
        assert execute(conn, f"MATCH ()-[r:{tables[0][1]}]->() RETURN count(*)") == [
            [orders + 100]
        ]
    result = dict(
        mode=args.mode,
        cold_endpoint_keys=args.cold_keys,
        pid=os.getpid(),
        query=query,
        orders_before=orders,
        append_ms=(committed - append_start) * 1000,
        commit_to_results_ms=(returned - committed) * 1000,
        query_ms=(returned - first_query) * 1000,
        commit_to_refresh_ms=refresh_ms,
        replay_ms=replay_ms,
        old_snapshot=snapshot.parent_snapshot_id,
        snapshots=new,
        added_files=int(snapshot.summary["added-data-files"]),
        native_table=tables[0],
        sidecar_bytes_before=before.st_size,
        sidecar_bytes_after=sidecar.stat().st_size,
        versions=versions,
        matched_join_rows=rows,
        reverse_and_old_reader_checked=True,
    )
    save(root / "result.json", result)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("base", type=Path)
    ap.add_argument("output", type=Path)
    ap.add_argument(
        "--cold-keys",
        action="store_true",
        help="Leave the endpoint key indexes cold until the append",
    )
    ap.add_argument("--trials", type=int, default=3)
    ap.add_argument("--mode", choices=("query", "refresh"), help=argparse.SUPPRESS)
    args = ap.parse_args()
    args.base, args.output = args.base.resolve(), args.output.resolve()
    if args.mode:
        worker(args)
        return
    args.output.mkdir(parents=True, exist_ok=False)
    results = []
    for trial in range(args.trials):
        for mode in ("query", "refresh"):
            path = args.output / f"{mode}-{trial+1}"
            os.environ["LBUG_ICEBERG_POC_PROFILE"] = "1"
            rc = profile(
                [
                    sys.executable,
                    __file__,
                    str(args.base),
                    str(path),
                    "--mode",
                    mode,
                    *(["--cold-keys"] if args.cold_keys else []),
                ],
                path,
                6,
            )
            if rc:
                raise RuntimeError(
                    f"{mode} failed ({rc}), see {path.with_suffix('.log')}"
                )
            log = path.with_suffix(".log").read_text()
            assert (
                "starting full build" not in log
                and log.count("[iceberg_csr] published append") == 1
            )
            assert log.count("data_files=1 order_rows=100 native_edges=100") == 1
            row = json.loads((path / "result.json").read_text())
            row["native_publish_ms"] = float(
                log.split("published append ")[1].split("ms=")[1].splitlines()[0]
            )
            mem = json.loads(path.with_suffix(".memory.json").read_text())
            row["peak_rss_bytes"] = mem["peak_rss"]
            results.append(
                {
                    k: v
                    for k, v in row.items()
                    if k not in ("matched_join_rows", "snapshots", "query")
                }
            )
            print(
                mode, trial + 1, round(row["commit_to_results_ms"], 2), "ms", flush=True
            )
            save(args.output / "summary.json", results)
    for mode in ("query", "refresh"):
        print(
            mode,
            "median commit to result (ms)",
            statistics.median(
                r["commit_to_results_ms"] for r in results if r["mode"] == mode
            ),
        )


if __name__ == "__main__":
    main()
