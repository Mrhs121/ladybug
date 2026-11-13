#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyiceberg[sql-sqlite]==0.11.1", "pyarrow==23.0.1", "numpy>=1.26", "duckdb==1.5.5"]
# ///
"""Prepare the Iceberg input for the native lbug CLI and append 100 real orders per call."""

import argparse
import fcntl
import json
import time
from pathlib import Path

import duckdb
from pyiceberg.catalog.sql import SqlCatalog

from large_build import iceberg
from large_fixture import generate
from run import batch, join_sql, lake_versions

DEFAULT_DIR = Path(__file__).resolve().parents[2] / "build/iceberg-csr-poc/native-cli-10m"


def append_orders(db, lake, fixture):
    path = lake / "catalog.sqlite"
    catalog = SqlCatalog("poc", uri=f"sqlite:///{path}", warehouse=(lake / "warehouse").as_uri())
    with (lake / ".append.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        table = catalog.load_table(("ecom", "orders"))
        pins = lake_versions(path)
        previous = db.execute(f"SELECT max(oid) FROM {iceberg(pins, 'orders')}").fetchone()[0]
        rows = [
            (previous + 13 * (i + 1), fixture["uid"], 503 + 11 * i, 1 + i % 9)
            for i in range(100)
        ]
        payload = batch("orders", *rows)
        started = time.perf_counter()
        table.append(payload)
        committed_ns = time.time_ns()
        append_ms = (time.perf_counter() - started) * 1000
        return dict(
            snapshot=table.current_snapshot().snapshot_id,
            orders_added=len(rows),
            uid=fixture["uid"],
            first_oid=rows[0][0],
            last_oid=rows[-1][0],
            iceberg_append_ms=round(append_ms, 3),
            commit_return_unix_ns=committed_ns,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["init", "append", "check"])
    parser.add_argument("--dir", type=Path, default=DEFAULT_DIR)
    args = parser.parse_args()
    lake = args.dir.resolve() / "lake"
    if args.action == "init":
        generate(lake, 10_000_000)
        return
    path = lake / "catalog.sqlite"
    if not (lake / "fixture.json").exists():
        parser.error(f"找不到 {lake}/fixture.json，请先对新目录执行 init")
    fixture = json.loads((lake / "fixture.json").read_text())
    with duckdb.connect() as db:
        db.execute("SET threads=1")
        db.execute("LOAD iceberg")
        if args.action == "append":
            result = append_orders(db, lake, fixture)
        else:
            pins = lake_versions(path)
            first_new = 100003 + 13 * fixture["counts"]["orders"]
            source = join_sql(pins, ordered=False)
            counts = db.execute(
                f"SELECT count(*), count(*) FILTER (WHERE oid >= {first_new}), "
                f"max(oid) FROM ({source}) matched"
            ).fetchone()
            result = dict(
                snapshots={name: p["snapshot"] for name, p in pins.items()},
                joined_edges=counts[0], appended_edges=counts[1], max_oid=counts[2],
            )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
