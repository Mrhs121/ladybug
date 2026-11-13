# /// script
# requires-python = ">=3.11"
# dependencies = ["pyiceberg[sql-sqlite]==0.11.1", "pyarrow==23.0.1"]
# ///
"""Exercise the same lbug MATCH across real commits to three Iceberg v2 tables."""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
os.environ["LBUG_PYTHON_BACKEND"] = "capi"
from tools.python_api.src_py import Connection, Database  # noqa: E402

MATCH = """
MATCH (u:User)-[o:Purchased]->(p:Product)
RETURN o.oid, u.uid, u.uname, p.pid, p.pname, o.amt ORDER BY o.oid
"""

SCHEMAS = {
    "users": pa.schema([("uid", pa.int64()), ("uname", pa.string())]),
    "products": pa.schema([("pid", pa.int64()), ("pname", pa.string())]),
    "orders": pa.schema([(k, pa.int64()) for k in ("oid", "uid", "pid", "amt")]),
}


def batch(table, *rows):
    schema = SCHEMAS[table]
    return pa.Table.from_pylist(
        [dict(zip(schema.names, row, strict=True)) for row in rows], schema
    )


def lake_versions(catalog_path):
    # An immutable metadata file identifies the exact table state read by iceberg_scan.
    with sqlite3.connect(catalog_path) as con:
        pointers = con.execute(
            "SELECT table_name, metadata_location FROM iceberg_tables "
            "WHERE catalog_name='poc' AND table_namespace='ecom' ORDER BY table_name"
        ).fetchall()
    versions = {}
    for name, location in pointers:
        metadata = json.loads(Path(unquote(urlparse(location).path)).read_text())
        versions[name] = {
            "metadata": location,
            "snapshot": metadata["current-snapshot-id"],
        }
    return versions


def join_sql(versions, uid=None, *, ordered=True):
    def scan(table):
        pointer = versions[table]
        path = pointer["metadata"].replace("'", "''")
        return f"iceberg_scan('{path}', snapshot_from_id={pointer['snapshot']})"

    # Independent SQL over raw Iceberg snapshots. No edge offsets or CSR are inputs.
    return f"""
        SELECT o.oid, u.uid, u.uname, p.pid, p.pname, o.amt
        FROM {scan("orders")} o
        JOIN {scan("users")} u ON o.uid = u.uid
        JOIN {scan("products")} p ON o.pid = p.pid
        {"" if uid is None else f"WHERE u.uid = {int(uid)}"}
        {"ORDER BY o.oid" if ordered else ""}
    """


def reference_join(versions, uid=None):
    result = subprocess.run(
        [
            "duckdb",
            "-no-init",
            "-batch",
            "-json",
            "-c",
            "LOAD iceberg; " + join_sql(versions, uid),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return [
        [r[k] for k in ("oid", "uid", "uname", "pid", "pname", "amt")]
        for r in json.loads(result.stdout.strip() or "[]")
    ]


def execute(conn, cypher):
    with conn.execute(cypher) as result:
        return list(result)


def register_mapping(conn, catalog_path, schema=HERE / "schema.cypher"):
    execute(conn, f"CALL iceberg_poc_catalog={json.dumps(str(catalog_path))}")
    for statement in schema.read_text().split(";"):
        if statement.strip():
            execute(conn, statement)


def create_fixture(root):
    root.mkdir(parents=True, exist_ok=True)
    catalog_path = root / "catalog.sqlite"
    catalog = SqlCatalog(
        "poc", uri=f"sqlite:///{catalog_path}", warehouse=(root / "warehouse").as_uri()
    )
    catalog.create_namespace("ecom")
    users, products, orders = [
        catalog.create_table(
            ("ecom", table), schema, properties={"format-version": "2"}
        )
        for table, schema in SCHEMAS.items()
    ]
    users.append(batch("users", (42, "Lin"), (11, "Ada"), (99, "No orders")))
    products.append(batch("products", (205, "Keyboard"), (101, "Mouse")))
    orders.append(
        batch("orders", (9003, 42, 205, 180), (7002, 11, 101, 26), (7001, 11, 101, 25))
    )
    return catalog_path, users, products, orders


def run(root):
    catalog_path, users, products, orders = create_fixture(root)
    checks = []
    with (
        Database(":memory:", buffer_pool_size=128 << 20, max_num_threads=2) as db,
        Connection(db) as conn,
    ):
        register_mapping(conn, catalog_path)

        def check(stage, expected, pinned=None):
            versions = lake_versions(catalog_path) if pinned is None else pinned
            actual = execute(conn, MATCH)
            # This fixture owns the only writer. No commit may intervene between selecting
            # the comparison version and executing an autocommit MATCH.
            if pinned is None and lake_versions(catalog_path) != versions:
                raise AssertionError(
                    "Iceberg catalog changed during the autocommit comparison"
                )
            oracle = reference_join(versions)
            if actual != oracle or actual != expected:
                raise AssertionError(
                    f"{stage}\nMATCH {actual}\nJOIN {oracle}\nexpected {expected}"
                )
            checks.append(
                {"stage": stage, "versions": versions, "match": actual, "join": oracle}
            )
            print(f"{stage:25} MATCH = JOIN   {len(actual)} rows", flush=True)

        original = [
            [7001, 11, "Ada", 101, "Mouse", 25],
            [7002, 11, "Ada", 101, "Mouse", 26],
            [9003, 42, "Lin", 205, "Keyboard", 180],
        ]
        check("initial", original)
        execute(conn, "BEGIN TRANSACTION READ ONLY")
        before = lake_versions(catalog_path)
        check("pin before append", original, before)
        # The same open lbug transaction stays on the original three snapshots.
        users.append(batch("users", (7, "Zoe")))
        products.append(batch("products", (3, "Cable")))
        orders.append(batch("orders", (5002, 7, 3, 49)))
        check("same txn after append", original, before)
        execute(conn, "COMMIT")

        expected = [[5002, 7, "Zoe", 3, "Cable", 49], *original]
        check("append visible", expected)

        # Only properties change; every edge retains its endpoints and identity.
        users.upsert(batch("users", (11, "Ada Lovelace")), join_cols=["uid"])
        products.upsert(batch("products", (101, "Wireless Mouse")), join_cols=["pid"])
        orders.upsert(batch("orders", (7001, 11, 101, 125)), join_cols=["oid"])
        expected = [
            expected[0],
            [7001, 11, "Ada Lovelace", 101, "Wireless Mouse", 125],
            [7002, 11, "Ada Lovelace", 101, "Wireless Mouse", 26],
            expected[3],
        ]
        check("properties changed", expected)

        orders.upsert(batch("orders", (7002, 42, 3, 26)), join_cols=["oid"])
        expected = [
            expected[0],
            expected[1],
            [7002, 42, "Lin", 3, "Cable", 26],
            expected[3],
        ]
        check("edge endpoints changed", expected)

        orders.delete("oid = 7001")
        expected = [expected[0], expected[2], expected[3]]
        check("edge deleted", expected)

        # The order remains in Iceberg but no longer has a destination node.
        products.delete("pid = 205")
        check("destination deleted", expected[:2])
    return {
        "lbug_version": Database.get_version(),
        "connections": 1,
        "cypher": MATCH.strip(),
        "columns": ["oid", "uid", "uname", "pid", "pname", "amt"],
        "checks": checks,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, help="write a report and keep the temporary lake here"
    )
    args = parser.parse_args()
    if args.output:
        args.output.mkdir(parents=True, exist_ok=True)
        root = Path(tempfile.mkdtemp(prefix="run-", dir=args.output.resolve()))
        report = run(root)
        path = root / "results.json"
        path.write_text(json.dumps(report, indent=2) + "\n")
        print(path)
    else:
        with tempfile.TemporaryDirectory() as tmp:
            run(Path(tmp))


if __name__ == "__main__":
    main()
