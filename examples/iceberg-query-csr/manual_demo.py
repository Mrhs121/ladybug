#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyiceberg[sql-sqlite]==0.11.1", "pyarrow==23.0.1"]
# ///
"""Use one interactive lbug connection while a second terminal appends to Iceberg."""

import argparse
import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
WORK = ROOT / "build/iceberg-csr-poc/manual-demo"
LIBRARY = "liblbug.dylib" if sys.platform == "darwin" else "liblbug.so"
os.environ.setdefault("LBUG_C_API_LIB_PATH", str(ROOT / "build/partition-demo/src" / LIBRARY))

from run import SCHEMAS, SqlCatalog, batch, execute, lake_versions, reference_join
from sidecar import open_graph

BASE = {
    "users": [(12, "Jo"), (42, "Lin"), (77, "Mei")],
    "products": [(103, "Pen"), (205, "Book")],
    "orders": [(10001, 12, 103, 2), (10002, 42, 103, 3)],
}
ORDER_NODE = """CREATE NODE TABLE OrderRow(oid INT64 PRIMARY KEY, amt INT64)
WITH (storage='ecom.orders', format='iceberg_stable_csr')"""


def append_once(catalog, name, step, rows):
    table = catalog.load_table(("ecom", name))
    if any(s.summary and s.summary.get("lbug-demo-step") == step for s in table.snapshots()):
        print(f"{name} 已提交过这批数据。")
        return
    table.append(batch(name, *rows), snapshot_properties={"lbug-demo-step": step})
    print(f"{name} 新增 {len(rows)} 行，snapshot={table.current_snapshot().snapshot_id}")


def interactive(conn):
    if sys.stdin.isatty():
        import readline  # noqa: F401  Enable terminal line editing and history.
    print("输入 Cypher，以分号结束。:quit 退出。", flush=True)
    lines = []
    while True:
        try:
            line = input("lbug> " if not lines else "  ... ")
        except EOFError:
            return
        except KeyboardInterrupt:
            lines.clear()
            print()
            continue
        if not lines and line.strip() in (":quit", ":q"):
            return
        if not line.strip():
            continue
        lines.append(line)
        if not line.rstrip().endswith(";"):
            continue
        query = "\n".join(lines)
        lines.clear()
        try:
            results = conn.execute(query)
            if not isinstance(results, list):
                results = [results]
            for result in results:
                with result:
                    print(" | ".join(result.get_column_names()))
                    for row in result:
                        print(" | ".join("NULL" if v is None else str(v) for v in row))
        except RuntimeError as error:
            print(error, file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["init", "graph", "append", "product", "join"])
    parser.add_argument("--dir", type=Path, default=WORK, help="Demo data directory")
    args = parser.parse_args()
    work = args.dir.resolve()
    catalog_path = work / "catalog.sqlite"
    if args.action != "init" and not catalog_path.is_file():
        parser.error(f"{work} 尚未初始化，请先运行 init")
    if args.action == "init":
        work.mkdir(parents=True, exist_ok=True)
    if args.action in ("init", "append", "product"):
        catalog = SqlCatalog(
            "poc", uri=f"sqlite:///{catalog_path}", warehouse=(work / "warehouse").as_uri()
        )
        if args.action == "init":
            catalog.create_namespace_if_not_exists("ecom")
            for name, rows in BASE.items():
                catalog.create_table_if_not_exists(
                    ("ecom", name), SCHEMAS[name], properties={"format-version": "2"}
                )
                append_once(catalog, name, "base", rows)
        elif args.action == "append":
            users = [(20, "Rui")] + [(1000 + i, f"User {i}") for i in range(99)]
            append_once(catalog, "users", "new-users", users)
            orders = [(10003, 20, 103, 4), (10004, 20, 150, 5)]
            append_once(catalog, "orders", "new-orders", orders)
        else:
            append_once(catalog, "products", "late-product", [(150, "Cup")])
    if args.action in ("init", "graph"):
        with open_graph(work / "graph.db") as (_, conn):
            execute(conn, "CALL iceberg_poc_catalog=" + json.dumps(str(catalog_path)))
            if args.action == "init":
                ddls = (HERE / "schema-stable.cypher").read_text().split(";") + [ORDER_NODE]
                for ddl in ddls:
                    if ddl.strip():
                        execute(conn, ddl.replace(" TABLE ", " TABLE IF NOT EXISTS ", 1))
                print(f"映射已就绪。数据目录：{work}")
            else:
                interactive(conn)
    if args.action == "join":
        pins = lake_versions(catalog_path)
        print("快照", {name: p["snapshot"] for name, p in pins.items()})
        print("oid | uid | uname | pid | pname | amt")
        for row in reference_join(pins):
            print(" | ".join(map(str, row)))


if __name__ == "__main__":
    main()
