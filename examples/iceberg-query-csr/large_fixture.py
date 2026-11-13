# /// script
# requires-python = ">=3.11"
# dependencies = ["pyiceberg[sql-sqlite]==0.11.1", "pyarrow==23.0.1", "numpy>=1.26"]
# ///
"""Generate the original ecommerce distribution in bounded Arrow batches and pin its snapshots."""

import argparse
import json
from pathlib import Path
from time import perf_counter
from urllib.parse import unquote, urlparse

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import schema_to_pyarrow

from run import SCHEMAS, lake_versions


def generate(root, edges, file_rows=1_000_000):
    if edges < 1 or file_rows < 1:
        raise ValueError("Both edge count and rows per file must be positive")
    root.mkdir(parents=True, exist_ok=True)
    path = root / "catalog.sqlite"
    if path.exists():
        raise FileExistsError(f"Use a new dataset directory: {path}")
    catalog = SqlCatalog(
        "poc", uri=f"sqlite:///{path}", warehouse=(root / "warehouse").as_uri()
    )
    catalog.create_namespace("ecom")
    counts = {
        "users": max(1, edges // 10),
        "products": max(1, edges // 50),
        "orders": edges,
    }
    rng = np.random.default_rng(314159)
    summary = {}
    for name, count in counts.items():
        start = perf_counter()
        t = catalog.create_table(
            ("ecom", name), SCHEMAS[name], properties={"format-version": "2"}
        )
        schema = schema_to_pyarrow(t.schema(), include_field_ids=True)
        data = Path(unquote(urlparse(t.location()).path)) / "data"
        data.mkdir(parents=True, exist_ok=True)
        files = []
        order = rng.permutation(edges) if name == "orders" else None
        for first in range(0, count, file_rows):
            end = min(count, first + file_rows)
            if name == "orders":
                ids = order[first:end]
                columns = [
                    100003 + 13 * ids,
                    1001 + 7 * (ids % counts["users"]),
                    503 + 11 * rng.integers(counts["products"], size=end - first),
                    10 + ids % 5001,
                ]
            else:
                ids = np.arange(first, end, dtype=np.int64)
                prefix, base, step = (
                    ("user-", 1001, 7) if name == "users" else ("product-", 503, 11)
                )
                names = pc.binary_join_element_wise(
                    prefix, pc.cast(pa.array(ids), pa.string()), ""
                )
                columns = [base + step * ids, names]
            p = data / f"part-{first:09d}.parquet"
            pq.write_table(
                pa.Table.from_arrays(columns, schema=schema),
                p,
                compression="zstd",
                row_group_size=131072,
                use_dictionary=False,
            )
            files.append(p.as_uri())
            if len(files) % 10 == 0:
                print(f"{name} {end:,}/{count:,}", flush=True)
        t.add_files(files)
        summary[name] = dict(
            rows=count,
            files=len(files),
            bytes=sum(p.stat().st_size for p in data.glob("*.parquet")),
            seconds=round(perf_counter() - start, 3),
        )
        print(name, summary[name], flush=True)
    report = dict(
        seed=314159,
        uid=1001 + 7 * (counts["users"] // 2),
        counts=counts,
        tables=summary,
        file_rows=file_rows,
        row_group_rows=131072,
        pins=lake_versions(path),
    )
    (root / "fixture.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--edges", type=int, default=100_000_000)
    args = parser.parse_args()
    generate(args.root.resolve(), args.edges)
