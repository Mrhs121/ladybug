# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psutil>=6", "pyarrow==23.0.1", "numpy>=1.26"]
# ///
"""Build a fresh benchmark sidecar in two bounded phases."""

import argparse
import json
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import unquote, urlparse

from large_benchmark import HERE, emit, profile


def quote(s):
    return "'" + str(s).replace("'", "''") + "'"


def iceberg(pins, name):
    t = pins[name]
    return (
        f"iceberg_scan({quote(t['metadata'])}, snapshot_from_id={int(t['snapshot'])})"
    )


def generation(db, pins):
    # Same versioned cache key as LakeRead::nativeGeneration. The query run verifies a cache hit.
    parts = ["sorted-key-v2-edge-key", "uid", "pid"]
    for name, key in [("users", "uid"), ("orders", "oid"), ("products", "pid")]:
        meta = json.loads(
            Path(unquote(urlparse(pins[name]["metadata"]).path)).read_text()
        )
        versions = [
            meta["table-uuid"],
            str(meta["current-snapshot-id"]),
            str(meta["current-schema-id"]),
        ]
        parts += ["ecom." + name, key, json.dumps(versions, separators=(",", ":"))]
    return (
        "IceEdges_"
        + db.execute(
            "SELECT sha256(to_json([" + ",".join("?" for _ in parts) + "]))", parts
        ).fetchone()[0]
    )


def prepare(root, shard_slots):
    import duckdb
    import numpy as np
    import pyarrow.parquet as pq

    work = root / "topology"
    work.mkdir(exist_ok=True)
    fixture = json.loads((root / "lake/fixture.json").read_text())
    pins = fixture["pins"]
    with duckdb.connect() as db:
        db.execute("SET threads=2")
        db.execute("SET memory_limit='1800MiB'")
        db.execute("SET preserve_insertion_order=false")
        db.execute("SET temp_directory=" + quote(work / "spill"))
        db.execute("LOAD iceberg")
        started = time.monotonic()
        emit(event="start_map")
        for table, key, extra in [
            ("users", "uid", ""),
            ("products", "pid", ""),
            ("orders", "oid", ", uid, pid"),
        ]:
            # A single externally sorted file gives the exact global rank via file_row_number.
            path = work / (table + "-keys.parquet")
            db.execute(
                f"COPY (SELECT {key}{extra} FROM {iceberg(pins, table)} ORDER BY {key}) "
                f"TO {quote(path)} (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            count, previous = 0, None
            for batch in pq.ParquetFile(path).iter_batches(
                columns=[key], use_threads=False
            ):
                column = batch.column(0)
                if column.null_count:
                    raise ValueError(f"Null key in {table}")
                values = column.to_numpy()
                if len(values) and (
                    (previous is not None and values[0] <= previous)
                    or np.any(values[1:] <= values[:-1])
                ):
                    raise ValueError(f"Non-unique or unsorted key in {table}")
                if len(values):
                    previous = values[-1]
                count += len(values)
            if count != fixture["counts"][table]:
                raise ValueError((table, count))
            db.execute(
                f"CREATE VIEW {table} AS SELECT *, file_row_number AS pos FROM "
                f"read_parquet({quote(path)},file_row_number=true)"
            )
            emit(event="ranked", table=table, rows=count)
        n = max(fixture["counts"]["users"], fixture["counts"]["products"])
        db.execute(
            f"COPY (SELECT range AS pos FROM range({n}) ORDER BY pos) TO {quote(work / 'slots.parquet')} (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        db.execute(f"""COPY (
            SELECT u.pos AS src, p.pos AS dst, o.pos AS lake_row, o.oid AS lake_key,
                   u.pos // {shard_slots} AS shard
            FROM orders o JOIN users u ON u.uid=o.uid JOIN products p ON p.pid=o.pid)
            TO {quote(work / "edges")} (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (shard))""")
        count = db.execute(
            f"SELECT count(*) FROM read_parquet({quote(work / 'edges/shard=*/*.parquet')})"
        ).fetchone()[0]
        if count != fixture["counts"]["orders"]:
            raise ValueError(f"Only {count} edges have both endpoints")
        result = dict(
            generation=generation(db, pins),
            slots=n,
            edges=count,
            shard_slots=shard_slots,
            map_s=time.monotonic() - started,
            pins=pins,
        )
        (work / "manifest.json").write_text(json.dumps(result, indent=2) + "\n")
        emit(event="end_map", **{k: v for k, v in result.items() if k != "pins"})


def native(root, buffer_gib):
    sys.path.insert(0, str(HERE.parents[1]))
    os.environ["LBUG_PYTHON_BACKEND"] = "capi"
    from tools.python_api.src_py import Connection, Database

    work = root / "topology"
    manifest = json.loads((work / "manifest.json").read_text())
    if manifest["pins"] != json.loads((root / "lake/fixture.json").read_text())["pins"]:
        raise ValueError("Staging and lake snapshots differ")
    target = root / "mapped/graph.db.csr.db"
    target.parent.mkdir(exist_ok=True)
    if target.exists():
        raise FileExistsError(target)
    tmp = target.with_suffix(".building")
    if tmp.exists():
        raise FileExistsError(f"An unpublished build exists: {tmp}")
    name = manifest["generation"]

    def run(conn, q):
        with conn.execute(q) as r:
            return list(r)

    @contextmanager
    def connection(read_only=False):
        with (
            Database(
                str(tmp),
                buffer_pool_size=buffer_gib << 30,
                read_only=read_only,
                max_num_threads=1,
            ) as db,
            Connection(db) as c,
        ):
            yield c

    start = time.monotonic()
    emit(event="start_native", buffer_gib=buffer_gib)
    with connection() as c:
        run(c, "CREATE NODE TABLE CsrSlots(pos INT64 PRIMARY KEY)")
        run(c, f"COPY CsrSlots FROM {quote(work / 'slots.parquet')}")
        run(
            c,
            f"CREATE REL TABLE {name}(FROM CsrSlots TO CsrSlots, lake_row INT64, lake_key INT64)",
        )
    emit(event="slots_written", ms=(time.monotonic() - start) * 1000)
    parts = sorted(
        (work / "edges").glob("shard=*"), key=lambda p: int(p.name.split("=")[1])
    )
    for part in parts:
        begin = time.monotonic()
        paths = ", ".join(quote(p) for p in sorted(part.glob("*.parquet")))
        with connection() as c:
            rows = run(c, f"COPY {name} FROM [{paths}]")
            run(c, "CHECKPOINT")
        emit(
            event="shard_written",
            part=part.name,
            result=rows,
            ms=(time.monotonic() - begin) * 1000,
        )
    with connection(read_only=True) as c:
        if run(c, f"MATCH ()-[r:{name}]->() RETURN count(*)") != [[manifest["edges"]]]:
            raise ValueError("Closed sidecar edge count differs from staged input")
    if Path(str(tmp) + ".wal").exists() and Path(str(tmp) + ".wal").stat().st_size:
        raise RuntimeError("Native WAL still contains uncheckpointed data")
    # Publish only a closed, checkpointed file into this new benchmark database.
    os.link(tmp, target)
    tmp.unlink()
    emit(
        event="published",
        native_s=time.monotonic() - start,
        sidecar_bytes=target.stat().st_size,
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("root", type=Path)
    p.add_argument("phase", choices=["map", "native"])
    p.add_argument("--buffer-gib", type=int, default=6)
    p.add_argument("--shard-slots", type=int, default=1_048_576)
    p.add_argument("--profile", type=Path)
    p.add_argument("--limit-gib", type=float, default=0)
    args = p.parse_args()
    root = args.root.resolve()
    if args.profile:
        command = [
            sys.executable,
            __file__,
            str(root),
            args.phase,
            "--buffer-gib",
            str(args.buffer_gib),
            "--shard-slots",
            str(args.shard_slots),
        ]
        sys.exit(profile(command, args.profile, args.limit_gib))
    if args.phase == "map":
        prepare(root, args.shard_slots)
    else:
        native(root, args.buffer_gib)
