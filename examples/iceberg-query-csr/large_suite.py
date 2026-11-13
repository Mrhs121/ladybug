# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psutil>=6"]
# ///
"""Verify snapshot joins and measure unmodified mapped MATCH queries in isolated processes."""

import argparse
import json
import os
import platform
import random
import sqlite3
import statistics
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

from large_benchmark import HERE, profile, queries
from large_build import iceberg, quote


def read(path):
    return json.loads(path.read_text())


def write(path, data):
    path.write_text(json.dumps(data, indent=2) + "\n")


def log_rows(path):
    return [json.loads(s) for s in path.read_text().splitlines() if s.strip()]


def check_pins(root, pins):
    catalog = (root / "lake/catalog.sqlite").as_uri() + "?mode=ro"
    with sqlite3.connect(catalog, uri=True) as con:
        current = dict(
            con.execute(
                "SELECT table_name, metadata_location FROM iceberg_tables WHERE catalog_name='poc' AND table_namespace='ecom'"
            )
        )
    assert current == {k: v["metadata"] for k, v in pins.items()}
    for row in pins.values():
        meta = read(Path(unquote(urlparse(row["metadata"]).path)))
        assert row["snapshot"] == meta["current-snapshot-id"]


def oracle(root, fixture, anchors=None):
    import duckdb

    pins = fixture["pins"]
    keys = ",".join(str(int(uid)) for uid in (anchors or [fixture["uid"]]))
    with duckdb.connect() as db:
        db.execute("SET threads=1")
        db.execute("SET memory_limit='1500MiB'")
        db.execute("SET temp_directory=" + quote(root / "oracle-spill"))
        db.execute("LOAD iceberg")
        for name in pins:
            db.execute(f"CREATE VIEW {name} AS SELECT * FROM {iceberg(pins, name)}")
            actual = db.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
            assert actual == fixture["counts"][name], (name, actual)
        # Each path stays a separate row, including several orders between the same endpoints.
        db.execute(f"""CREATE TEMP TABLE h1 AS SELECT u.uid AS origin, p.pid AS first_pid, a.oid, a.amt
            FROM users u JOIN orders a ON a.uid=u.uid JOIN products p ON p.pid=a.pid
            WHERE u.uid IN ({keys})""")
        db.execute("""CREATE TEMP TABLE h2 AS SELECT h.origin,h.first_pid,v.uid AS peer,b.amt AS peer_amt
            FROM h1 h JOIN orders b ON b.pid=h.first_pid JOIN users v ON v.uid=b.uid
            WHERE v.uid<>h.origin""")
        db.execute("""CREATE TEMP TABLE h3 AS SELECT h.origin,h.peer,q.pid AS next_pid FROM h2 h
            JOIN orders c ON c.uid=h.peer JOIN products q ON q.pid=c.pid
            WHERE q.pid<>h.first_pid""")
        sql = {
            "one_count": "SELECT count(*) FROM h1",
            "one_topk": "SELECT oid, amt FROM h1 WHERE amt<4900 ORDER BY oid LIMIT 3",
            "two_count": "SELECT count(*) FROM h2",
            "two_buyers": "SELECT peer,count(*) AS n FROM h2 GROUP BY peer ORDER BY n DESC,peer LIMIT 10",
            "three_count": "SELECT count(*) FROM h3",
            "four_count": """SELECT count(*) FROM h3 h JOIN orders d ON d.pid=h.next_pid
                      JOIN users w ON w.uid=d.uid WHERE w.uid<>h.peer""",
        }
        if anchors:
            # Group complete paths by their starting user for an independent per-query oracle.
            four = sql["four_count"].replace(
                "SELECT count(*)", "SELECT h.origin,count(*)"
            )
            return {
                "two_amount": dict(
                    db.execute(
                        "SELECT origin,count(*) FROM h2 WHERE peer_amt<4900 GROUP BY origin"
                    ).fetchall()
                ),
                "two_count": dict(
                    db.execute(
                        "SELECT origin,count(*) FROM h2 GROUP BY origin"
                    ).fetchall()
                ),
                "four_count": dict(db.execute(four + " GROUP BY h.origin").fetchall()),
            }
        return {
            k: [list(row) for row in db.execute(query).fetchall()]
            for k, query in sql.items()
        }


def verify_sidecar(root, manifest):
    sys.path.insert(0, str(HERE.parents[1]))
    os.environ["LBUG_PYTHON_BACKEND"] = "capi"
    from tools.python_api.src_py import Connection, Database

    path = root / "mapped/graph.db.csr.db"

    def run(c, q):
        with c.execute(q) as result:
            return list(result)

    name = manifest["generation"]
    with (
        Database(
            str(path), read_only=True, buffer_pool_size=256 << 20, max_num_threads=1
        ) as db,
        Connection(db) as conn,
    ):
        tables = [
            r[0] for r in run(conn, "CALL show_tables() RETURN name ORDER BY name")
        ]
        assert set(tables) == {name, "CsrSlots"}
        assert run(conn, "MATCH (s:CsrSlots) RETURN count(*)") == [[manifest["slots"]]]
        assert run(
            conn, "MATCH (s:CsrSlots) WHERE offset(id(s))<>s.pos RETURN count(*)"
        ) == [[0]]
        assert run(conn, f"MATCH ()-[e:{name}]->() RETURN count(*)") == [
            [manifest["edges"]]
        ]
        fields = [r[0] for r in run(conn, f"CALL table_info('{name}') RETURN name")]
        assert fields == ["lake_row", "lake_key"]
        descriptors = run(
            conn, f"CALL storage_info('{name}') RETURN column_name,num_pages"
        )
        columns = {col for col, _ in descriptors}
        stored = sorted({col for col, pages in descriptors if pages and pages > 0})
        for prefix in ("fwd_", "bwd_"):
            assert prefix + "NBR_ID" in stored
            for suffix in (
                "csr_offset",
                "csr_length",
                "NBR_ID",
                "lake_row",
                "lake_key",
            ):
                assert prefix + suffix in columns
    return dict(
        file_bytes=path.stat().st_size,
        allocated_bytes=path.stat().st_blocks * 512,
        fields=fields,
        columns_with_pages=stored,
        tables=tables,
        lbug_version=Database.get_version(),
        checked_dense_slots=manifest["slots"],
        checked_native_edges=manifest["edges"],
    )


def measure(root, repeats, limit_gib, main_buffer_mib):
    out = root / "queries"
    out.mkdir(exist_ok=True)
    f = read(root / "lake/fixture.json")
    manifest = read(root / "topology/manifest.json")
    check_pins(root, f["pins"])
    assert f["pins"] == manifest["pins"]
    sidecar = verify_sidecar(root, manifest)
    expected = oracle(root, f)
    write(out / "joins.json", expected)
    print("Verified native sidecar and snapshot JOINs", flush=True)
    metrics = {}
    for case, q in queries(f["uid"]).items():
        metrics[case] = {}
        for mode in ("auto", "txn"):
            prefix = out / (case + "-" + mode)
            cmd = [
                sys.executable,
                str(HERE / "large_benchmark.py"),
                str(root),
                "--graph",
                str(root / "mapped/graph.db"),
                "--mode",
                mode,
                "--case",
                case,
                "--repeats",
                str(repeats + 1),
            ]
            cmd.extend(["--main-buffer-mib", str(main_buffer_mib)])
            if profile(cmd, prefix, limit_gib):
                raise RuntimeError(
                    f"{case} {mode} failed; see {prefix}.log and {prefix}.memory.json"
                )
            rows = log_rows(prefix.with_suffix(".jsonl"))
            baseline = next(r for r in rows if r["event"] == "ready")
            results = [r for r in rows if r["event"] == "result"]
            if len(results) != repeats + 1 or any(
                r["rows"] != expected[case] for r in results
            ):
                raise ValueError(
                    f"{case}/{mode} differs from the snapshot JOIN: {results}"
                )
            native_log = prefix.with_suffix(".log").read_text()
            assert (
                f"reused {manifest['generation']}" in native_log
                and "[iceberg_csr] built " not in native_log
            )
            sampled = read(prefix.with_suffix(".memory.json"))
            active = [
                v
                for v in sampled["samples"]
                if baseline["t"] <= v[0] <= results[-1]["t"]
            ]
            resident_peak = max(
                [
                    baseline["baseline"][0],
                    *(v[1] for v in active),
                    *(r["memory"][0] for r in results),
                ]
            )
            footprint_peak = max(
                [
                    baseline["baseline"][1] or 0,
                    *(v[2] or 0 for v in active),
                    *(r["memory"][1] or 0 for r in results),
                ]
            )
            warm = [r["ms"] for r in results[1:]]
            m = dict(
                cypher=q,
                result=expected[case],
                first_ms=results[0]["ms"],
                samples_ms=warm,
                median_ms=statistics.median(warm),
                min_ms=min(warm),
                max_ms=max(warm),
                baseline_rss=baseline["baseline"][0],
                query_peak_rss=resident_peak,
                query_peak_footprint=footprint_peak,
                kernel_process_peak_rss=rows[-1]["peak_rss_bytes"],
            )
            metrics[case][mode] = m
            write(out / "measurements.json", metrics)
            print(
                case,
                mode,
                round(m["median_ms"], 2),
                "ms",
                round(resident_peak / 2**30, 3),
                "GiB RSS",
                flush=True,
            )
        cmd = [
            sys.executable,
            str(HERE / "large_benchmark.py"),
            str(root),
            "--graph",
            str(root / "mapped/graph.db"),
            "--mode",
            "plan",
            "--case",
            case,
        ]
        cmd.extend(["--main-buffer-mib", str(main_buffer_mib)])
        with (out / (case + ".plan.txt")).open("w") as plan:
            subprocess.run(cmd, stdout=plan, stderr=subprocess.PIPE, check=True)
    check_pins(root, f["pins"])
    assert (root / "mapped/graph.db.csr.db").stat().st_size == sidecar["file_bytes"]
    report = dict(
        fixture=f,
        sidecar=sidecar,
        main_file_bytes=(root / "mapped/graph.db").stat().st_size,
        queries=metrics,
        python=sys.version.split()[0],
        platform=platform.platform(),
        repeats=repeats,
        main_buffer_bytes=main_buffer_mib << 20,
        reader_sidecar_buffer_bytes=256 << 20,
        query_threads=1,
        sampling_ms=20,
    )
    for k in ("map", "native"):
        prefix = root / "build-metrics" / k
        if prefix.with_suffix(".jsonl").exists():
            mem = read(prefix.with_suffix(".memory.json"))
            report[k] = dict(
                events=log_rows(prefix.with_suffix(".jsonl")),
                peak_rss=mem["peak_rss"],
                peak_footprint=mem["peak_footprint"],
                elapsed_s=mem["elapsed_s"],
            )
    write(root / "results.json", report)
    return report


def rotation_samples(prefix, anchors, expected):
    results = [
        r for r in log_rows(prefix.with_suffix(".jsonl")) if r["event"] == "result"
    ]
    mem = read(prefix.with_suffix(".memory.json"))
    assert [r["anchor"] for r in results] == anchors[: len(results)]
    assert all(r["rows"] == [[expected.get(r["anchor"], 0)]] for r in results)
    if mem["returncode"] == 0:
        assert len(results) == len(anchors)
    times = [r["ms"] for r in results[1:]]
    return dict(
        completed=len(results),
        requested=len(anchors),
        returncode=mem["returncode"],
        first_incomplete_anchor=anchors[len(results)]
        if len(results) < len(anchors)
        else None,
        median_ms=statistics.median(times) if times else None,
        min_ms=min(times, default=None),
        max_ms=max(times, default=None),
        peak_rss=mem["peak_rss"],
        peak_footprint=mem["peak_footprint"],
        results=results,
    )


def rotation(root, main_buffer_mib, limit):
    fixture = read(root / "lake/fixture.json")
    check_pins(root, fixture["pins"])
    n = fixture["counts"]["users"]
    sample = random.Random(314159).sample(range(n), min(13, n))
    anchors = [fixture["uid"]] + [
        1001 + 7 * x for x in sample if 1001 + 7 * x != fixture["uid"]
    ][:12]
    if len(anchors) < 2:
        raise ValueError("At least two users are needed for rotating anchors")
    dest = root / "rotation"
    dest.mkdir(exist_ok=True)
    write(dest / "anchors.json", anchors)
    expected = oracle(root, fixture, anchors)
    report = {}
    for case in ("two_count", "four_count"):
        prefix = dest / case
        cmd = [
            sys.executable,
            str(HERE / "large_benchmark.py"),
            str(root),
            "--graph",
            str(root / "mapped/graph.db"),

            "--case",
            case,
            "--mode",
            "txn",
            "--anchors",
            str(dest / "anchors.json"),
            "--main-buffer-mib",
            str(main_buffer_mib),
        ]
        profile(cmd, prefix, limit)
        report[case] = rotation_samples(prefix, anchors, expected[case])
    check_pins(root, fixture["pins"])
    write(dest / "results.json", report)
    return all(case["returncode"] == 0 for case in report.values())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument(
        "--rotation",
        action="store_true",
        help="measure different users in a warm transaction",
    )
    parser.add_argument("--limit-gib", type=float, default=7)
    parser.add_argument("--main-buffer-mib", type=int, default=2048)
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("At least one timed repetition is required")
    if args.rotation:
        complete = rotation(
            args.root.resolve(),
            args.main_buffer_mib,
            args.limit_gib,
        )
        sys.exit(0 if complete else 1)
    measure(
        args.root.resolve(),
        args.repeats,
        args.limit_gib,
        args.main_buffer_mib,
    )
