# /// script
# requires-python = ">=3.11"
# dependencies = ["pyiceberg[sql-sqlite]==0.11.1", "pyarrow==23.0.1"]
# ///
"""Check path multiplicity, NULLs, batch boundaries and snapshot visibility on both CSR formats."""

import argparse
from collections import Counter
import json
from pathlib import Path

from sidecar import HERE, Connection, batch, create_fixture, execute, open_graph


def check(root, schema):
    path, users, _, orders = create_fixture(root / "lake")
    users.append(batch("users", (0, None), (-7, "Bo")))
    purchases = [(0, 0, 101, 2), (-5, -7, 101, 4900), (-6, -7, 101, -2)]
    purchases.extend(
        (18000 + i, 0, 101, None if i % 3 == 0 else i) for i in range(2100)
    )
    orders.append(batch("orders", *purchases))
    base = "MATCH (u:User)-[:Purchased]->(p:Product)<-[b:Purchased]-(v:User) WHERE u.uid=11 AND id(v)<>id(u)"
    filtered = base + " AND b.amt<4900"
    amount_count = filtered + " RETURN count(*)"
    detail = base + " RETURN b.oid,v.uid,v.uname,b.amt"
    names = (
        base.replace("id(v)<>id(u)", "v.uid<>u.uid") + " RETURN DISTINCT v.uid,v.uname"
    )
    one_hop = (
        "MATCH (u:User)-[r:Purchased]->(p:Product) "
        "WHERE u.uid=0 AND r.oid>=0 AND r.oid<20100 "
        "RETURN r.oid AS oid,u.uid,p.pid,r.amt,u.uname,p.pname ORDER BY oid DESC"
    )
    expected_orders = sorted(
        (
            [oid, uid, pid, amt, None, "Mouse"]
            for oid, uid, pid, amt in purchases
            if uid == 0
        ),
        key=lambda row: row[0],
        reverse=True,
    )
    negative_orders = one_hop.replace(
        "u.uid=0 AND r.oid>=0 AND r.oid<20100", "u.uid=-7 AND r.oid>-78 AND r.oid<=-5"
    )
    chain = "MATCH (u:User)-[a:Purchased]->(p:Product)<-[b:Purchased]-(v:User)-[c:Purchased]->(q:Product)"
    fixed = " WHERE u.uid=-7 AND a.oid=-5 AND v.uid=11 AND id(u)<>id(v) AND c.oid=7001"
    three = (
        chain
        + fixed
        + " AND c.amt<26 RETURN b.oid,c.oid,v.uname,q.pname ORDER BY b.oid"
    )
    fourth = chain + "<-[d:Purchased]-(w:User)" + fixed
    four = (
        fourth + " AND w.uid=0 AND d.amt<8 RETURN b.oid,d.oid,d.amt,w.uname "
        "ORDER BY b.oid,d.oid"
    )
    # >2048 distinct lookup keys, a NULL name, and duplicate paths across output vectors.
    bulk_four = four.replace("d.amt<8", "d.amt<4900 AND c.amt<26").replace(
        "ORDER BY b.oid,d.oid", ""
    )
    expected_bulk = Counter(
        (b, oid, amt, None)
        for b in (7001, 7002)
        for oid, uid, _, amt in purchases
        if uid == 0 and amt is not None
    )
    expected_four = [
        [b, oid, amt, None]
        for b in (7001, 7002)
        for oid, uid, _, amt in purchases
        if uid == 0 and amt is not None and amt < 8
    ]
    fourth_count = fourth + " AND w.uid=-7 AND d.amt<9 RETURN count(*)"
    middle_filter = fourth_count.replace("AND w.uid=-7", "AND b.amt<26 AND w.uid=-7")
    reversed_path = (
        "MATCH (p:Product)<-[a:Purchased]-(u:User)-[b:Purchased]->(q:Product)"
        "<-[c:Purchased]-(v:User) WHERE p.pid=205 AND c.amt<181 RETURN c.oid,v.uname"
    )
    five = reversed_path.replace(
        "(v:User) WHERE",
        "(v:User)-[d:Purchased]->(t:Product)<-[e:Purchased]-(w:User) WHERE",
    ).replace("c.amt<181 RETURN c.oid,v.uname", "e.amt<181 RETURN e.oid,w.uname")
    # The fixture already has two purchases by user 11 for product 101. Every peer purchase
    # is therefore reached by two different paths, even when both paths use the same order key.
    expected = Counter(
        {
            (oid, uid, None if uid == 0 else "Bo", amt): 2
            for oid, uid, _, amt in purchases
        }
    )
    hits = 2 * sum(amt is not None and amt < 4900 for _, _, _, amt in purchases)
    with open_graph(root / "graph.db") as (db, conn):
        show_threads = "CALL current_setting('iceberg_scan_threads') RETURN *"
        assert execute(conn, show_threads) == [["1"]]
        execute(conn, "CALL iceberg_scan_threads=4")
        with Connection(db) as other:
            assert execute(other, show_threads) == [["1"]]
        for invalid in ("0", "-1", "NULL"):
            try:
                execute(conn, f"CALL iceberg_scan_threads={invalid}")
            except RuntimeError as e:
                assert "positive integer" in str(e)
            else:
                raise AssertionError(f"Accepted invalid scan thread count {invalid}")
        assert execute(conn, show_threads) == [["4"]]
        execute(conn, "CALL iceberg_poc_catalog=" + json.dumps(str(path)))
        for ddl in (HERE / schema).read_text().split(";"):
            if ddl.strip():
                execute(conn, ddl)
        for query in (detail, amount_count, names, one_hop):
            assert "ICEBERG_CSR_KEY_LOOKUP" in execute(conn, "EXPLAIN " + query)[0][0]
        for query in (three, four, fourth_count, reversed_path, five):
            assert "ICEBERG_CSR_PATH_LOOKUP" in execute(conn, "EXPLAIN " + query)[0][0]
        assert (
            "ICEBERG_CSR_PATH_LOOKUP"
            not in execute(
                conn, "EXPLAIN " + reversed_path.replace("p.pid=205 AND ", "")
            )[0][0]
        )
        assert (
            "ICEBERG_CSR_KEY_LOOKUP"
            not in execute(conn, "EXPLAIN " + base + " RETURN DISTINCT v.uname")[0][0]
        )
        for enabled in (False, True):
            execute(conn, "CALL iceberg_poc_key_pushdown=" + str(enabled).lower())
            assert execute(conn, one_hop) == expected_orders
            assert execute(conn, three) == [
                [7001, 7001, "Ada", "Mouse"],
                [7002, 7001, "Ada", "Mouse"],
            ]
            assert execute(conn, four) == expected_four
            assert execute(conn, reversed_path) == [[9003, "Lin"]]
            assert Counter(map(tuple, execute(conn, detail))) == expected
            assert execute(conn, amount_count) == [[hits]]
            assert set(map(tuple, execute(conn, names))) == {(0, None), (-7, "Bo")}
        assert Counter(map(tuple, execute(conn, bulk_four))) == expected_bulk
        limited = list(map(tuple, execute(conn, bulk_four + " LIMIT 3")))
        assert len(limited) == 3 and Counter(limited) <= expected_bulk
        assert execute(conn, bulk_four.replace("d.amt<4900", "d.amt<-100")) == []
        # This order reaches 2101 * 2101 complete paths. Filter on an early edge
        # and count to exercise window rollover without materializing their payloads.
        many = chain + " WHERE u.uid=11 AND a.oid=7001 AND v.uid=0 AND b.amt<1"
        orders_of_zero = sum(uid == 0 for _, uid, _, _ in purchases)
        assert execute(conn, many + " RETURN count(*)") == [[0]]
        assert execute(conn, many.replace("b.amt<1", "b.amt<2") + " RETURN count(*)") == [
            [orders_of_zero]
        ]
        counted = many.replace("b.amt<1", "c.amt<4900")
        priced = sum(uid == 0 and amt is not None for _, uid, _, amt in purchases)
        assert execute(conn, counted + " RETURN count(*)") == [[orders_of_zero * priced]]
        assert execute(conn, amount_count.replace("u.uid=11", "u.uid=1234567")) == [[0]]
        assert execute(conn, amount_count.replace("b.amt<4900", "b.amt<-100")) == [[0]]
        execute(conn, "BEGIN TRANSACTION READ ONLY")
        assert execute(conn, amount_count) == [[hits]]
        assert execute(conn, fourth_count) == [[2]]
        assert execute(conn, middle_filter) == [[1]]
        assert [r[0] for r in execute(conn, negative_orders)] == [-5, -6]
        orders.append(batch("orders", (-77, -7, 101, 8)))
        assert execute(conn, amount_count) == [[hits]]
        assert execute(conn, fourth_count) == [[2]]
        assert [r[0] for r in execute(conn, negative_orders)] == [-5, -6]
        execute(conn, "COMMIT")
        assert execute(conn, amount_count) == [[hits + 2]]
        assert execute(conn, fourth_count) == [[4]]
        assert execute(conn, middle_filter) == [[2]]
        assert execute(conn, fourth_count.replace("u.uid=-7", "u.uid=1234567")) == [[0]]
        assert execute(conn, five) == [[9003, "Lin"]]
        assert [r[0] for r in execute(conn, negative_orders)] == [-5, -6, -77]
        assert execute(conn, one_hop.replace("u.uid=0", "u.uid=999999")) == []
    print(
        schema,
        len(purchases) * 2,
        "paths, NULLs, duplicate paths, two batches, old/new transactions passed",
    )


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("root", type=Path)
    args = p.parse_args()
    for schema in ("schema-sidecar.cypher", "schema-stable.cypher"):
        check(
            args.root.resolve() / schema.removesuffix(".cypher"),
            schema,
        )
