MATCH (u:User)-[a:Purchased]->(p:Product)<-[b:Purchased]-(v:User)
      -[c:Purchased]->(q:Product)
WHERE u.uid=3501001 AND a.oid=130100003
  AND id(v)<>id(u) AND q.pid<>p.pid AND c.amt<500
RETURN a.oid AS first_oid, b.oid AS second_oid, c.oid AS third_oid,
       v.uid AS uid, q.pid AS pid, c.amt AS amt, q.pname AS pname
ORDER BY second_oid, third_oid;

MATCH (u:User)-[a:Purchased]->(p:Product)<-[b:Purchased]-(v:User)
      -[c:Purchased]->(q:Product)<-[d:Purchased]-(w:User)
WHERE u.uid=3501001 AND a.oid=130100003
  AND id(v)<>id(u) AND q.pid<>p.pid AND w.uid<>v.uid AND d.amt<50
RETURN b.oid AS second_oid, c.oid AS third_oid, d.oid AS last_oid,
       w.uid AS uid, d.amt AS amt, w.uname AS uname, q.pname AS pname
ORDER BY second_oid, third_oid, last_oid;
