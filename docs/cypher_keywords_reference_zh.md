# Ladybug Cypher 合法关键字清单（按语句类型分组）

## 说明
- 统计口径：仅保留在语法规则中实际引用的单词型关键字，共 114 个。

## 查询（Query）

| 关键字 | 用法说明 | 用法示例 |
|---|---|---|
| `ALL` | UNION ALL 或量词/最短路修饰 | `MATCH (a) RETURN a.ID AS x UNION ALL MATCH (b) RETURN b.ID AS x;` |
| `ASC` | 升序排序 | `MATCH (p:Person) RETURN p.age ORDER BY p.age ASC;` |
| `ASCENDING` | 升序排序 | `MATCH (p:Person) RETURN p.age ORDER BY p.age ASC;` |
| `BY` | 与 ORDER/COPY 组合 | `COPY Person FROM ('id.csv','name.csv') BY COLUMN;` |
| `CALL` | 调用 setting 或表函数 | `CALL SHOW_TABLES() RETURN *;` |
| `DESC` | 降序排序 | `MATCH (p:Person) RETURN p.age ORDER BY p.age DESC;` |
| `DESCENDING` | 降序排序 | `MATCH (p:Person) RETURN p.age ORDER BY p.age DESC;` |
| `DISTINCT` | 去重 | `MATCH (p:Person) RETURN DISTINCT p.name;` |
| `LIMIT` | 结果数量限制 | `MATCH (p:Person) RETURN p LIMIT 10;` |
| `MATCH` | 模式匹配 | `MATCH (p:Person) RETURN p;` |
| `OPTIONAL` | 可选匹配 | `OPTIONAL MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p,c;` |
| `ORDER` | 排序子句 | `MATCH (p:Person) RETURN p.age ORDER BY p.age DESC;` |
| `RETURN` | 返回结果 | `MATCH (p:Person) RETURN p.name;` |
| `UNION` | 结果集合并 | `MATCH (a:Person) RETURN a.ID UNION MATCH (b:Org) RETURN b.ID;` |
| `UNWIND` | 展开列表 | `UNWIND [1,2,3] AS x RETURN x;` |
| `WHERE` | 过滤条件 | `MATCH (p:Person) WHERE p.age > 20 RETURN p;` |
| `WITH` | 管道传递与中间投影 | `MATCH (p:Person) WITH p.name AS name RETURN name;` |
| `YIELD` | CALL 结果列投影 | `CALL show_tables() YIELD name RETURN name;` |

## 更新（DML）

| 关键字 | 用法说明 | 用法示例 |
|---|---|---|
| `CREATE` | 创建对象或图数据 | `CREATE NODE TABLE Person(ID INT64, PRIMARY KEY(ID));` |
| `DELETE` | 删除节点/关系 | `MATCH (p:Person {ID:1}) DELETE p;` |
| `DETACH` | DETACH DELETE 或 DETACH DB | `MATCH (p:Person {ID:1}) DETACH DELETE p;` |
| `MERGE` | 匹配或创建 | `MERGE (p:Person {ID:1});` |
| `ON` | MERGE ON MATCH/ON CREATE 或 COMMENT ON | `MERGE (p:Person {ID:1}) ON MATCH SET p.name='A';` |
| `SET` | 更新属性 | `MATCH (p:Person {ID:1}) SET p.age = 30;` |

## 定义与结构管理（DDL）

| 关键字 | 用法说明 | 用法示例 |
|---|---|---|
| `ADD` | ALTER 增加列/连接 | `ALTER TABLE Person ADD age INT64 DEFAULT 0;` |
| `ALTER` | 表结构变更 | `ALTER TABLE Person RENAME name TO full_name;` |
| `COMMENT` | 表注释 | `COMMENT ON TABLE Person IS 'user table';` |
| `CYCLE` | 序列循环选项 | `CREATE SEQUENCE s START WITH 1 CYCLE;` |
| `DECIMAL` | DECIMAL 类型定义 | `CREATE TYPE money AS DECIMAL(18,2);` |
| `DEFAULT` | 默认值定义 | `ALTER TABLE Person ADD score INT64 DEFAULT 0;` |
| `DROP` | 删除表/序列/图等 | `DROP TABLE IF EXISTS Person;` |
| `EXISTS` | EXISTS 子查询 / IF EXISTS | `MATCH (p) WHERE EXISTS { MATCH (p)-[:KNOWS]->() } RETURN p;` |
| `FROM` | COPY/LOAD/ATTACH/连接定义等 | `COPY Person FROM 'person.csv';` |
| `GROUP` | CREATE REL TABLE GROUP | `CREATE REL TABLE GROUP Interacts(FROM User TO User);` |
| `IF` | IF EXISTS / IF NOT EXISTS | `CREATE NODE TABLE IF NOT EXISTS Person(ID INT64, PRIMARY KEY(ID));` |
| `INCREMENT` | 序列步长 | `CREATE SEQUENCE s INCREMENT BY 2;` |
| `IS` | 空值判断（IS NULL） | `MATCH (p:Person) WHERE p.name IS NULL RETURN p;` |
| `KEY` | PRIMARY KEY 定义 | `CREATE NODE TABLE Person(ID INT64, PRIMARY KEY(ID));` |
| `MACRO` | 宏定义 | `CREATE MACRO add(a,b) AS a + b;` |
| `MAP` | MAP 类型 | `CREATE TYPE kv AS MAP(STRING, INT64);` |
| `MAXVALUE` | 序列最大值 | `CREATE SEQUENCE s MAXVALUE 1000;` |
| `MINVALUE` | 序列最小值 | `CREATE SEQUENCE s MINVALUE 1;` |
| `NO` | 序列选项（NO MINVALUE/NO MAXVALUE/NO CYCLE） | `CREATE SEQUENCE s NO CYCLE;` |
| `NODE` | 节点表定义 | `CREATE NODE TABLE Person(ID INT64, PRIMARY KEY(ID));` |
| `PRIMARY` | 主键定义 | `CREATE NODE TABLE Person(ID INT64, PRIMARY KEY(ID));` |
| `REL` | 关系表定义 | `CREATE REL TABLE Knows(FROM Person TO Person, MANY_MANY);` |
| `RENAME` | 重命名表或列 | `ALTER TABLE Person RENAME TO People;` |
| `SEQUENCE` | 序列对象 | `CREATE SEQUENCE my_seq;` |
| `START` | 序列起始值 | `CREATE SEQUENCE s START WITH 1;` |
| `STRUCT` | STRUCT 类型 | `CREATE TYPE point AS STRUCT(x DOUBLE, y DOUBLE);` |
| `TABLE` | DDL 目标对象 | `DROP TABLE IF EXISTS Person;` |
| `TO` | 重命名/连接方向/COPY TO | `ALTER TABLE Person RENAME name TO full_name;` |
| `TYPE` | 自定义类型 | `CREATE TYPE point AS STRUCT(x DOUBLE, y DOUBLE);` |

## 数据导入导出与多库/图管理

| 关键字 | 用法说明 | 用法示例 |
|---|---|---|
| `ATTACH` | 挂载外部数据库 | `ATTACH '/data/demo' AS demo (DBTYPE SQLITE);` |
| `COLUMN` | COPY BY COLUMN 子句 | `COPY Person FROM ('id.csv','name.csv') BY COLUMN;` |
| `COPY` | 导入/导出数据 | `COPY Person FROM 'person.csv' (HEADER=true);` |
| `DATABASE` | 导入/导出数据库关键字 | `EXPORT DATABASE '/tmp/db_export';` |
| `DBTYPE` | ATTACH 时指定库类型 | `ATTACH '/data/demo' AS demo (DBTYPE SQLITE);` |
| `EXPORT` | 导出数据库 | `EXPORT DATABASE '/tmp/db_export';` |
| `GLOB` | 批量文件匹配 | `LOAD FROM GLOB('data/*.parquet') RETURN *;` |
| `GRAPH` | 图管理 | `CREATE GRAPH g1;` |
| `HEADERS` | LOAD WITH HEADERS | `LOAD WITH HEADERS (name STRING, age INT64) FROM 'x.csv' RETURN *;` |
| `IMPORT` | 导入数据库 | `IMPORT DATABASE '/tmp/db_export';` |
| `LOAD` | LOAD FROM / LOAD EXTENSION | `LOAD FROM 'x.csv' RETURN *;` |
| `PROJECT` | 投影图函数名中的保留词 | `CALL PROJECT_GRAPH_NATIVE('pg', ['Person'], ['Knows']);` |
| `USE` | 切换数据库/图 | `USE GRAPH g1;` |

## 事务与恢复

| 关键字 | 用法说明 | 用法示例 |
|---|---|---|
| `BEGIN` | 事务开始 | `BEGIN TRANSACTION;` |
| `CHECKPOINT` | 手动 checkpoint | `CHECKPOINT;` |
| `COMMIT` | 提交事务 | `COMMIT;` |
| `ONLY` | BEGIN TRANSACTION READ ONLY | `BEGIN TRANSACTION READ ONLY;` |
| `READ` | 只读事务 | `BEGIN TRANSACTION READ ONLY;` |
| `ROLLBACK` | 回滚事务 | `ROLLBACK;` |
| `TRANSACTION` | 事务关键字 | `BEGIN TRANSACTION;` |

## 优化与诊断

| 关键字 | 用法说明 | 用法示例 |
|---|---|---|
| `EXPLAIN` | 执行计划 | `EXPLAIN MATCH (p:Person) RETURN p;` |
| `HINT` | 匹配阶段 Join Hint | `MATCH (a:Person), (b:Person) HINT a JOIN b RETURN a,b;` |
| `JOIN` | HINT JOIN | `MATCH (a),(b) HINT a JOIN b RETURN a,b;` |
| `LOGICAL` | EXPLAIN LOGICAL | `EXPLAIN LOGICAL MATCH (p:Person) RETURN p;` |
| `MULTI_JOIN` | HINT MULTI_JOIN | `MATCH (a),(b),(c) HINT a MULTI_JOIN b MULTI_JOIN c RETURN a,b,c;` |
| `PROFILE` | 执行剖析 | `PROFILE MATCH (p:Person) RETURN p;` |

## 表达式与谓词

| 关键字 | 用法说明 | 用法示例 |
|---|---|---|
| `AND` | 逻辑与 | `MATCH (p:Person) WHERE p.age > 18 AND p.age < 65 RETURN p;` |
| `ANY` | 量词或 CREATE GRAPH ANY | `CREATE GRAPH g2 ANY;` |
| `CASE` | 条件表达式 | `RETURN CASE WHEN 1 < 2 THEN 'ok' ELSE 'bad' END;` |
| `CAST` | 类型转换 | `RETURN CAST('42', 'INT64');` |
| `CONTAINS` | 字符串包含 | `MATCH (p:Person) WHERE p.name CONTAINS 'Al' RETURN p;` |
| `COUNT` | 聚合或 COUNT 子查询 | `MATCH (p:Person) RETURN COUNT(*);` |
| `ELSE` | CASE 分支 | `RETURN CASE WHEN 1=2 THEN 'a' ELSE 'b' END;` |
| `END` | CASE 结束 | `RETURN CASE WHEN 1=2 THEN 'a' ELSE 'b' END;` |
| `ENDS` | 字符串后缀匹配（ENDS WITH） | `MATCH (p:Person) WHERE p.name ENDS WITH 'son' RETURN p;` |
| `FALSE` | 布尔字面量 | `RETURN TRUE, FALSE;` |
| `IN` | 列表包含测试 | `RETURN 3 IN [1,2,3];` |
| `NONE` | 量词表达式 | `RETURN NONE(x IN [1,2,3] WHERE x < 0);` |
| `NOT` | 逻辑非 / IF NOT EXISTS | `MATCH (p:Person) WHERE NOT p.age > 30 RETURN p;` |
| `NULL` | 空值字面量 | `RETURN NULL;` |
| `OR` | 逻辑或 | `MATCH (p:Person) WHERE p.age < 18 OR p.age > 65 RETURN p;` |
| `SINGLE` | 量词表达式 | `RETURN SINGLE(x IN [1,2,3] WHERE x = 2);` |
| `STARTS` | 字符串前缀匹配（STARTS WITH） | `MATCH (p:Person) WHERE p.name STARTS WITH 'Al' RETURN p;` |
| `THEN` | CASE 分支返回 | `RETURN CASE WHEN 1=1 THEN 'ok' ELSE 'bad' END;` |
| `TRUE` | 布尔字面量 | `RETURN TRUE, FALSE;` |
| `WHEN` | CASE 条件 | `RETURN CASE WHEN 1=1 THEN 'ok' ELSE 'bad' END;` |
| `XOR` | 逻辑异或 | `RETURN TRUE XOR FALSE;` |

## 路径语义

| 关键字 | 用法说明 | 用法示例 |
|---|---|---|
| `ACYCLIC` | 路径语义（无环） | `MATCH (a)-[e:knows* ACYCLIC 1..3]->(b) RETURN COUNT(*);` |
| `SHORTEST` | 最短路语义 | `MATCH (a)-[e:knows* SHORTEST 1..5]->(b) RETURN b.ID;` |
| `TRAIL` | 路径语义（边不重复） | `MATCH (a)-[e:knows* TRAIL 1..4]->(b) RETURN COUNT(*);` |
| `WSHORTEST` | 带权最短路语义 | `MATCH (a)-[e:road* WSHORTEST(distance) 1..10]->(b) RETURN b.ID;` |

## 扩展与权限

| 关键字 | 用法说明 | 用法示例 |
|---|---|---|
| `EXTENSION` | 扩展管理 | `LOAD EXTENSION 'my_ext';` |
| `FORCE` | 强制安装扩展 | `FORCE INSTALL my_ext FROM 'https://example/ext';` |
| `INSTALL` | 安装扩展 | `INSTALL my_ext FROM 'https://example/ext';` |
| `PASSWORD` | CREATE USER 时密码 | `CREATE USER IF NOT EXISTS alice WITH PASSWORD 'secret';` |
| `ROLE` | 角色创建 | `CREATE ROLE IF NOT EXISTS analyst;` |
| `UNINSTALL` | 卸载扩展 | `UNINSTALL my_ext;` |
| `UPDATE` | 更新扩展 | `UPDATE my_ext;` |
| `USER` | 用户创建 | `CREATE USER IF NOT EXISTS alice WITH PASSWORD 'secret';` |
| `WRITE` | 保留词（可见于语法保留集合） | `-- 该词在当前语法中主要作为保留词出现` |

## 其他/保留词

| 关键字 | 用法说明 | 用法示例 |
|---|---|---|
| `AS` | 别名 | `MATCH (p:Person) RETURN p.name AS name;` |

## 覆盖校验
- 本文覆盖语法关键字：114/114。
- `WRITE` 等词在当前版本主要作为保留词/上下文词出现。

