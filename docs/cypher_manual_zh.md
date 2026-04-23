# Ladybug Cypher 使用与语法手册

> 版本范围：基于仓库当前实现与测试（检查时间：2026-04-04）。  
> 说明：本手册是"项目实际支持能力"总结，不等同于完整 openCypher 标准。

---

## 目录

1. [如何在项目中使用 Cypher](#1-如何在项目中使用-cypher)
2. [语句级支持总览](#2-语句级支持总览)
3. [查询语法](#3-查询语法)
4. [递归、最短路与路径语义](#4-递归最短路与路径语义)
5. [更新语法（DML）](#5-更新语法dml)
6. [DDL 与管理语法](#6-ddl-与管理语法)
7. [表达式与运算符](#7-表达式与运算符)
8. [数据类型](#8-数据类型)
9. [内置函数参考](#9-内置函数参考)
10. [CALL 能力](#10-call-能力)
11. [EXPLAIN / PROFILE](#11-explain--profile)
12. [已知限制与不支持项](#12-已知限制与不支持项)
13. [完整示例](#13-完整示例)
14. [语法与能力验证来源](#14-语法与能力验证来源)

---

## 1. 如何在项目中使用 Cypher

### 1.1 Shell 交互执行

```bash
# 构建
make shell

# 启动（默认打开或创建当前目录数据库）
./build/release/tools/shell/lbug_shell

# 指定数据库路径
./build/release/tools/shell/lbug_shell /path/to/db
```

**Shell CLI 参数：**

| 参数 | 说明 |
|------|------|
| `-h, --help` | 显示帮助 |
| `-v, --version` | 显示数据库版本 |
| `-d, --default_bp_size` | 缓冲池大小（MB） |
| `--max_db_size` | 最大数据库大小（字节） |
| `--no_compression` | 禁用压缩 |
| `-r, --read_only` | 只读模式打开 |
| `-p, --path_history` | Shell 历史文件目录 |
| `-m, --mode` | 输出模式（如 JSON） |
| `-s, --no_stats` | 禁用查询统计 |
| `-b, --no_progress_bar` | 禁用进度条 |
| `-w, --ignore_wal_replay_errors` | 忽略 WAL 回放错误 |
| `-i, --init` | 启动时执行的脚本文件路径 |

进入后可直接执行 Cypher：

```cypher
CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));
CREATE (:Person {name: 'Alice', age: 25});
MATCH (p:Person) RETURN p.name, p.age;
```

### 1.2 C++ API

```cpp
#include "lbug.hpp"
using namespace lbug::main;

auto database = std::make_unique<Database>("/path/to/db");
auto connection = std::make_unique<Connection>(database.get());

// DDL
connection->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));");

// DML
connection->query("CREATE (:Person {name: 'Alice', age: 25});");

// 查询
auto result = connection->query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;");
std::cout << result->toString();
```

### 1.3 C API

```c
#include "lbug.h"

lbug_database db;
lbug_connection conn;
lbug_query_result result;

lbug_database_init("/path/to/db", lbug_default_system_config(), &db);
lbug_connection_init(&db, &conn);

lbug_connection_query(&conn, "MATCH (a:Person) RETURN a.name, a.age;", &result);

lbug_flat_tuple tuple;
while (lbug_query_result_has_next(&result)) {
    lbug_query_result_get_next(&result, &tuple);
    // 处理 tuple ...
}

lbug_query_result_destroy(&result);
lbug_connection_destroy(&conn);
lbug_database_destroy(&db);
```

### 1.4 Python API

```python
import real_ladybug as lb

db = lb.Database("./test_db")
conn = lb.Connection(db)

# 建表
conn.execute("CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY(name))")
conn.execute("CREATE NODE TABLE City(name STRING, population INT64, PRIMARY KEY(name))")
conn.execute("CREATE REL TABLE LivesIn(FROM User TO City)")

# 导入数据
conn.execute('COPY User FROM "user.csv"')
conn.execute('COPY City FROM "city.csv"')
conn.execute('COPY LivesIn FROM "lives_in.csv"')

# 查询
results = conn.execute("MATCH (u:User) RETURN u.name, u.age;")
while results.has_next():
    print(results.get_next())
```

**Python API 主要类：**

- `Database` — 数据库管理
- `Connection` — 查询执行
- `QueryResult` — 结果集遍历
- `PreparedStatement` — 预编译语句
- `Type` — 数据类型枚举

---

## 2. 语句级支持总览

| 类别 | 语法/能力 | 支持情况 | 备注 |
|------|-----------|----------|------|
| 查询 | `MATCH`, `OPTIONAL MATCH`, `UNWIND`, `CALL ... YIELD`, `LOAD FROM`, `WITH`, `RETURN`, `UNION/UNION ALL` | 支持 | 存在若干约束（见第 12 节） |
| 更新 | `CREATE`, `MERGE`, `SET`, `DELETE`, `DETACH DELETE` | 支持 | 关系与主键更新有限制 |
| 表结构 | `CREATE NODE TABLE`, `CREATE REL TABLE`, `CREATE REL TABLE GROUP`, `ALTER TABLE`, `DROP TABLE` | 支持 | 主键必填、关系连接约束 |
| 复制导入导出 | `COPY FROM`, `COPY FROM ... BY COLUMN`, `COPY TO`, `EXPORT DATABASE`, `IMPORT DATABASE` | 支持 | |
| 事务 | `BEGIN TRANSACTION [READ ONLY]`, `COMMIT`, `ROLLBACK`, `CHECKPOINT` | 支持 | 并发与 checkpoint 有冲突保护 |
| 扩展 | `LOAD/INSTALL/UNINSTALL/UPDATE` extension | 支持 | `CREATE USER/ROLE` 走扩展解析 |
| 多库 | `ATTACH`, `DETACH`, `USE <db>` | 支持 | 可配合限定名 `db.table` |
| 图管理 | `CREATE GRAPH [ANY]`, `USE GRAPH`, `DROP GRAPH` | 支持 | `ANY` 图支持动态 label |
| 其他 | `CREATE MACRO`, `CREATE SEQUENCE`, `CREATE TYPE`, `COMMENT ON TABLE`, `EXPLAIN/PROFILE` | 支持 | |

---

## 3. 查询语法

### 3.1 查询结构

```
RegularQuery ::=
    SingleQuery ( UNION [ALL] SingleQuery )*

SingleQuery ::=
    SinglePartQuery | MultiPartQuery

SinglePartQuery ::=
    ReadingClause* RETURN
  | ReadingClause* UpdatingClause+ RETURN?

MultiPartQuery ::=
    (ReadingClause* UpdatingClause* WITH)+ SinglePartQuery
```

### 3.2 MATCH / OPTIONAL MATCH

```cypher
-- 基本匹配
MATCH (a:Person)-[e:KNOWS]->(b:Person) WHERE a.ID = 0 RETURN b.ID;

-- 可选匹配
OPTIONAL MATCH (a:Person)-[:WORKS_AT]->(o:Org) RETURN a.ID, o.ID;

-- 多模式
MATCH (a:Person), (b:City) WHERE a.city = b.name RETURN a, b;
```

**模式语法支持：**

| 特性 | 语法 | 说明 |
|------|------|------|
| 节点标签 | `(n:Label)` | 单标签 |
| 多标签 | `(n:A:B)` | 多标签（AND 语义） |
| 标签或 | `(n:A\|B)` | 多标签（OR 语义） |
| 关系类型 | `-[e:TYPE]->` | 单类型 |
| 多关系类型 | `-[e:A\|B]->` | OR 语义 |
| 关系方向 | `->`, `<-`, `--` | 右向、左向、无向 |
| 属性谓词 | `(n {id: 1, name: 'Alice'})` | 内联属性过滤 |
| 路径变量 | `p = (a)-[e]->(b)` | 绑定路径到变量 |
| 可变长关系 | `-[e*1..3]->` | 见第 4 节 |

**HINT 语法（查询优化提示）：**

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)-[:LIVES_IN]->(c:City)
WHERE a.name = 'Alice'
RETURN c.name
HINT a JOIN b;
```

### 3.3 WITH / RETURN

```cypher
MATCH (a:Person)
WITH a.ID AS id, a.age AS age ORDER BY age DESC LIMIT 10
RETURN id, age;

RETURN DISTINCT a.name, COUNT(*) AS cnt;
```

**投影子句要素：**

| 要素 | 语法 | 说明 |
|------|------|------|
| 别名 | `expr AS alias` | WITH 中必须显式别名 |
| 去重 | `DISTINCT` | 去除重复行 |
| 通配 | `*` | 返回所有作用域内变量 |
| 排序 | `ORDER BY expr [ASC\|DESC]` | |
| 跳过 | `SKIP n` | 仅允许参数或字面量 |
| 限制 | `LIMIT n` | 仅允许参数或字面量 |

关键规则：

- `WITH` 中表达式**必须**显式 `AS` 别名。
- `WITH` 中如果用了 `ORDER BY`，**必须**跟 `SKIP` 或 `LIMIT`。
- `RETURN` 只能出现在查询末尾。
- `RETURN/WITH *` 在作用域为空时不允许。

### 3.4 UNION

```cypher
MATCH (a:Person) RETURN a.ID AS x
UNION ALL
MATCH (o:Org) RETURN o.ID AS x;
```

规则：
- `UNION` / `UNION ALL` 不能在同一个 regular query 中混用。
- 各分支列数与对应列类型必须一致。

### 3.5 UNWIND

```cypher
UNWIND [1, 2, 3] AS x RETURN x;

UNWIND range(1, 10) AS i
MATCH (a:Person {ID: i}) RETURN a.name;
```

### 3.6 CALL（查询内）

```cypher
CALL show_tables() YIELD name
RETURN name;

CALL table_info('Person') WHERE name = 'age'
YIELD type RETURN type;
```

- 查询内 `CALL ...` 绑定为表函数调用（table function）。
- 可选 `WHERE` 与 `YIELD`。

### 3.7 LOAD FROM

```cypher
-- 从文件加载
LOAD FROM "data.csv" (header=true) RETURN *;

-- 带表头声明
LOAD WITH HEADERS (name STRING, age INT64) FROM "data.csv" RETURN *;

-- 从文件列表
LOAD FROM ["a.csv", "b.csv"] RETURN *;

-- 从 GLOB
LOAD FROM GLOB("data/*.parquet") RETURN *;

-- 从参数
LOAD FROM $param RETURN *;

-- 带过滤
LOAD FROM "data.csv" WHERE col1 > 10 RETURN *;
```

限制：
- `LOAD FROM (<subquery>)` 不支持。
- `LOAD FROM func(...)` 不支持。

---

## 4. 递归、最短路与路径语义

### 4.1 可变长关系

```cypher
-- 基本可变长
MATCH (a)-[e:knows*1..3]->(b) RETURN a.ID, b.ID, length(e);

-- 仅指定上界
MATCH (a)-[e*..5]->(b) RETURN COUNT(*);

-- 仅指定下界
MATCH (a)-[e*2..]->(b) RETURN COUNT(*);

-- 固定长度
MATCH (a)-[e*3]->(b) RETURN COUNT(*);
```

### 4.2 路径语义类型

| 语义 | 语法 | 说明 |
|------|------|------|
| 默认 | `[e*1..3]` | 无语义约束 |
| TRAIL | `[e* TRAIL 1..5]` | 不重复边 |
| ACYCLIC | `[e* ACYCLIC 1..5]` | 不重复节点 |
| SHORTEST | `[e* SHORTEST 1..5]` | 最短路（单条） |
| ALL SHORTEST | `[e* ALL SHORTEST 1..5]` | 所有最短路 |
| WSHORTEST | `[e* WSHORTEST(prop) 1..10]` | 加权最短路 |
| ALL WSHORTEST | `[e* ALL WSHORTEST(prop) 1..10]` | 所有加权最短路 |

```cypher
-- 最短路
MATCH (a)-[e:knows* SHORTEST 1..5]->(b) RETURN b.ID, length(e);

-- 所有最短路
MATCH (a)-[e:knows* ALL SHORTEST 1..5]->(b) RETURN b.ID, length(e);

-- 加权最短路
MATCH (a)-[e:road* WSHORTEST(distance) 1..10]->(b) RETURN b.ID, cost(e);

-- TRAIL（不重复边）
MATCH (a)-[e:knows* TRAIL 1..5]->(b) RETURN COUNT(*);

-- ACYCLIC（不重复节点）
MATCH (a)-[e:knows* ACYCLIC 1..5]->(b) RETURN COUNT(*);
```

### 4.3 递归 comprehension（过滤/投影）

```cypher
-- 带过滤条件的递归
MATCH p = (a)-[e:knows*2..3 (r, n | WHERE n.ID <> 0)]->(b) RETURN p;

-- 带投影的递归
MATCH p = (a)-[e:knows*2..3 (r, n | WHERE n.ID <> 0 | {r.date}, {n.ID})]->(b)
RETURN p;
```

语法：`(rel_var, node_var | WHERE condition | {rel_projections}, {node_projections})`

### 4.4 路径函数

```cypher
MATCH p = (a)-[e*1..3]->(b)
RETURN nodes(p), rels(p), length(p), properties(nodes(p), 'name');
```

| 函数 | 说明 |
|------|------|
| `nodes(path)` | 提取路径中的节点列表 |
| `rels(path)` / `relationships(path)` | 提取路径中的关系列表 |
| `length(path)` | 路径长度（跳数） |
| `properties(nodes, prop_name)` | 提取节点/关系列表中的指定属性 |
| `is_trail(path)` | 是否为 trail（不重复边） |
| `is_acyclic(path)` | 是否为无环路径 |
| `cost(e)` | 获取加权最短路的代价 |

### 4.5 关键限制

- `SHORTEST` / `ALL SHORTEST` 下界**必须**是 `1`。
- 上界不能超过 `var_length_extend_max_depth` 设置（默认 30）。
- 加权最短路 `WSHORTEST(prop)` 的权重属性必须是数值类型。

---

## 5. 更新语法（DML）

### 5.1 CREATE

```cypher
-- 创建节点
CREATE (:Person {ID: 1, name: 'Alice'});

-- 创建关系（需先 MATCH 端点）
MATCH (a:Person {ID: 1}), (b:Person {ID: 2})
CREATE (a)-[:KNOWS {since: 2020}]->(b);

-- 批量创建
CREATE (:Person {ID: 3, name: 'Carol'}), (:Person {ID: 4, name: 'Dave'});
```

### 5.2 MERGE

```cypher
-- 基本 MERGE
MERGE (a:Person {ID: 1});

-- 带 ON MATCH / ON CREATE
MERGE (a:Person {ID: 1})
  ON MATCH SET a.name = 'A'
  ON CREATE SET a.age = 20;
```

### 5.3 SET

```cypher
MATCH (a:Person {ID: 1}) SET a.age = 30;

-- 批量设置属性
MATCH (a:Person {ID: 1}) SET a = {name: 'Alice', age: 31};

-- 设置多个属性
MATCH (a:Person {ID: 1}) SET a.name = 'Alice', a.age = 31;
```

### 5.4 DELETE / DETACH DELETE

```cypher
-- 删除节点（需先删关联关系）
MATCH (a:Person {ID: 1}) DELETE a;

-- DETACH DELETE（自动删除关联关系）
MATCH (a:Person {ID: 1}) DETACH DELETE a;

-- 删除关系
MATCH (a)-[e:KNOWS]->(b) WHERE a.ID = 1 DELETE e;
```

---

## 6. DDL 与管理语法

### 6.1 CREATE NODE TABLE

```cypher
-- 基本建表
CREATE NODE TABLE Person(ID INT64, name STRING, PRIMARY KEY(ID));

-- 带默认值
CREATE NODE TABLE Person(
    ID SERIAL, 
    name STRING DEFAULT '', 
    age INT64 DEFAULT 0, 
    PRIMARY KEY(ID)
);

-- IF NOT EXISTS
CREATE NODE TABLE IF NOT EXISTS Person(ID INT64, PRIMARY KEY(ID));

-- 带选项
CREATE NODE TABLE Person(ID INT64, PRIMARY KEY(ID)) WITH (storage_layout='column');

-- 从查询创建
CREATE NODE TABLE NewPerson AS
MATCH (p:Person) WHERE p.age > 18 RETURN p.*;
```

### 6.2 CREATE REL TABLE

```cypher
-- 基本关系表
CREATE REL TABLE Knows(FROM Person TO Person, since DATE, MANY_MANY);

-- 多对一
CREATE REL TABLE LivesIn(FROM Person TO City, since INT64, MANY_ONE);

-- 多个连接
CREATE REL TABLE Follows(FROM User TO User, FROM User TO Page);

-- 关系表组（多种连接）
CREATE REL TABLE GROUP Interacts(
    FROM Person TO Person,
    FROM Person TO Org,
    weight DOUBLE
);

-- IF NOT EXISTS
CREATE REL TABLE IF NOT EXISTS Knows(FROM Person TO Person);
```

### 6.3 ALTER TABLE

```cypher
-- 添加属性
ALTER TABLE Person ADD age INT64 DEFAULT 0;
ALTER TABLE Person ADD IF NOT EXISTS email STRING;

-- 删除属性
ALTER TABLE Person DROP age;
ALTER TABLE Person DROP IF EXISTS email;

-- 重命名表
ALTER TABLE Person RENAME TO People;

-- 重命名属性
ALTER TABLE Person RENAME name TO full_name;

-- 添加关系连接
ALTER TABLE Knows ADD FROM Employee TO Employee;
ALTER TABLE Knows ADD IF NOT EXISTS FROM Student TO Student;

-- 删除关系连接
ALTER TABLE Knows DROP FROM Employee TO Employee;
ALTER TABLE Knows DROP IF EXISTS FROM Student TO Student;
```

### 6.4 DROP

```cypher
DROP TABLE Person;
DROP TABLE IF EXISTS Person;

DROP SEQUENCE my_seq;
DROP SEQUENCE IF EXISTS my_seq;

DROP MACRO my_func;
DROP MACRO IF EXISTS my_func;

DROP GRAPH my_graph;
DROP GRAPH IF EXISTS my_graph;
```

### 6.5 CREATE SEQUENCE

```cypher
CREATE SEQUENCE my_seq;
CREATE SEQUENCE IF NOT EXISTS my_seq;

-- 带选项
CREATE SEQUENCE my_seq
    INCREMENT BY 1
    MINVALUE 0
    MAXVALUE 1000
    START WITH 1
    NO CYCLE;
```

序列函数：
```cypher
RETURN nextval('my_seq');
RETURN currval('my_seq');
```

### 6.6 CREATE TYPE

```cypher
CREATE TYPE point AS STRUCT(x DOUBLE, y DOUBLE);
```

### 6.7 CREATE MACRO

```cypher
-- 位置参数
CREATE MACRO add(a, b) AS a + b;

-- 带默认值
CREATE MACRO greet(name, prefix := 'Hello') AS concat(prefix, ' ', name);
```

### 6.8 COMMENT ON

```cypher
COMMENT ON TABLE Person IS '人员信息表';
```

### 6.9 COPY FROM / TO

```cypher
-- 从文件导入
COPY Person FROM "person.csv" (HEADER=true);

-- 从多个文件
COPY Person FROM ["p1.csv", "p2.csv"];

-- 按列导入
COPY Person FROM ("id.csv", "name.csv") BY COLUMN;

-- 从子查询导入
COPY NewTable FROM (MATCH (p:Person) RETURN p.ID, p.name);

-- 从函数导入
COPY Person FROM parquet_scan("data.parquet");

-- 从参数导入
COPY Person FROM $file_path;

-- 指定列
COPY Person(name, age) FROM "person.csv";

-- 导出到文件
COPY (MATCH (p:Person) RETURN p.ID, p.name) TO "out.csv" (HEADER=true);
```

### 6.10 EXPORT / IMPORT DATABASE

```cypher
EXPORT DATABASE "/path/to/export" (FORMAT='csv', HEADER=true);
IMPORT DATABASE "/path/to/export";
```

### 6.11 事务

```cypher
BEGIN TRANSACTION;
BEGIN TRANSACTION READ ONLY;
COMMIT;
ROLLBACK;
CHECKPOINT;
```

### 6.12 图与数据库管理

```cypher
-- 图管理
CREATE GRAPH g1;
CREATE GRAPH g2 ANY;      -- ANY 图支持动态 label
USE GRAPH g1;
DROP GRAPH g1;

-- 多数据库
ATTACH "other.db" AS other (DBTYPE native);
USE other;
DETACH other;
```

### 6.13 扩展管理

```cypher
LOAD EXTENSION "path_or_name";
LOAD "path_or_name";          -- EXTENSION 关键字可省略
INSTALL my_ext;
FORCE INSTALL my_ext;         -- 强制重新安装
INSTALL my_ext FROM "url";    -- 从指定位置安装
UNINSTALL my_ext;
UPDATE my_ext;
```

注意：`CREATE USER` / `CREATE ROLE` 由扩展解析器处理；未加载对应扩展时不会作为内核语义生效。

---

## 7. 表达式与运算符

### 7.1 运算符优先级（从低到高）

| 优先级 | 运算符 | 说明 |
|--------|--------|------|
| 1 | `OR` | 逻辑或 |
| 2 | `XOR` | 逻辑异或 |
| 3 | `AND` | 逻辑与 |
| 4 | `NOT` | 逻辑非 |
| 5 | `=`, `<>`, `<`, `<=`, `>`, `>=` | 比较 |
| 6 | `\|` | 位或 |
| 7 | `&` | 位与 |
| 8 | `<<`, `>>` | 位移 |
| 9 | `+`, `-` | 加减 |
| 10 | `*`, `/`, `%` | 乘除取模 |
| 11 | `^` | 幂运算 |
| 12 | 字符串/列表/空值运算符 | `IN`, `STARTS WITH` 等 |
| 13 | 一元 `-`, `!`（阶乘） | 前缀负号、后缀阶乘 |
| 14 | `.` 属性访问 | 属性查找 |

### 7.2 比较运算符

```cypher
a = b       -- 等于
a <> b      -- 不等于（不支持 !=）
a < b       -- 小于
a <= b      -- 小于等于
a > b       -- 大于
a >= b      -- 大于等于
```

### 7.3 字符串运算符

```cypher
a STARTS WITH 'prefix'
a ENDS WITH 'suffix'
a CONTAINS 'sub'
a =~ 'regex_pattern'         -- 正则匹配
```

### 7.4 列表运算符

```cypher
x IN [1, 2, 3]              -- 包含判断
list[0]                      -- 索引访问（从 0 开始）
list[1:3]                    -- 切片
list[1..]                    -- 从索引 1 到末尾
list[..3]                    -- 从头到索引 3
```

### 7.5 空值运算符

```cypher
a IS NULL
a IS NOT NULL
```

### 7.6 CASE 表达式

```cypher
-- 简单 CASE
CASE a.type WHEN 'A' THEN 1 WHEN 'B' THEN 2 ELSE 0 END

-- 搜索 CASE
CASE WHEN a.age < 18 THEN 'minor' WHEN a.age >= 65 THEN 'senior' ELSE 'adult' END
```

### 7.7 量词表达式

```cypher
ALL(x IN list WHERE x > 0)
ANY(x IN list WHERE x > 0)
NONE(x IN list WHERE x < 0)
SINGLE(x IN list WHERE x = target)
```

### 7.8 子查询表达式

```cypher
-- EXISTS 子查询
WHERE EXISTS { MATCH (a)-[:KNOWS]->(b) WHERE b.name = 'Alice' }

-- COUNT 子查询
WHERE COUNT { MATCH (a)-[:KNOWS]->() } > 5
```

### 7.9 参数

```cypher
$name          -- 命名参数
$1             -- 位置参数
```

### 7.10 Lambda 表达式（用于高阶函数参数）

```cypher
-- 单参数 lambda
list_transform([1,2,3], x -> x * 2)

-- 多参数 lambda
list_reduce([1,2,3], 0, (acc, x) -> acc + x)
```

### 7.11 字面量

| 类型 | 示例 |
|------|------|
| 整数 | `42`, `0` |
| 浮点 | `3.14`, `1e10`, `2.5E-3` |
| 字符串 | `'hello'`, `"hello"` |
| 布尔 | `TRUE`, `FALSE` |
| 空值 | `NULL` |
| 列表 | `[1, 2, 3]`, `['a', 'b']` |
| 结构体 | `{name: 'Alice', age: 30}` |

转义字符：`\\`, `\'`, `\"`, `\b`, `\f`, `\n`, `\r`, `\t`, `\xHH`, `\uHHHH`, `\uHHHHHHHH`

---

## 8. 数据类型

### 8.1 标量类型

| 类型 | 说明 |
|------|------|
| `BOOL` | 布尔值 |
| `INT8` | 8 位有符号整数 |
| `INT16` | 16 位有符号整数 |
| `INT32` | 32 位有符号整数 |
| `INT64` | 64 位有符号整数 |
| `INT128` | 128 位有符号整数 |
| `UINT8` | 8 位无符号整数 |
| `UINT16` | 16 位无符号整数 |
| `UINT32` | 32 位无符号整数 |
| `UINT64` | 64 位无符号整数 |
| `UINT128` | 128 位无符号整数 |
| `FLOAT` | 单精度浮点 |
| `DOUBLE` | 双精度浮点 |
| `DECIMAL(p, s)` | 定点数（精度, 小数位） |
| `SERIAL` | 自增 INT64 |
| `STRING` | 字符串 |
| `BLOB` | 二进制对象 |
| `UUID` | UUID |
| `JSON` | JSON 数据 |

### 8.2 时间类型

| 类型 | 说明 |
|------|------|
| `DATE` | 日期 |
| `TIMESTAMP` | 微秒精度时间戳 |
| `TIMESTAMP_SEC` | 秒精度时间戳 |
| `TIMESTAMP_MS` | 毫秒精度时间戳 |
| `TIMESTAMP_NS` | 纳秒精度时间戳 |
| `TIMESTAMP_TZ` | 带时区时间戳 |
| `INTERVAL` | 时间间隔 |

### 8.3 复合类型

| 类型 | DDL/CAST 语法 | 示例 |
|------|---------------|------|
| 列表 | `T[]` | `INT64[]` |
| 固定长度数组 | `T[n]` | `DOUBLE[3]` |
| 嵌套列表 | `T[][]` | `INT64[][]` |
| 结构体 | `STRUCT(field T, ...)` | `STRUCT(x DOUBLE, y DOUBLE)` |
| 映射 | `MAP(K, V)` | `MAP(STRING, INT64)` |
| 联合 | `UNION(tag1 T1, ...)` | `UNION(num INT64, str STRING)` |

### 8.4 图类型（内部使用）

| 类型 | 说明 |
|------|------|
| `NODE` | 节点 |
| `REL` | 关系 |
| `RECURSIVE_REL` | 递归关系/路径 |
| `INTERNAL_ID` | 内部 ID |

---

## 9. 内置函数参考

### 9.1 聚合函数

| 函数 | 说明 |
|------|------|
| `COUNT(*)` | 计数所有行 |
| `COUNT(expr)` | 计数非空值 |
| `COUNT(DISTINCT expr)` | 计数去重非空值 |
| `SUM(expr)` | 求和 |
| `AVG(expr)` | 平均值 |
| `MIN(expr)` | 最小值 |
| `MAX(expr)` | 最大值 |
| `COLLECT(expr)` | 收集为列表 |

### 9.2 数学函数

| 函数 | 说明 |
|------|------|
| `ABS(x)` | 绝对值 |
| `CEIL(x)` / `CEILING(x)` | 向上取整 |
| `FLOOR(x)` | 向下取整 |
| `ROUND(x)` | 四舍五入 |
| `SQRT(x)` | 平方根 |
| `CBRT(x)` | 立方根 |
| `POW(x, y)` / `POWER(x, y)` | 幂 |
| `EXP(x)` | e^x |
| `LOG(x)` / `LN(x)` | 自然对数 |
| `LOG2(x)` | 以 2 为底对数 |
| `LOG10(x)` | 以 10 为底对数 |
| `SIGN(x)` | 符号（-1/0/1） |
| `EVEN(x)` | 取最近偶数 |
| `FACTORIAL(x)` | 阶乘 |
| `PI()` | 圆周率 |
| `RAND()` | 随机数 [0,1) |
| `SET_SEED(x)` | 设置随机种子 |
| `GAMMA(x)` | Gamma 函数 |
| `LGAMMA(x)` | Log-Gamma 函数 |
| `DEGREES(x)` | 弧度转角度 |
| `RADIANS(x)` | 角度转弧度 |

**三角函数：** `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `COT`

### 9.3 字符串函数

| 函数 | 说明 |
|------|------|
| `LOWER(s)` / `TOLOWER(s)` / `LCASE(s)` | 转小写 |
| `UPPER(s)` / `TOUPPER(s)` / `UCASE(s)` | 转大写 |
| `INITCAP(s)` | 首字母大写 |
| `LTRIM(s)` | 去左空白 |
| `RTRIM(s)` | 去右空白 |
| `TRIM(s)` | 去两端空白 |
| `SUBSTR(s, start, len)` / `SUBSTRING(s, start, len)` | 子串 |
| `LEFT(s, n)` | 左取 n 字符 |
| `RIGHT(s, n)` | 右取 n 字符 |
| `LPAD(s, len, pad)` | 左填充 |
| `RPAD(s, len, pad)` | 右填充 |
| `REPEAT(s, n)` | 重复 |
| `REVERSE(s)` | 反转 |
| `CONCAT(a, b, ...)` | 拼接 |
| `CONCAT_WS(sep, a, b, ...)` | 带分隔符拼接 |
| `CONTAINS(s, sub)` | 是否包含 |
| `STARTS_WITH(s, prefix)` / `PREFIX(s, prefix)` | 前缀判断 |
| `ENDS_WITH(s, suffix)` / `SUFFIX(s, suffix)` | 后缀判断 |
| `STRING_SPLIT(s, sep)` / `STR_SPLIT(s, sep)` | 分割 |
| `SPLIT_PART(s, sep, idx)` | 分割后取第 idx 部分 |
| `LEVENSHTEIN(a, b)` | 编辑距离 |
| `SIZE(s)` | 字符串长度 |

**正则函数：**

| 函数 | 说明 |
|------|------|
| `REGEXP_FULL_MATCH(s, pattern)` | 完整匹配 |
| `REGEXP_MATCHES(s, pattern)` | 是否匹配 |
| `REGEXP_REPLACE(s, pattern, replacement)` | 替换 |
| `REGEXP_EXTRACT(s, pattern)` | 提取首个匹配 |
| `REGEXP_EXTRACT_ALL(s, pattern)` | 提取所有匹配 |
| `REGEXP_SPLIT_TO_ARRAY(s, pattern)` | 按正则分割 |

### 9.4 列表/数组函数

| 函数 | 说明 |
|------|------|
| `LIST_CREATION(a, b, ...)` | 创建列表 |
| `LIST_RANGE(start, end)` / `RANGE(start, end)` | 生成范围列表 |
| `LIST_EXTRACT(list, idx)` / `LIST_ELEMENT(list, idx)` | 按索引取值 |
| `LIST_APPEND(list, val)` / `ARRAY_APPEND(list, val)` | 末尾追加 |
| `LIST_PREPEND(list, val)` / `ARRAY_PREPEND(list, val)` | 头部插入 |
| `LIST_CONCAT(a, b)` / `LIST_CAT(a, b)` / `ARRAY_CONCAT(a, b)` | 列表拼接 |
| `LIST_CONTAINS(list, val)` / `LIST_HAS(list, val)` | 包含判断 |
| `LIST_POSITION(list, val)` / `LIST_INDEX_OF(list, val)` | 查找位置 |
| `SIZE(list)` | 列表长度 |
| `LIST_SORT(list)` | 排序 |
| `LIST_REVERSE_SORT(list)` | 降序排序 |
| `LIST_REVERSE(list)` | 反转 |
| `LIST_DISTINCT(list)` / `LIST_UNIQUE(list)` | 去重 |
| `LIST_SUM(list)` | 列表求和 |
| `LIST_PRODUCT(list)` | 列表求积 |
| `LIST_ANY_VALUE(list)` | 列表中任意值 |
| `LIST_HAS_ALL(list, sublist)` | 是否包含所有元素 |
| `LIST_TO_STRING(list, sep)` | 列表转字符串 |

**高阶列表函数（接受 lambda）：**

| 函数 | 说明 |
|------|------|
| `LIST_TRANSFORM(list, x -> expr)` | 映射转换 |
| `LIST_FILTER(list, x -> expr)` | 过滤 |
| `LIST_REDUCE(list, init, (acc, x) -> expr)` | 归约 |
| `LIST_ANY(list, x -> expr)` | 任意满足 |
| `LIST_ALL(list, x -> expr)` | 全部满足 |
| `LIST_NONE(list, x -> expr)` | 全不满足 |
| `LIST_SINGLE(list, x -> expr)` | 恰好一个满足 |

**向量/数组相似度函数：**

| 函数 | 说明 |
|------|------|
| `ARRAY_COSINE_SIMILARITY(a, b)` | 余弦相似度 |
| `ARRAY_DISTANCE(a, b)` | 欧氏距离 |
| `ARRAY_SQUARED_DISTANCE(a, b)` | 平方欧氏距离 |
| `ARRAY_INNER_PRODUCT(a, b)` / `ARRAY_DOT_PRODUCT(a, b)` | 内积 |
| `ARRAY_CROSS_PRODUCT(a, b)` | 叉积 |

### 9.5 日期/时间函数

| 函数 | 说明 |
|------|------|
| `DATE(str)` | 解析日期 |
| `CURRENT_DATE()` | 当前日期 |
| `TIMESTAMP(str)` | 解析时间戳 |
| `CURRENT_TIMESTAMP()` | 当前时间戳 |
| `TO_TIMESTAMP(epoch)` | 从 epoch 转时间戳 |
| `INTERVAL(str)` / `DURATION(str)` | 解析时间间隔 |
| `MAKE_DATE(year, month, day)` | 构造日期 |
| `DATE_PART(part, date)` | 提取日期部分 |
| `DATE_TRUNC(part, date)` | 截断日期 |
| `DAY_NAME(date)` | 星期名 |
| `MONTH_NAME(date)` | 月名 |
| `LAST_DAY(date)` | 该月最后一天 |
| `EPOCH_MS(ts)` | 时间戳转毫秒 epoch |
| `TO_EPOCH_MS(ts)` | 同上 |
| `CENTURY(date)` | 世纪 |
| `GREATEST(a, b)` | 取较大值 |
| `LEAST(a, b)` | 取较小值 |

**间隔转换函数：** `TO_YEARS`, `TO_MONTHS`, `TO_DAYS`, `TO_HOURS`, `TO_MINUTES`, `TO_SECONDS`, `TO_MILLISECONDS`, `TO_MICROSECONDS`

**常见 DATE 过滤写法：**

```cypher
-- 最近 30 个自然日（含今天）
MATCH (a:YourLabel)
WHERE a.bizDate >= CURRENT_DATE() - 29
  AND a.bizDate <= CURRENT_DATE()
RETURN a;

-- 指定基准日往前 30 个自然日（含基准日）
MATCH (a:YourLabel)
WHERE a.bizDate >= DATE('2026-04-04') - 29
  AND a.bizDate <= DATE('2026-04-04')
RETURN a;

-- INTERVAL 写法
MATCH (a:YourLabel)
WHERE a.bizDate >= CURRENT_DATE() - INTERVAL('29 days')
  AND a.bizDate <= CURRENT_DATE()
RETURN a;
```

说明：
- 上面使用 `- 29`，表示“含今天/含基准日在内的近 30 天”。
- 如果写成 `>= CURRENT_DATE() - 30`，通常会得到 31 个自然日窗口。
- 当前项目里“当前日期”建议写为 `CURRENT_DATE()`，不要写成无参的 `DATE()`。

**常见 TIMESTAMP 用法：**

```cypher
-- 构造时间戳
RETURN timestamp('2025-02-01 11:22:33.53');
RETURN CURRENT_TIMESTAMP();

-- 比较时间范围
MATCH (e:Event)
WHERE e.ts >= timestamp('2025-02-01 00:00:00')
  AND e.ts <  timestamp('2025-03-01 00:00:00')
RETURN e;

-- 最近 30 天（按当前时刻回溯 30 x 24 小时）
MATCH (e:Event)
WHERE e.ts >= CURRENT_TIMESTAMP() - INTERVAL('30 days')
  AND e.ts <= CURRENT_TIMESTAMP()
RETURN e;

-- 与 INTERVAL 做加减
MATCH (e:Event)
RETURN e.ts + INTERVAL('2 hours'),
       e.ts - INTERVAL('30 days');

-- 两个 TIMESTAMP 相减，结果为时间差
MATCH (e:Event)
RETURN e.ts - timestamp('2025-01-01 00:00:00');
```

说明：
- `timestamp('YYYY-MM-DD HH:MM:SS[.fraction]')` 可用于构造 `TIMESTAMP` 字面量。
- 当前项目里“当前时间戳”建议写为 `CURRENT_TIMESTAMP()`。
- `TIMESTAMP +/- INTERVAL` 是支持的；两个 `TIMESTAMP` 相减会得到时间差。
- 如果你要的是“最近 30 个自然日”，优先用上面的 `DATE` 写法；如果你要的是“从当前时刻往前滚动 30 天”，用这里的 `TIMESTAMP` 写法更合适。

### 9.6 类型转换函数

```cypher
CAST(expr AS type)
CAST('42' AS INT64)
CAST(3.14 AS STRING)
```

### 9.7 结构体/映射/联合函数

| 函数 | 说明 |
|------|------|
| `STRUCT_PACK(k1:=v1, k2:=v2)` | 创建结构体 |
| `STRUCT_EXTRACT(s, key)` | 提取字段 |
| `MAP(keys, values)` | 创建映射 |
| `MAP_EXTRACT(map, key)` / `ELEMENT_AT(map, key)` | 取值 |
| `MAP_KEYS(map)` | 所有键 |
| `MAP_VALUES(map)` | 所有值 |
| `CARDINALITY(map)` | 映射大小 |
| `UNION_VALUE(tag:=val)` | 创建联合值 |
| `UNION_TAG(u)` | 获取标签 |
| `UNION_EXTRACT(u, tag)` | 提取值 |
| `KEYS(struct)` | 结构体的键列表 |

### 9.8 节点/关系函数

| 函数 | 说明 |
|------|------|
| `ID(n)` / `id(n)` | 内部 ID |
| `OFFSET(n)` | 偏移量 |
| `LABEL(n)` / `LABELS(n)` | 节点/关系标签 |
| `START_NODE(e)` | 关系起始节点 |
| `END_NODE(e)` | 关系结束节点 |

### 9.9 哈希函数

| 函数 | 说明 |
|------|------|
| `MD5(s)` | MD5 哈希 |
| `SHA256(s)` | SHA-256 哈希 |
| `HASH(expr)` | 通用哈希 |

### 9.10 Blob 函数

| 函数 | 说明 |
|------|------|
| `OCTET_LENGTH(blob)` | 字节长度 |
| `ENCODE(blob)` | 编码 |
| `DECODE(s)` | 解码 |

### 9.11 工具函数

| 函数 | 说明 |
|------|------|
| `COALESCE(a, b, ...)` | 第一个非空值 |
| `IF_NULL(expr, default)` | 空值替代 |
| `NULL_IF(a, b)` | 相等则返回 NULL |
| `TYPE_OF(expr)` | 返回表达式类型名 |
| `GEN_RANDOM_UUID()` | 生成随机 UUID |
| `COUNT_IF(expr)` | 条件计数 |

---

## 10. CALL 能力

### 10.1 会话/系统设置

```cypher
CALL threads=8;
CALL timeout=30000;
CALL var_length_extend_max_depth=100;
CALL recursive_pattern_semantic='TRAIL';
CALL current_setting('threads') RETURN *;
```

**内置设置项：**

| 设置项 | 说明 |
|--------|------|
| `threads` | 查询线程数 |
| `timeout` | 查询超时（ms） |
| `warning_limit` | 警告数量限制 |
| `progress_bar` | 是否显示进度条 |
| `var_length_extend_max_depth` | 可变长扩展最大深度 |
| `sparse_frontier_threshold` | 稀疏前沿阈值 |
| `enable_semi_mask` | 启用半掩码 |
| `disable_map_key_check` | 禁用 map key 检查 |
| `enable_zone_map` | 启用 zone map |
| `home_directory` | 主目录路径 |
| `file_search_path` | 文件搜索路径 |
| `recursive_pattern_semantic` | 递归模式语义 |
| `recursive_pattern_factor` | 递归模式因子 |
| `checkpoint_threshold` | Checkpoint 阈值 |
| `auto_checkpoint` | 自动 checkpoint |
| `force_checkpoint_on_close` | 关闭时强制 checkpoint |
| `spill_to_disk` | 允许溢出到磁盘 |
| `enable_plan_optimizer` | 启用计划优化器 |
| `enable_internal_catalog` | 启用内部 catalog |
| `debug_enable_multi_writes` | 调试：多写 |

### 10.2 系统信息表函数

```cypher
CALL SHOW_TABLES() RETURN *;
CALL SHOW_GRAPHS() RETURN *;
CALL TABLE_INFO('Person') RETURN *;
CALL STATS_INFO('Person') RETURN *;
CALL STORAGE_INFO('Person') RETURN *;
CALL SHOW_SEQUENCES() RETURN *;
CALL SHOW_FUNCTIONS() RETURN *;
CALL SHOW_MACROS() RETURN *;
CALL SHOW_INDEXES() RETURN *;
CALL SHOW_WARNINGS() RETURN *;
CALL CLEAR_WARNINGS();
CALL SHOW_CONNECTION('Knows') RETURN *;
CALL SHOW_ATTACHED_DATABASES() RETURN *;
CALL SHOW_LOADED_EXTENSIONS() RETURN *;
CALL SHOW_OFFICIAL_EXTENSIONS() RETURN *;
CALL DB_VERSION() RETURN *;
CALL CATALOG_VERSION() RETURN *;
CALL CURRENT_SETTING('threads') RETURN *;
```

### 10.3 存储/诊断函数

```cypher
CALL BM_INFO() RETURN *;           -- 缓冲池信息
CALL FILE_INFO('Person') RETURN *;  -- 文件信息
CALL DISK_SIZE_INFO() RETURN *;     -- 磁盘大小
CALL FREE_SPACE_INFO() RETURN *;    -- 空闲空间
```

### 10.4 投影图函数

```cypher
CALL PROJECT_GRAPH_NATIVE('pg', ['Person'], ['Knows']);
CALL PROJECT_GRAPH_CYPHER('pg', 'MATCH (a:Person)-[e:Knows]->(b:Person) RETURN a, e, b');
CALL SHOW_PROJECTED_GRAPHS() RETURN *;
CALL PROJECTED_GRAPH_INFO('pg') RETURN *;
CALL DROP_PROJECTED_GRAPH('pg');
```

### 10.5 扫描函数

```cypher
CALL PARQUET_SCAN("data.parquet") RETURN *;
CALL NPY_SCAN("data.npy") RETURN *;
```

---

## 11. EXPLAIN / PROFILE

```cypher
-- 物理执行计划（默认）
EXPLAIN MATCH (a:Person)-[]->(b:Person) RETURN COUNT(*);

-- 逻辑执行计划
EXPLAIN LOGICAL MATCH (a:Person)-[]->(b:Person) RETURN COUNT(*);

-- 运行时性能分析
PROFILE MATCH (a:Person)-[]->(b:Person) RETURN COUNT(*);
```

| 模式 | 说明 |
|------|------|
| `EXPLAIN` | 显示物理执行计划，不实际执行 |
| `EXPLAIN LOGICAL` | 显示逻辑执行计划，不实际执行 |
| `PROFILE` | 实际执行并显示运行时性能指标 |

可用于 DDL、DML、CALL 等所有语句类型。

---

## 12. 已知限制与不支持项

### 12.1 运算符限制

| 项目 | 当前状态 |
|------|----------|
| `!=` | **不支持**，必须使用 `<>` |
| 链式比较 `a=b=c` | **不支持** |

### 12.2 子句限制

| 项目 | 当前状态 |
|------|----------|
| `RETURN` 中途出现 | **不支持**（必须在查询末尾） |
| `WITH` 未别名表达式 | **不支持** |
| `WITH ORDER BY` 无 `SKIP/LIMIT` | **不支持** |
| `UNION` 与 `UNION ALL` 混用 | **不支持** |
| `LOAD FROM` 子查询源 | **不支持** |
| `LOAD FROM` 表函数源 | **不支持** |

### 12.3 DML 限制

| 项目 | 当前状态 |
|------|----------|
| `CREATE` 递归关系（`*`） | **不支持** |
| `CREATE` 无向关系 | **不支持** |
| `CREATE` 普通图多标签节点 | **不支持**（`ANY` 图除外） |
| `SET` 主键属性 | **不支持** |
| `DELETE` 无向关系 | **不支持** |
| `DETACH DELETE` 关系 | **不支持** |

### 12.4 其他限制

| 项目 | 当前状态 |
|------|----------|
| 无向关系匹配在 `storage_direction != both` 场景 | **不支持** |
| `ORDER BY` 对 `NODE/REL/LIST/STRUCT/MAP/UNION` 等复杂类型 | **不支持** |
| `SKIP/LIMIT` 非参数/字面量表达式 | **不支持** |
| `RETURN/WITH *` 在作用域为空时 | **不支持** |
| `CALL setting=...` 非字面量值 | **不支持**（需 literal） |

---

## 13. 完整示例

### 13.1 建库建表与数据插入

```cypher
-- 建表
CREATE NODE TABLE Person(ID INT64, name STRING, age INT64, PRIMARY KEY(ID));
CREATE NODE TABLE City(ID INT64, name STRING, PRIMARY KEY(ID));
CREATE REL TABLE LIVES_IN(FROM Person TO City, since INT64, MANY_ONE);
CREATE REL TABLE KNOWS(FROM Person TO Person, weight DOUBLE, MANY_MANY);

-- 插入数据
CREATE (:Person {ID: 1, name: 'Alice', age: 30});
CREATE (:Person {ID: 2, name: 'Bob', age: 28});
CREATE (:Person {ID: 3, name: 'Carol', age: 35});
CREATE (:City {ID: 10, name: 'Shanghai'});
CREATE (:City {ID: 11, name: 'Beijing'});

-- 创建关系
MATCH (a:Person {ID: 1}), (c:City {ID: 10})
CREATE (a)-[:LIVES_IN {since: 2020}]->(c);

MATCH (a:Person {ID: 1}), (b:Person {ID: 2})
CREATE (a)-[:KNOWS {weight: 0.9}]->(b);

MATCH (a:Person {ID: 2}), (b:Person {ID: 3})
CREATE (a)-[:KNOWS {weight: 0.7}]->(b);
```

### 13.2 基本查询

```cypher
-- 简单查询
MATCH (p:Person) WHERE p.age > 25
RETURN p.name, p.age ORDER BY p.age DESC;

-- 关系查询
MATCH (p:Person)-[r:LIVES_IN]->(c:City)
RETURN p.name AS person, c.name AS city, r.since AS since;

-- 多跳查询
MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
WHERE a.ID = 1
RETURN c.name;
```

### 13.3 高级查询

```cypher
-- 可变长路径
MATCH (a:Person {ID: 1})-[e:KNOWS*1..3]->(b:Person)
RETURN b.name, length(e) AS hops;

-- 聚合 + WITH
MATCH (p:Person)-[:KNOWS]->(friend)
WITH p, COUNT(friend) AS friend_count
WHERE friend_count >= 1
RETURN p.name, friend_count;

-- UNWIND + 子查询
UNWIND [1, 2, 3] AS id
MATCH (p:Person {ID: id})
RETURN p.name;

-- EXISTS 子查询
MATCH (p:Person)
WHERE EXISTS { MATCH (p)-[:LIVES_IN]->(:City {name: 'Shanghai'}) }
RETURN p.name;
```

---

## 14. 语法与能力验证来源

核心依据：

| 来源 | 文件路径 |
|------|----------|
| ANTLR4 语法 | `src/antlr4/Cypher.g4` |
| 语句路由 | `src/parser/transformer.cpp` |
| 查询/子句转换 | `src/parser/transform/transform_*.cpp` |
| 绑定与限制 | `src/binder/bind/**/*.cpp` |
| 语句与子句枚举 | `src/include/common/enums/statement_type.h`, `clause_type.h` |
| 设置项 | `src/include/main/settings.h`, `src/main/settings.cpp`, `src/main/db_config.cpp` |
| 函数注册 | `src/function/function_collection.cpp` |
| CALL 函数 | `src/include/function/table/simple_table_function.h`, `standalone_call_function.h` |
| Shell | `tools/shell/shell_runner.cpp` |
| 使用入口 | `examples/cpp/main.cpp`, `examples/c/main.c`, `tools/python_api/src_py/` |
| 测试覆盖 | `test/test_files/{shortest_path,recursive_join,path,graph,ddl,dml_node,dml_rel,copy,transaction}` |
