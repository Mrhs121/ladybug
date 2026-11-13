# lbug 的 Iceberg 映射表如何构建 CSR、执行多跳查询并回湖取属性

本文记录当前工作区的 `iceberg-query-csr` POC，核验更新至 2026-11-13。三张原始 Iceberg 表保存业务数据，lbug 主库 `graph.db` 保存图映射定义。持久模式在独立文件 `graph.db.csr.db` 中建立两个方向的压缩稀疏行（CSR）邻接索引，分别回答“这个用户买过哪些商品”和“这个商品被哪些用户买过”。每条边还保存订单号。因此 lbug 可以遍历购买关系，再用命中的订单号和用户主键从原始快照取金额和姓名。

以“谁和 Jo 买过同一件商品，对方哪笔订单的金额小于 10”为例，构建和查询发生在两个时刻。

```text
建索引时
  湖中 users.uid、products.pid、orders.(oid, uid, pid)
       → 为用户和商品分配槽位，把每笔订单的两端换成槽位
       → 写 graph.db.csr.db：从用户找商品、从商品找用户，两个方向都能找到订单

查明细时，复用上面建好的索引
  Jo 的 uid 12 → 用户槽位 0 ──正向──→ 商品槽位 0 ──反向──→ 用户槽位 1，订单号 10002
  订单号 10002 → 在 orders 快照取 amt=3，通过 amt<10 的过滤
  用户槽位 1   → 查字典得 uid=42 → 在 users 快照取 uname=Lin
  返回 (10002, 42, Lin, 3)
```

图中省略了反向遍历遇到 Jo 本人的一条边。下面展开这个过程，并区分**普通映射表扫描**和**已接入批量键下推的两跳计划**。两者都会遍历 CSR，发给 DuckDB 的属性查询不同。

## 为什么多跳可以不下发 DuckDB 多表 JOIN

**lbug 原有的外表图查询确实有一条多表 JOIN 路径。** 单独调用 `iceberg_scan` 只会扫描指定的表。要把三张外表作为图查询，lbug 需要知道哪张表表示购买关系，以及哪两列指向端点。若顶点和边来自同一个挂载库、扫描支持下推，且优化器识别出连通的固定跳数图模式，`ForeignJoinPushDownOptimizer` 会把这段图计划改写成一条 SQL，交给外部引擎。在 DuckDB 上查“与 Jo 买过同一商品的其他人”时，三张原表会出现五个别名。订单表出现两次，分别代表 Jo 的购买 `a` 和另一位买家的购买 `b`。查询依靠 `a.pid = p.pid` 和 `b.pid = p.pid` 配出购买同一商品的两笔订单。lbug 自有原生关系表本来就能走其自身的 CSR。

**映射表把单笔订单的端点匹配移到了建索引或追加边的时候。** 订单两端在对应快照都存在，才写入一条以用户槽位为入口的正向邻接和一条以商品槽位为入口的反向邻接。两个方向使用同一套商品槽位。第一跳取出的商品槽位可以直接作为第二跳的输入，运行时由此省去按商品主键连接两份订单的工作。

| 查询要做的事 | DuckDB 多表 JOIN 怎样做 | lbug 如何用 CSR 做 |
|---|---|---|
| 找 Jo 买过的商品 | 用 `u.uid = a.uid` 找到订单，用 `a.pid = p.pid` 匹配真实商品 | `uid=12` 查字典得用户槽位 0，读正向邻接，第一笔购买给出商品槽位 0 |
| 找同一商品上的其他购买 | 再用 `p.pid = b.pid` 匹配第二份订单 | 直接读**商品槽位 0 的反向邻接**，得到该商品上的每一笔购买，包含另一端用户槽位和本笔订单号 |
| 识别另一位买家 | 用 `b.uid = v.uid` 匹配用户，排除 Jo | 比较用户槽位排除 Jo，剩下用户槽位 1，经字典还原 `uid=42`，边上已有 `oid=10002` |

索引存的是**一跳邻接**。两跳路径在查询时组合，图中没有预存“Jo 和 Lin 共同买过商品”这条关系。对固定起点的顺序遍历，要访问多少邻接项取决于 Jo 的订单数和每笔订单所指商品的购买次数。同一商品下有多笔购买就能形成多条路径。本例命中批量键下推时，候选路径形成之后，才轮到 DuckDB 按 `oid` 或 `uid` 读取金额或姓名。普通图计划则可能在展开某一跳时就请求属性列。

**代码从表定义处就选择了不同入口。** 原外表关系将外部扫描函数登记在 catalog 中，旧优化器通过 `hasForeignScanFunction` 等检查收集可下推的图模式。`format='iceberg_stable_csr'` 则将关系交给内核映射后端的 `MappedEdge`。普通 `MATCH` 的关系扫描从这里读邻接。命中专用两跳规则时，`ICEBERG_CSR_KEY_LOOKUP` 直接操作两个原生游标。第一版事务内临时 CSR 已经让 lbug 自己找下一跳，后来的批量 `IN` 进一步缩小了回湖取属性的结果集。因此关掉 `iceberg_poc_key_pushdown` 后，图路径仍由 lbug 计算。

建索引和使用索引的成本也可以单独观察。临时模式在事务首次需要拓扑时匹配单笔订单的两端。持久模式让相同快照的后续事务直接复用结果。一亿订单的分批预构建程序确实用过 `orders JOIN users JOIN products`，目的是把**每笔订单**翻译成一对有效槽位并写入双向索引。后续查询复用这些邻接，即使做四跳，也无需重新向 DuckDB 下发拼路径的多表 SQL。该数据集的四跳计数实测计划有四个 lbug `SCAN_REL_TABLE`，按正、反、正、反四个方向展开，结果 229,213 与同快照 JOIN 一致。业务属性的扫描量仍取决于查询形状和湖表布局。

## 1. 原始输入与映射定义

样例故意让两人购买同一商品，并保留一个没有订单的用户和一个没有卖出的商品。槽位列由 lbug 建索引时生成，原始 Iceberg 表只有业务列。

| `ecom.users.uid` | `uname` | 用户槽位 |
|---:|---|---:|
| 12 | Jo | 0 |
| 42 | Lin | 1 |
| 77 | Mei | 2 |

| `ecom.products.pid` | `pname` | 商品槽位 |
|---:|---|---:|
| 103 | Pen | 0 |
| 205 | Book | 1 |

| `ecom.orders.oid` | `uid` | `pid` | `amt` |
|---:|---:|---:|---:|
| 10001 | 12 | 103 | 2 |
| 10002 | 42 | 103 | 3 |

以下是本例在 lbug 主库执行的完整映射 DDL。直接打开内置映射支持的文件型主库，例如 `graph.db`。将连接设置 `iceberg_poc_catalog` 指向实验用的 SQLite Iceberg catalog。

```cypher
CALL iceberg_poc_catalog='/替换为本次实验的绝对路径/catalog.sqlite';

CREATE NODE TABLE User(uid INT64 PRIMARY KEY, uname STRING)
WITH (storage = 'ecom.users', format = 'iceberg_stable_csr');

CREATE NODE TABLE Product(pid INT64 PRIMARY KEY, pname STRING)
WITH (storage = 'ecom.products', format = 'iceberg_stable_csr');

CREATE REL TABLE Purchased(FROM User TO Product, oid INT64, amt INT64)
WITH (storage = 'ecom.orders KEY oid FROM uid TO pid', format = 'iceberg_stable_csr');
```

`FROM User TO Product` 声明图中的方向。`storage` 是内核映射后端解析的映射字符串，其中 `KEY oid` 选定订单的唯一业务键，`FROM uid` 取订单的 `uid` 匹配 `User` 的主键，`TO pid` 取订单的 `pid` 匹配 `Product` 的主键。图里声明的 `uname`、`pname`、`amt` 按同名湖列取值。建表只登记映射。本次空库实测，执行完三条 DDL 尚未生成 CSR 文件。

## 2. 首次怎样把湖表变成持久 CSR

### 2.1 选定快照，给端点编号

1. **固定输入。** 一个 lbug 事务首次访问映射数据时，`LakeRead::get` 创建事务内的读取对象。它用一次 SQLite 只读查询取出 catalog 中各表的 `metadata_location`。后续扫描使用各自的不可变 metadata JSON 路径。一个图读版本因此对应 `(users 快照, orders 快照, products 快照)` 三元组。三张表分别提交，订单已到、顶点未到也是合法的快照组合。
2. **建立顶点字典。** `NativeStore::stableNodes` 第一次分别从用户表读 `uid`、从商品表读 `pid`，按主键升序排列，检查 `INT64`、非空和唯一。从 0 开始编号。样例中 `User[0]=12`、`User[1]=42`，而 `Product[0]=103`。编号须和顶点类型一起解释。字典在独立库中分块保存，每块最多 4,096 个键，后续既能从业务键找槽位，也能从槽位还原业务键。
3. **把订单翻译成边。** 首次需要关系拓扑时，`LakeRead::stableGeneration` 按映射及快照三元组查版本。在空独立库上，`NativeStore::stablePin` 调用 `LakeRead::mapEndpoints` 读取端点主键及订单的 `oid, uid, pid`，订单按 `oid` 排序。它先按主键匹配两端，稳定模式再用本版本的顶点字典确定最终槽位。两端存在才能产生边。整个步骤无需读取 `uname`、`pname` 或 `amt`。

翻译结果恰好是原生写入所需的四列。表中两行已通过直接打开生成的独立库核实。

| 源槽位 `src` | 目标槽位 `dst` | `lake_row` | `lake_key` |
|---:|---:|---:|---:|
| 0 | 0 | 0 | 10001 |
| 1 | 0 | 1 | 10002 |

`lake_key` 就是订单的业务主键 `oid`，后面的按键回湖会用到它。`lake_row` 是映射层给订单分配的逻辑序号。首批数据中它等于按 `oid` 排序后的行号。追加时从上一快照的订单总行数往后分配，等待端点的订单也会保留这个序号。它和 Parquet 文件名、row group、物理行号之间没有换算关系。

空缓存首建时，稳定字典读取顶点键之后，`mapEndpoints` 还要从事务列缓存取两张顶点表的键。缓存未命中时会再次扫键列。在线构建也会把订单的三列拓扑键物化到内存。第 4.3 节的一亿订单基线因此使用了单独的分批构建程序。

### 2.2 从四列输入写出两个方向

持久化文件是主库旁的一份原生 lbug 数据库。主库叫 `graph.db` 时，文件名为 `graph.db.csr.db`。其中的 `CsrSlots(pos INT64 PRIMARY KEY)` 存放连续槽位，槽位数取两端顶点数的最大值，本例初始为 3 个，编号 `0,1,2`。物理关系表名为 `StableEdges_<映射指纹>`。下面用 `StableEdges_example` 表示这张表。

```cypher
CREATE REL TABLE StableEdges_example(
    FROM CsrSlots TO CsrSlots, lake_row INT64, lake_key INT64
);
```

物理两端共用 `CsrSlots`。例如第一条物理边为 `0 → 0`，映射层按关系的起点、终点类型把它解释为 `User(12) → Product(103)`。同一对端点的多笔订单保留为多条边，每条有自己的 `lake_key`。

1. **原生 COPY 的入口。** `copyTopology` 把四列注册成当前写连接可扫描的 `TopologyInput`，按批次提供 `(src,dst,lake_row,lake_key)`，然后执行 `COPY StableEdges_<指纹> FROM TopologyInput`。这里 COPY 的内容仅为拓扑及定位键，原始表和业务属性继续保存在湖中。
2. **按方向和起查点分组。** lbug 的 `PlanMapper::mapCopyRelFrom` 为这个关系表创建正向和反向写入。正向以 `src` 为起查槽位，把 `dst` 作为邻居。反向以 `dst` 为起查槽位，把 `src` 作为邻居。`Partitioner` 再按起查槽位所属的 node group 分区。
3. **数度数，分配区间，填入边。** 在每个方向的每个组内，`CopyRelBatchInsert` 先为每个槽位统计关系数 `length`。原生批量写入器对这些长度做累加，得到各邻接段的起始位置。它为每条输入边分配该段中的下一个位置，按位置写入邻居、原生关系 ID 和定位列。因此给定一个槽位，读取器可以直接找到它的邻接段。
4. **提交与落盘。** 独立库的同一笔图写事务发布物理关系和图版本。原生存储管理事务提交、日志、数据页和 checkpoint。持久层按方向和 node group 存放 CSR 头（`offset`、`length`）及邻接列。读取器从头部取 `start` 与实际 `length`，访问 `[start, start + length)`。原生存储在部分写入或 checkpoint 路径可以为区域预留空位。提交后的新边可通过原生事务读取，查询不需要等一次全库 checkpoint。

以磁盘上已经形成的一个 CSR 组为例，本次构建每个 node group 覆盖 131,072 个连续槽位。下面是按一个槽位读取已持久化邻接的等价写法。原生读取器还会合并本事务可见的已提交内存记录。

```text
group = slot // 131072
i     = slot % 131072
header = 所选方向在 group 中的 CSR 头
start  = 0（i 为 0 时），否则取 header.offset[i-1]
length = header.length[i]
读取该组邻接列从 start 开始的 length 条记录
```

### 2.3 手算一遍相同的邻接关系

下面用无空隙的数组表示两笔订单，便于手算。选择临时格式 `iceberg_poc` 时，映射后端在 `LakeRead::adjacency` 中就按这个算法实际建数组。上节的持久格式使用原生分组存储，同一邻接关系在磁盘上会多出组头、列块等结构。

| 方向 | 按槽位排列的度数 | 边界数组 `indptr` | 各位置的 `(邻居槽位, lake_row)` |
|---|---|---|---|
| 正向，从用户找商品 | `[1, 1, 0]` | `[0, 1, 2, 2]` | `[(0, 0), (0, 1)]` |
| 反向，从商品找用户 | `[2, 0]` | `[0, 2, 2]` | `[(0, 0), (1, 1)]` |

临时构建先遍历有效边，分别累加 `fwd[src+1]` 与 `bwd[dst+1]`，求前缀和得到上表的 `indptr`，再遍历一次写入两个方向的邻居和订单序号。读槽位 `s` 时使用半开区间 `[indptr[s], indptr[s+1])`。例如，反向读商品槽位 0，取位置 0、1，得到用户槽位 0、1。正向读用户槽位 2，区间为 `[2,2)`，得到零条边。

| 映射格式 | 首次构建 | 相同快照的下一笔事务 |
|---|---|---|
| `iceberg_poc` | 在事务内存中创建上述两组数组 | 再次需要拓扑时从湖表重建 |
| `iceberg_csr` | 以按主键排序的顶点编号建立原生物理关系 | 复用独立库中的版本。符合条件的订单追加可沿用物理表 |
| `iceberg_stable_csr` | 持久化稳定顶点字典、原生物理关系及快照版本 | 复用已有版本。支持的追加会扩展字典、补边并发布新版本 |

### 2.4 打开 `graph.db.csr.db`，看十一张表和实际的数据页

`NativeStore::open` 调用 lbug 的 `Database(mainPath + ".csr.db", config)` 创建独立数据库。用原生 CLI 直接只读打开它就能检查表、列和页。下面取自第 6.5 节体验目录的实际文件。此时 1,000 万笔基线订单已追加 100 笔，文件为 **357,249,024 字节（340.70 MiB）**，共有 **87,219 个 4 KiB 页**。

**库内对象。** 下表把同一用途的表放在一起。代号 `<tag>` 在这份库中是 `b534a95a402b2721c55b076604cca4630e5fb2dc762e7ea6fbcad3232b81e00f`。它由三张湖表的映射身份、源端点列和目标端点列计算，单次订单追加沿用已有物理关系表。

| 表名 | 当前内容 | 归属页与空间 |
|---|---|---:|
| `StableEdges_<tag>` | 一张从 `CsrSlots` 到 `CsrSlots` 的原生关系表，10,000,100 条逻辑购买边。正向 38,290 页，反向 37,800 页 | 76,090 页，297.23 MiB |
| `CsrSlots` | `pos INT64 PRIMARY KEY`。共 1,000,000 行，取用户数与商品数的较大值。两个方向按端点类型解释槽位。节点数据 526 页，索引 8,359 页 | 8,885 页，34.71 MiB |
| `StableKeyChunks` | `id STRING PRIMARY KEY, data INT64[]`。用户键和商品键按各自版本分块存放，每块至多 4,096 个键。294 块合计 1,200,000 个业务键 | 1,383 页，5.40 MiB |
| `StableNodeHeads`、`StableNodeVersions`、`StableGraphHeads`、`StableGraphVersions`、`StableOrderVersions` | 顶点字典版本与前序版本、当前发布的图版本、三张表的快照号、各表行数及可见边数。两次发布分别记录 10,000,000 和 10,000,100 条可见边 | 77 页，0.30 MiB |
| `Pending_<tag>`、`Missing_<tag>`、`WaitsFor_<tag>` | 保存待补订单、缺失端点和两者间的关系。此次追加的订单两端齐全，三张表均为 0 行 | 0 个表数据或索引页 |

归属空间按原生 `storage_info` 报告的各表数据和索引页范围去重统计。本文件其余 784 页（3.06 MiB）是总页数与上述表级页数的差额。第 0 页的文件头含有 `LBUG` 标记，并给出当前表目录及存储元数据的位置。这个检查点的目录位于第 86,361–86,362 页，存储元数据位于第 87,214–87,218 页。存储元数据将具体方向、节点组及列块定位到文件页。

**与普通原生关系表的对照。** 另建一份普通 lbug 库，定义 `Purchased(FROM User TO Product, oid INT64, amt INT64)`，按默认双向存储写入一笔订单。下面是两份库的 `storage_info` 实际列名，反向列把前缀 `fwd_` 换成 `bwd_`。

| 结构 | 普通原生 `Purchased` | 映射的 `StableEdges_<tag>` |
|---|---|---|
| 物理端点 | 起点 `User`，终点 `Product` | 起点和终点都使用 `CsrSlots`，分别通过用户、商品字典解释 |
| 每个方向的 CSR 头、邻居和原生关系 ID | `fwd_csr_offset`、`fwd_csr_length`、`fwd_NBR_ID`、`fwd__ID` | 同样四列，使用同一套原生 CSR 读写实现 |
| 每条关系携带的属性列 | `fwd_oid`、`fwd_amt` | `fwd_lake_key`（订单号）、`fwd_lake_row`（订单逻辑序号）。金额由固定 Iceberg 快照提供 |
| 版本与追加 | 原生 lbug 写事务更新关系 | 根据 Iceberg 快照组合选择图版本，追加的拓扑交给原生关系写入，再发布新的版本记录 |

**用一笔真实订单串起页、槽位和湖表键。** 订单 `130100003` 的用户 `3501001` 在用户字典的槽位为 `500000`，商品 `503` 在商品字典的槽位为 `0`。同一条物理关系的 `lake_key=130100003`，`lake_row=10000000`，后者从 0 起按订单分配，是这批追加的第一个序号。顺着任一方向读到的都是这笔订单。

| 从哪端查 | 当前检查点读取的 CSR 头 | 枚举邻接时看到的这笔订单 |
|---|---|---|
| 用户槽位 `500000` | 正向节点组 3，组内槽位 106784，邻接起点 1,334,080，有效长度 110 | 邻居是商品槽位 `0`，订单号 `130100003` |
| 商品槽位 `0` | 反向节点组 0，组内槽位 0，邻接起点 0，有效长度 40 | 邻居是用户槽位 `500000`，订单号相同 |

上述邻接起点是节点组的邻接列内位置。另一个数字是列块的物理页号。例如正向第 3 组的 `fwd_NBR_ID` 从文件第 25,365 页开始，占 901 页，`fwd_lake_key` 从第 28,718 页开始，占 1,351 页。两个方向复用原生邻接遍历。回湖取 `amt` 使用 `lake_key` 匹配原始订单，`lake_row` 用于逻辑行定位和历史版本可见性。

手动查看时，在 `native-cli-10m` 目录运行以下命令。如果映射 CLI 正持有这个文件的写锁，先在那条连接的 `lbug>` 输入 `:quit`。

```bash
../../partition-demo/tools/shell/lbug graph.db.csr.db -r -d 256 -b
```

```cypher
CALL show_tables() RETURN name, type ORDER BY name;
CALL table_info('StableEdges_b534a95a402b2721c55b076604cca4630e5fb2dc762e7ea6fbcad3232b81e00f') RETURN *;
CALL storage_info('StableEdges_b534a95a402b2721c55b076604cca4630e5fb2dc762e7ea6fbcad3232b81e00f')
WHERE node_group_id=0 AND column_name IN ['fwd_NBR_ID','fwd_csr_offset','fwd_csr_length']
RETURN column_name, start_page_idx, num_pages, compression ORDER BY column_name;
MATCH (u:CsrSlots {pos:500000})-[e:StableEdges_b534a95a402b2721c55b076604cca4630e5fb2dc762e7ea6fbcad3232b81e00f]->(p:CsrSlots)
WHERE e.lake_key=130100003
RETURN u.pos AS user_slot, p.pos AS product_slot, e.lake_key AS oid, e.lake_row AS ordinal;
```

## 3. 查询怎样与 CSR 交互

以下面的 Cypher 为例。第一条边 `a` 表示 Jo 的订单，第二条边 `b` 表示另一位买家的订单。金额条件应当落在 `b` 上。

```cypher
MATCH (u:User)-[a:Purchased]->(p:Product)<-[b:Purchased]-(v:User)
WHERE u.uid = 12 AND id(v) <> id(u) AND b.amt < 10
RETURN b.oid, v.uid, v.uname, b.amt;
```

| 步骤 | 实际操作 | 样例中传给下一步的值 |
|---|---|---|
| 找起点 | 查稳定字典 `slot(12)` | 用户槽位 `0` |
| 第一跳 | 原生游标 `scanFwd(0)` 读取该用户的正向邻接段 | 商品槽位 `0`，对应 `pid=103`。下一跳直接用槽位，无需读取商品表的名称 |
| 第二跳 | 对商品槽位 `0` 执行 `scanBwd(0)`，同时取邻居和本条边的 `lake_key` | `(用户槽位 0, oid 10001)` 和 `(用户槽位 1, oid 10002)` |
| 排除本人 | 比较起点和邻居的用户槽位，剔除第一项，再查字典 `key(1)` | 一条候选路径，第二跳 `oid=10002`，对方 `uid=42` |
| 取回属性 | 用订单键取金额并过滤，再用剩余的买家键取姓名 | `(10002, 42, Lin, 3)` |

**普通图执行器的接口。** `MappedNode::lookupPK` 可以把业务主键转换为 lbug 的节点 offset。`MappedEdge::scanInternal` 接收上游给的一批已绑定节点及遍历方向。对每个节点，它把槽位交给 `NativeCursor::seek`，后者在独立库的只读事务里调用 `OnDiskGraph::scanFwd` 或 `scanBwd`。底层按方向、node group 和组内槽位找到邻接段，游标分批取出邻居。映射层把邻居包装为 `{offset, 图中邻居的表 ID}`，下一跳可以直接用它做起查点。lbug 也可以从两端扩展再连接。本例关闭批量下推后，小样本明细的实测计划分别扫描 `(u)-[a]->(p)` 和 `(v)-[b]->(p)`，用 lbug 的 `HASH_JOIN_BUILD/PROBE` 按商品内部 ID `p._ID` 合并两个图分支。这里的哈希表由 lbug 建，两次关系扫描仍读 CSR。相同形状的一亿订单计数对照则选择正向接反向的两次连续扫描。

**本例开启键下推时的接口。** 在 Cypher 完成绑定后，`KeyLookupPlanner` 识别此模式，安装 `ICEBERG_CSR_KEY_LOOKUP` 算子。`KeyLookupScan` 自己持有一正一反两个同版本的原生游标，执行上表的遍历，不再经由两个独立的 `MappedEdge` 算子。第二跳游标额外请求 `lake_key`。跨批时游标保留当前位置，因此一个商品的买家很多时，可以接着上批继续读。

## 4. 命中的端点和订单，究竟怎样下推给 Iceberg

本例中的“下推”有一条具体调用链。lbug 映射后端把候选业务键收集成批，`LakeRead::lookup` 生成 SQL 并在**同一查询进程内的 DuckDB** 上执行。DuckDB 的 Iceberg 扩展打开指定表的 metadata JSON，按快照读取底层数据文件。传给 DuckDB 的是业务键 `oid`、`uid`，图内部的槽位只在 lbug 中使用。

### 回湖扫描的线程数

`CALL iceberg_scan_threads=4;` 给当前 lbug 连接设置 DuckDB 每次读湖可用的线程数，默认值为 1。执行 `CALL current_setting('iceberg_scan_threads') RETURN *;` 可查看连接设置。每个事务第一次需要读湖时以该值创建 DuckDB 实例。已经开始读湖的显式事务会继续使用原有实例，提交后同一连接可以切换线程数。设置覆盖 DuckDB 扫描拓扑键和回查属性这两个阶段。两跳算子仍用 CSR 游标找邻居，DataFusion 使用自己的执行器。

2026-11-13 在 M1 Max 上用同一份已建的稳定 CSR 和一亿订单快照测了第二跳金额过滤。起始用户为 `35001001`，条件为 `v.uid <> u.uid AND b.amt < 4900`，返回订单号、买家 ID、姓名和金额。lbug 图执行器固定为 1 个线程。每种配置用两个独立进程各测 6 次，每次在同一连接上开启新的自动提交事务；计时到取完明细，保留操作系统文件缓存。36 次结果都等于同快照的独立 JOIN。

| DuckDB 读湖线程数 | 两轮各自首次查询 | 后续 10 次合并中位数 | 两轮进程峰值 RSS |
|---:|---:|---:|---:|
| 1 | 3.15–3.27 秒 | 2.266 秒 | 349.6 MiB |
| 4 | 1.51–1.53 秒 | 0.665 秒 | 363.2–363.5 MiB |
| 10 | 1.22–1.26 秒 | 0.389 秒 | 375.3–376.1 MiB |

三种配置的计划均为 `ICEBERG_CSR_KEY_LOOKUP`。计时外从 DuckDB 实例核对实际线程数，均与配置一致。三组都回查 511 个订单号，经金额过滤得到 503 笔，再按 503 个用户 ID 取姓名。下面的例子展示业务键如何组成这些回湖查询。

### 4.1 实际发出的两条 SQL

以下取自样例运行日志。为了能转发到其他环境，只把两段本机绝对文件路径换成了占位符。

```sql
SELECT "oid", "amt"
FROM iceberg_scan('<orders 的固定 metadata.json>')
WHERE "oid" IN (10002) AND "amt" < 10;

SELECT "uid", "uname"
FROM iceberg_scan('<users 的固定 metadata.json>')
WHERE "uid" IN (42);
```

1. `KeyLookupScan::nextPaths` 每批最多取一个 lbug 向量，本次构建为 2,048 条候选路径。每项保存第二跳的订单号和另一位买家的业务键。两跳经过的商品只起连接作用。
2. `LakeRead::lookup` 将**本批要查的键**排序去重，构造 `IN (...)`，投影业务键和需要的属性列。第一条 SQL 返回 `(10002,3)`。如果金额为 10 或 NULL，这笔订单会被 DuckDB 过滤，其对应路径也会被剔除。
3. 若要返回姓名，算子只取过滤后仍有路径的买家键，发第二条 SQL，得到 `(42,Lin)`。若改为同条件的 `RETURN count(*)`，只需执行第一条 SQL，本例返回 `1`。若去掉金额条件、只要 `RETURN DISTINCT v.uid,v.uname`，则只发用户 SQL，本例返回 `(42,Lin)`。这里统计的是**索引已可用之后的业务属性查询**，首次建立字典和 CSR 的拓扑读单独计算。
4. DuckDB 返回的行由映射后端按业务键建立查找表。lbug 再按原候选路径逐条取值，无需假定 DuckDB 的输出顺序。去重的是 SQL 的键列表，普通 `RETURN` 和 `count(*)` 保留路径重数。例如 Jo 对同一商品有两笔购买，Lin 有一笔合格购买，图上就有两条合格路径。只有查询要求且优化器能证明语义等价的 `DISTINCT` 才会对结果去重。

### 4.2 哪些查询实际走这条路径

| 查询及计划 | 遍历由谁做 | 请求业务属性时如何读湖 |
|---|---|---|
| 固定买家、按订单号范围查一跳明细，计划含 `ICEBERG_CSR_KEY_LOOKUP (CSR FWD` | lbug 用正向 CSR 枚举该买家的边，按订单号过滤 | DuckDB 按命中键读取返回列。本例为 100 个订单号、1 个用户 ID、100 个商品 ID |
| 本文的有向两跳，`EXPLAIN` 出现 `ICEBERG_CSR_KEY_LOOKUP` | lbug 两个原生游标 | DuckDB 收到分批的 `oid IN (...)` 或 `uid IN (...)`，可同时收到第二跳金额谓词 |
| 同一持久映射关系上的三跳或更长的定长链，端点以主键等值指定，计划含 `ICEBERG_CSR_PATH_LOOKUP` | 每一跳一个原生 CSR 游标，按实际箭头选择正反方向 | 在一个窗口内归集候选键。先查带谓词的属性，再为保留的路径读返回列。小键集用 `IN`，大键集与临时键表做半连接，见第 6.7 节 |
| 普通多跳、关闭键下推，或查询形状不匹配 | lbug 的普通图计划，映射关系通过原生游标或临时邻接数组展开 | 已有按列裁剪。通用属性读取会按业务键排序、加载请求的整列到事务缓存，通常没有候选键 `IN` |
| 编译了 DataFusion 且命中其一跳规则，`EXPLAIN` 出现 `DATAFUSION_ICEBERG_MATCH` | 原生 CSR 提供候选订单键 | Rust Iceberg provider 取湖表列，DataFusion 按订单键连接候选集。这是另一条一跳执行路径 |

一跳明细规则要求有向关系、起点主键等于整型字面量、关系业务键至少有一个等值或范围条件，并且需要返回业务属性。支持两端和边的 `INT64`、`STRING` 属性，以及按已投影字段做原生排序。带 `DISTINCT`、聚合、`SKIP/LIMIT` 或额外谓词时，规划器继续匹配其他规则或执行普通图计划。在通过 `ATTACH` 和 `USE` 重开的图库里，算子按绑定的关系表定位所属图库的 CSR 文件。

两跳规则目前识别同一持久映射关系上的 `(u)→(p)←(v)`。起点以整型主键字面量等值指定，同时显式排除本人。可选谓词为第二条边的一个 `INT64` 属性小于常量。支持带此谓词的 `count(*)`，以及返回第二条边和另一位买家的键、属性，包括某些能由唯一键确定的 `DISTINCT`。两跳纯键投影和带 `ORDER BY/SKIP/LIMIT` 的两跳查询交由其他规则或普通图计划处理。对三跳及以上的固定长度直链，规划器还会匹配下一条路径规则。递归与 `OPTIONAL MATCH` 仍走普通计划。

例如，关闭键下推再运行同一条 Cypher，普通关系扫描要取 `b.amt` 时，如果这两个列尚未缓存，DuckDB 会执行形如下面的 SQL。列裁剪省去了 `uid,pid`，但所有订单的 `amt` 仍会被读取，并按 `oid` 排序。映射层把值缓存在事务里，按逻辑行位置取金额，图执行器再过滤。若按业务键穿插追加导致订单序号不等于当前主键排序位置，通用路径会用 `lake_key` 在已排序的订单键列中寻找正确位置。稳定顶点也会把槽位转换为当前按主键排序的行位置，再读通用属性列。

```sql
SELECT "oid", "amt"
FROM iceberg_scan('<orders 的固定 metadata.json>')
ORDER BY "oid";
```

### 4.3 传了 511 个键，物理上读了多少数据

一亿订单实验使用 `iceberg_csr` 已建好的独立库、1,000 万用户、200 万商品和随机打散的 1 亿订单。独立库约 **3.791 GiB**。基线先生成排序键及四列拓扑的临时 Parquet 分片，再分批做原生 COPY 和 checkpoint，共 **258.98 秒**。这次分批预构建使用单独的工作目录，原始湖表继续服务属性查询。查询触发的整列内存构建在该规模触达实验的 6 GiB 保护线，没有测得完成时间。

| `uid=35001001`，第二跳 `b.amt<4900` | 已归档的实测结果 |
|---|---|
| CSR 找到的候选 | 511 条路径，511 个不同订单号，511 个不同买家 |
| 订单属性查询 | 投影 `oid, amt`，列出 511 个候选订单号，DuckDB 向 lbug 返回 503 行 |
| 金额过滤并计数 | 图查询与三张原表的同快照 JOIN 均为 503。自动提交组后续 5 次中位数 **1.993 秒**，查询进程峰值 RSS **462.7 MiB** |
| 同条件的明细 | DuckDB 再按幸存的 503 个 `uid` 取 `uid, uname`。503 行明细及其重复次数与 JOIN 一致 |
| 对日志 SQL 重放 DuckDB profile | 订单扫描覆盖 **100/100 个文件、1 亿行**。明细中的用户扫描覆盖 **10/10 个文件、1,000 万行** |

因此，这个实验确实让小批键、列投影和金额条件进入 DuckDB，也减少了返回 lbug 的行及 lbug 中的属性物化。至于物理 I/O，当前 CSR 只知道 `oid`，没有保存它对应的 Parquet 文件及行组。文件裁剪的效果取决于数据布局、统计信息和扫描器实际采用的过滤条件，单凭 SQL 中的 `IN` 无法推算。

## 5. 快照变了，现有的两组邻接怎样更新

在 `iceberg_stable_csr` 中，新事务首次需要关系拓扑时检查上述三元组。物理关系表名中的映射指纹由三张源表的身份（表名、业务键、Iceberg 表 UUID、schema ID）和订单的两个端点列名计算。每个图版本再用该指纹和三张表的快照 ID 标识。`StableGraphVersions` 存各版本，`StableGraphHeads` 指向已发布的最新版本。命中已有版本即可打开独立库的只读视图。遇到受支持的追加，新事务会在查询过程中补齐索引后读新版本。这条链路使用查询本身触发，当前 POC 没有运行后台 CDC。

**字典怎样保持编号稳定。** 初建字典是一段排好序的键。追加顶点时，扩展把新键在本批内排序，作为新的一段接在字典尾部。查键时在各段二分，反查时按段的累计长度定位。本例在 `12,42,77` 后追加 `uid=20`，它拿到槽位 3，Lin 仍是槽位 1。在 `103,205` 后追加 `pid=150`，它拿到商品槽位 2。

**订单怎样补成边。** `LakeRead::readAdded` 核验可追溯的 Iceberg v2 append 父子快照，比较父子快照的有效数据文件，读取新增 Parquet 文件中的 `oid,uid,pid`。对新增顶点只读其主键。若订单两端都已存在，`writeEdges` 按两个槽位点查 `CsrSlots`，在同一物理关系表执行预编译的关系 `CREATE`，由原生 lbug 写入正反两向。若端点键已给出但该顶点尚缺，扩展把 `oid,src,dst,ord` 写入等待表，按“端点角色＋缺失业务键”建立反向查找。新顶点到达时仅唤醒等待该键的订单，两端齐全才写边并移出等待表。这个增量路径要求受支持的追加历史，删除、覆盖、重写数据文件需要另外处理。

我们在原图连接上按顺序做了三个真实 Iceberg 提交。每次提交前先用显式读事务读一次图，再提交湖表变更。已固定快照的旧事务保持原结果。结束旧事务后，原连接立即发起新的查询，每一步都与当时三张表的快照 JOIN 对照。

| 湖中刚提交的数据 | 新事务中可见的订单边数 | 图侧实测 |
|---|---:|---|
| 基线两笔订单 | 2 | 物理表有 `(0,0,0,10001)`、`(1,0,1,10002)` |
| 订单 `(10003,20,150,4)` | 2 | 只读 1 个新增订单文件，1 行进入等待表 |
| 用户 `(20,Rui)` | 2 | 本步读取新增订单文件 0 个，商品仍缺，补边 0 条 |
| 商品 `(150,Cup)` | 3 | 本步读取新增订单文件 0 个，唤醒并补边 1 条，物理行是 `(3,2,2,10003)` |

表中统计的是图上全部可见的订单边。新增的第三笔购买了另一件商品，Jo 的同购查询仍只命中 Lin。每个图版本都记录该组合的订单行数、两端顶点数以及可见边数。对复用同一物理关系表的历史版本，原生只读事务和映射游标上的边界检查共同约束可见性，包括 `lake_row < 历史订单数` 和两端槽位在历史字典范围内。较晚补齐的端点不会让老快照凭空多出一条边。

## 6. 如何复现并判断优化是否生效

### 6.1 生成与本文一致的真实湖表

在一个新的工作目录中，用带 `pyiceberg[sql-sqlite]==0.11.1` 和 `pyarrow==23.0.1` 的 Python 运行以下代码。输入故意倒序写入。脚本打印 lbug 的 catalog 设置和 DuckDB 创建三个固定快照视图的语句。

```python
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

root = Path("csr-internals-demo").resolve()
root.mkdir()
db_file = root / "catalog.sqlite"
catalog = SqlCatalog(
    "poc", uri=f"sqlite:///{db_file}", warehouse=(root / "warehouse").as_uri()
)
catalog.create_namespace("ecom")
data = {
    "users": pa.table({"uid": [77, 42, 12], "uname": ["Mei", "Lin", "Jo"]}),
    "products": pa.table({"pid": [205, 103], "pname": ["Book", "Pen"]}),
    "orders": pa.table({
        "oid": [10002, 10001], "uid": [42, 12], "pid": [103, 103], "amt": [3, 2]
    }),
}
print(f"CALL iceberg_poc_catalog='{db_file}';")
for name, rows in data.items():
    table = catalog.create_table(("ecom", name), rows.schema, properties={"format-version": "2"})
    table.append(rows)
    pinned = catalog.load_table(("ecom", name))
    meta = pinned.metadata_location.replace("'", "''")
    snap = pinned.current_snapshot().snapshot_id
    print(f"CREATE TEMP VIEW {name}_s AS SELECT * FROM "
          f"iceberg_scan('{meta}', snapshot_from_id={snap});")
```

### 6.2 对照图查询、扫描日志和独立 JOIN

1. **图端。** 使用内置 Iceberg 映射的 lbug。从设置了 `LBUG_ICEBERG_POC_PROFILE=1` 的环境打开一个新的文件型图数据库，按第 1 节设置脚本打印的 catalog 路径、创建映射。执行第 3 节的 Cypher，预期一行 `10002, 42, Lin, 3`。
2. **计划与运行日志。** 给该 Cypher 加上 `EXPLAIN` 前缀，可看到 `TABLE_FUNCTION_CALL` 中的 `ICEBERG_CSR_KEY_LOOKUP`。执行查询时，日志中的两行 `[iceberg_key_batch]` 分别显示 `table=ecom.orders keys=1 rows=1` 和 `table=ecom.users keys=1 rows=1`，其 SQL 见第 4.1 节。
3. **关闭优化对照。** 在同一连接执行 `CALL iceberg_poc_key_pushdown=false;`，再做 `EXPLAIN` 和查询。样例实测走 `SCAN_REL_TABLE`、`FILTER` 等普通算子，得到相同的一行。用 `CALL iceberg_poc_key_pushdown=true;` 可以再次启用。
4. **湖端。** 在独立 DuckDB 会话里先执行 `LOAD iceberg;`，再执行脚本打印的三条 `CREATE TEMP VIEW` 和下列 SQL。它只依赖三张原表，用五个别名表达同一件商品上的两笔购买。对照基线时使用脚本打印的初始快照。

```sql
SELECT b.oid, v.uid, v.uname, b.amt
FROM users_s u
JOIN orders_s a   ON a.uid = u.uid
JOIN products_s p ON p.pid = a.pid
JOIN orders_s b   ON b.pid = p.pid
JOIN users_s v    ON v.uid = b.uid
WHERE u.uid = 12 AND v.uid <> u.uid AND b.amt < 10;
```

本次小样本还在新主库中把三条映射的 `format` 全部换成 `iceberg_poc` 运行了同一查询。临时模式、持久模式的普通计划、持久模式的键下推计划和原始快照 JOIN，均返回 `(10002,42,Lin,3)`。只有持久模式生成了 `graph.db.csr.db`。

### 6.3 同一组原始 Iceberg 文件，比较旧外表和新映射的执行计划

为了核实旧路径到底发了什么 SQL，我们在本地 DuckDB 注册三个只读视图，分别读取同一组固定快照的 `users`、`products`、`orders`。订单视图名为 `order_links`，把 `uid` 和 `pid` 分别别名为 `src_id` 和 `dst_id`，让原外表优化器识别端点列，湖表数据保持原样。在 lbug 中 `ATTACH` 这个 DuckDB 库为 `g`，用 `CREATE REL TABLE Purchased(FROM g.users TO g.products) WITH (storage='g.order_links')` 注册旧式外表关系。新映射直接使用第 1 节的 DDL。

主对照用 `u.uid=12 AND v.uid<>u.uid AND b.amt<10`，返回 `b.oid,v.uid,v.uname,b.amt`。下面摘录**原外表 EXPLAIN 显示的 SQL**，省略库名、schema 和 SELECT 投影。优化器选择从 `v` 侧起步，仍按相同条件连接两笔购买。

```sql
FROM users v
JOIN order_links b ON v.uid = b.src_id
JOIN products p    ON b.dst_id = p.pid
JOIN order_links a ON p.pid = a.dst_id
JOIN users u       ON a.src_id = u.uid
WHERE b.amt < 10 AND u.uid = 12
```

| 同快照执行 | 计划和实际回湖操作 | 结果 |
|---|---|---|
| 原外表图 | 一个外部表函数执行上述四个 JOIN，`v.uid<>u.uid` 留在 lbug 的 `FILTER` | `(10002,42,Lin,3)` |
| 稳定 CSR，开启键下推 | 一个 `ICEBERG_CSR_KEY_LOOKUP` 执行正反向遍历。DuckDB 先收到 `orders WHERE oid IN (10002) AND amt<10`，再收到 `users WHERE uid IN (42)` | `(10002,42,Lin,3)` |
| 稳定 CSR，关闭键下推 | lbug 自己的关系扫描和哈希连接按 `p._ID` 组合路径，属性按列回湖 | `(10002,42,Lin,3)` |
| 独立 SQL 对照 | 直接执行第 6.2 节的三张原表 JOIN | `(10002,42,Lin,3)` |

旧外表实测使用当前 lbug 的优化器、DuckDB 1.5.5 和兼容该版本的外表连接器（扩展仓库 `14af298`，在临时构建副本中适配了一个查询结果返回类型）。额外把排除本人条件换为 `id(v)<>id(u)` 后，该旧连接器组合还返回了 Jo 自己的订单 `10001`。两个 CSR 计划使用此写法均只返回 Lin。本节用业务主键对照的结果一致。这个额外差异尚未在 DuckDB 2.0 连接器上复核。

#### 一亿订单上，旧 JOIN 和已建 CSR 使用相同的 DuckDB 线程数

2026-11-13 在 M1 Max、32 GiB 内存和本地 Iceberg 文件上交错复测了这两条路径。数据含 1 亿笔订单、1,000 万个用户、200 万个商品，订单快照 ID 为 `8560108283879943351`。从用户 `35001001` 开始，查购买同一件商品的其他买家，过滤对方订单 `b.amt < 4900`，取回订单号、买家 ID、姓名及金额。72 次计时查询均返回 503 行，逐行等于独立执行的同快照 SQL JOIN。

| DuckDB 线程 | 旧 JOIN 重复查询中位数 | CSR 重复查询中位数 | 加速比 | 旧 JOIN 峰值 RSS | CSR 峰值 RSS |
|---:|---:|---:|---:|---:|---:|
| 1 | 6.532 秒 | 2.298 秒 | 2.84 倍 | 1,137 MiB | 350 MiB |
| 4 | 1.729 秒 | 0.653 秒 | 2.65 倍 | 978 MiB | 363 MiB |
| 10 | 0.904 秒 | 0.366 秒 | 2.47 倍 | 1,024 MiB | 377 MiB |

两条路径使用同一版本的 lbug 和 DuckDB 1.5.5，lbug 执行线程固定为 1。旧外表通过三个 DuckDB 视图读固定的湖表快照，一条 SQL 执行四个 JOIN。CSR 用原生正反向游标得到 511 个第二跳订单号，按金额筛到 503 笔，再按买家 ID 读取姓名。每种配置用两个新进程各跑六次，第二轮反转执行顺序。每个进程复用连接，每条查询独立开启事务并取完明细。表中中位数合并两轮各自后五次，RSS 取两轮进程高水位的最大值。外表挂载和首次建 CSR 都在计时前完成，操作系统文件缓存保留。

新进程的第一条查询单独统计。1 线程时，旧 JOIN 为 6.63–6.69 秒，CSR 为 3.11–3.29 秒。4 线程时分别为 1.82–1.86 秒和 1.49–1.65 秒。10 线程时旧 JOIN 首查更快，两轮为 0.96–1.17 秒，CSR 为 1.22–1.33 秒。

#### 同一亿笔订单，10 线程的三跳和四跳明细（优化前基线）

2026-11-13 使用与上面两跳测试相同的三个快照，起始用户仍为 `uid=35001001`。第一跳覆盖他所有的 10 笔订单。第二跳从共同商品找到其他买家，第三跳找他们买过的其他商品，第四跳再找这些商品的其他买家。只在当前查询的最后一跳过滤订单金额 `amt < 4900`。三跳取末笔订单号、商品号、商品名和金额，四跳取末笔订单号、买家号、姓名和金额。两条查询均保留重复路径，取完全部明细。

```cypher
MATCH (q:Product)<-[c:Purchased]-(v:User)-[b:Purchased]->(p:Product)<-[a:Purchased]-(u:User)
WHERE u.uid = 35001001 AND v.uid <> u.uid AND q.pid <> p.pid AND c.amt < 4900
RETURN c.oid, q.pid, q.pname, c.amt;

MATCH (u:User)-[a:Purchased]->(p:Product)<-[b:Purchased]-(v:User)
      -[c:Purchased]->(q:Product)<-[d:Purchased]-(w:User)
WHERE u.uid = 35001001 AND v.uid <> u.uid AND q.pid <> p.pid
  AND w.uid <> v.uid AND d.amt < 4900
RETURN d.oid, w.uid, w.uname, d.amt;
```

旧外表使用相同 Cypher，将 `User`、`Product` 换成 `g.users`、`g.products`。三跳若从 `u` 一端书写，旧优化器在这组表上生成 `u._ID`，DuckDB 绑定时报列不存在。上面从 `q` 一端书写的等价形式保留每条箭头的方向及起始用户条件，两条路径都使用了这个形式。实测旧外表计划分别下推一条含 6 个和 8 个 JOIN 的 SQL，两份 CSR 计划分别出现 3 跳和 4 跳的 `ICEBERG_CSR_PATH_LOOKUP`。

| 跳数 | 执行路径 | 首次耗时 | 后续 5 次中位数 | 进程峰值 RSS | 返回行数 |
|---:|---|---:|---:|---:|---:|
| 3 | 旧外表 JOIN | 4.45 秒 | 4.38 秒 | 11,009.9 MiB | 4,476 |
| 3 | 已建 CSR | 2.46 秒 | 1.05 秒 | 419.6 MiB | 4,476 |
| 4 | 旧外表 JOIN | 8.52 秒 | 8.00 秒 | 9,792.0 MiB | 224,056 |
| 4 | 已建 CSR | 44.50 秒 | 42.35 秒 | 853.5 MiB | 224,056 |

M1 Max、32 GiB 内存、本地 Iceberg 文件、DuckDB 1.5.5。四组均从 lbug 发起，lbug 图执行线程为 1，DuckDB 线程为 10，每组一个新进程，在同一连接上连续运行 6 个自动提交事务。计时含规划、执行、Python C API 取回全部明细；峰值 RSS 取 macOS 进程高水位。CSR 已建好，旧外表已挂载，两项初始化均在计时前完成，操作系统文件缓存保留。独立 DuckDB 会话在固定快照上用业务主键逐跳 JOIN，核对每次返回的四列及重复次数。24 次均一致。四跳 224,056 行中有 223,721 个不同的四列组合，额外 335 行均能在对照中找到。

三跳枚举 4,599 条候选路径，分 3 批查订单金额并分 3 批查商品名。四跳枚举 229,213 条候选路径，每批最多 2,048 条，共发出 112 次订单查询和 112 次用户查询。计时外重放第 1、56 和 112 批的真实 SQL，每次订单查询均读了 100/100 个订单文件，用户查询均读了 10/10 个用户文件。四跳仅取回明细并关闭结果的中位耗时，两边均约 4 秒；引擎报告的执行时间中位数则是旧 JOIN 3.94 秒、CSR 38.26 秒。

#### 一亿笔订单复测，合并 CSR 命中的属性键（移植到上游 main 前）

使用相同的两条 Cypher、同一组三表快照及相同的线程设置。订单快照为 `8560108283879943351`。每组从新进程开始，用同一连接的 6 个自动提交事务分别取完明细。下表的“湖表属性查询”统计每次 MATCH 发给 DuckDB、读取某一张 Iceberg 表的 SQL 次数。旧外表的 SQL 是完整的多表 JOIN。

| 跳数 | 执行方式 | 湖表属性查询或外表 SQL | 首次 | 后续 5 次中位 | 峰值 RSS |
|---:|---|---|---:|---:|---:|
| 3 | 优化前 CSR（上轮基线） | 3 次订单 + 3 次商品 | 2.46 秒 | 1.05 秒 | 419.6 MiB |
| 3 | 本轮 CSR | 1 次订单 + 1 次商品 | 2.03 秒 | 0.48 秒 | 426.6 MiB |
| 3 | 本轮旧外表 JOIN | 1 条含 6 个 JOIN 的 SQL | 3.92 秒 | 3.90 秒 | 10,925.7 MiB |
| 4 | 优化前 CSR（上轮基线） | 112 次订单 + 112 次用户 | 44.50 秒 | 42.35 秒 | 853.5 MiB |
| 4 | 本轮 CSR | 1 次订单 + 1 次用户 | 8.55 秒 | 4.89 秒 | 936.8 MiB |
| 4 | 本轮旧外表 JOIN | 1 条含 8 个 JOIN 的 SQL | 7.90 秒 | 7.69 秒 | 9,620.4 MiB |

四跳的 CSR 游标给出 229,213 条候选路径。先用 228,871 个不同的订单号查 `oid, amt` 并过滤 `amt < 4900`，命中 223,721 笔不同的订单。再用幸存路径上的 221,506 个不同用户号查 `uid, uname`。将金额和姓名按键填回，最终得到 224,056 行，含 335 行重复明细。三跳的 4,476 行、四跳的 224,056 行均已与重新执行的独立同快照 JOIN 按四列值和出现次数核对，本轮 24 次计时结果全部一致。

四跳后续 5 次的引擎执行中位数从上轮的 38.26 秒降至 0.69 秒。此次 Python C API 读取并关闭全部结果的中位数为 4.13 秒。复用现有 CSR、保留系统文件缓存，计时包含取回结果。窗口内暂存候选键和命中属性，本轮四跳进程峰值比上轮高 83.3 MiB。

#### 三跳首次查询多花的时间

上表中 CSR 首次取完 4,476 行用时 2.03 秒。原始计时含编译 6.6 ms、引擎执行 1,951.4 ms、客户端取完并关闭结果 75.4 ms。在相同快照上给读取器和游标分别计时，一轮新进程复测得到以下结果。

| 三跳查询阶段 | 首次 | 同一连接后 5 次中位数 |
|---|---:|---:|
| 打开独立 CSR 库 | 105 ms | 复用已打开的库 |
| 从 `StableKeyChunks` 加载用户和商品字典 | 865 ms | 命中内存缓存 |
| 沿 CSR 枚举候选路径 | 347 ms | 2 ms |
| DuckDB 按订单号查金额 | 551 ms | 337 ms |
| DuckDB 按商品号查商品名 | 16 ms | 13 ms |
| 客户端取完结果 | 77 ms | 80 ms |
| 端到端，含读事务初始化和编译等 | 2.02 秒 | 0.48 秒 |

三个新进程均命中已发布的关系版本。当前实现即使只查一个用户，也会先从独立库装入 1,000 万个用户键（2,442 块）和 200 万个商品键（489 块），三个进程单是加载字典就用了 815–865 ms。另两轮的首次总耗时为 1.31、1.32 秒，首次打开已有数据页和读湖所需时间会波动。单独用不存在的用户作起点，首次返回空集也花了 1.10 秒，紧接着用原用户取完明细为 0.61 秒。同一图库内新开的第二条连接也命中字典缓存，三个进程均在 0.45–0.47 秒内取完相同的 4,476 行。

#### 在上游 main 基线重新编译后的三组对照

2026-11-13 从上游 `main` 的 `71f38adf2` 建分支后重新编译内核和测试用外表连接器。两边实测 DuckDB 都开 10 个线程，lbug 开 1 个线程。每种跳数、每条执行路径用一个新进程，每次完整读取四列明细。首次从新进程提交第一条 MATCH 到客户端取完结果，随后是同连接的五个新事务。索引已建好，保留操作系统文件缓存。本次每个配置都执行一轮六次。为展示首次启动和后续波动，表中保留五个后续样本。

| 跳数 | 执行方式 | 首次（秒） | 后续 5 次（秒，按执行顺序） | 后续中位（秒） | 峰值 RSS（MiB） | 明细行数 |
|---:|---|---:|---|---:|---:|---:|
| 2 | 旧外表 JOIN | 1.03 | 1.12、1.31、1.12、1.11、1.01 | 1.12 | 1,015.6 | 503 |
| 2 | 已建 CSR | 2.26 | 0.36、0.42、0.42、0.40、0.38 | 0.40 | 371.9 | 503 |
| 3 | 旧外表 JOIN | 4.28 | 4.21、4.70、3.98、3.90、3.68 | 3.98 | 10,900.2 | 4,476 |
| 3 | 已建 CSR | 1.83 | 0.69、0.48、0.79、0.46、0.50 | 0.50 | 422.9 | 4,476 |
| 4 | 旧外表 JOIN | 11.33 | 8.86、9.31、8.20、8.09、8.22 | 8.22 | 9,574.5 | 224,056 |
| 4 | 已建 CSR | 7.36 | 4.71、4.70、4.82、5.12、4.96 | 4.82 | 926.7 | 224,056 |

旧外表的三个实际执行计划各包含一条 SQL，分别有 4、6、8 个 JOIN。CSR 对应两跳的 `ICEBERG_CSR_KEY_LOOKUP` 和三、四跳的 `ICEBERG_CSR_PATH_LOOKUP`。分别核实挂载视图及映射 catalog 指向同一组三张湖表快照，再用业务主键在独立 DuckDB 中重新 JOIN。六组共 36 次计时查询均核对了四列值和重复次数，三跳得到 4,476 行，四跳得到 224,056 行，含 335 行重复明细。

### 6.4 用两个终端手动体验旧事务、新顶点和迟到的边

下面使用仓库中的 `examples/iceberg-query-csr/manual_demo.py`。它用 lbug Python API 保持一个真实的图连接，用 PyIceberg 向湖表提交数据。两个终端都从仓库根目录执行 `cd examples/iceberg-query-csr`。这个工作区已有配套的 lbug 库和扩展，脚本通过 `uv` 启动。默认数据目录是仓库下的 `build/iceberg-csr-poc/manual-demo`。

**① 终端 A 打开图，固定旧快照。** 在新的体验目录中，`init` 会创建 3 个用户、2 个商品、2 笔订单，并把同一张 `orders` 表同时映射为顶点 `OrderRow` 和关系 `Purchased`。已准备过的目录直接执行 `graph` 即可。

```bash
./manual_demo.py init
./manual_demo.py graph
```

在 `lbug>` 下逐行输入。第一条关系查询先建好旧版本的 CSR，得到 2。随后显式事务中的用户数为 3。让这个终端继续开着。

```cypher
MATCH ()-[r:Purchased]->() RETURN count(*);
BEGIN TRANSACTION READ ONLY;
MATCH (u:User) RETURN count(*);
```

**② 终端 B 向 Iceberg 追加数据。** 此命令提交 100 个新用户和两笔订单。两笔都属于新用户 Rui（`uid=20`）。订单 `10003` 买的是已有商品 103，订单 `10004` 买的是尚未登记的商品 150。

```bash
./manual_demo.py append
```

**③ 回终端 A，观察同一个连接。** 前后两次计数分别是 **3** 和 **103**。第二次只需更新顶点字典。查询 `OrderRow` 得到 **10003、10004** 两行。最后查询 `Purchased` 才追上新快照的关系索引，此时只有 **10003** 能连成边。

```cypher
MATCH (u:User) RETURN count(*);
COMMIT;
MATCH (u:User) RETURN count(*);
MATCH (u:User) WHERE u.uid=20 RETURN u.uid,u.uname;
MATCH (o:OrderRow) WHERE o.oid>=10003 RETURN o.oid ORDER BY o.oid;
MATCH ()-[r:Purchased]->() WHERE r.oid>=10003 RETURN r.oid ORDER BY r.oid;
```

**④ 终端 B 补商品，再到 A 查同一条关系。**

```bash
./manual_demo.py product
./manual_demo.py join
```

`product` 提交 `(150,Cup)`。`join` 用独立 DuckDB 扫描三个固定的 Iceberg 快照，打印所有两端齐全的订单，包括 10003、10004。回到 A，重复下面这条查询，图也返回 **10003、10004**。本步的索引日志含 `promoted=1`，表示一笔等待端点的订单刚刚补成边。

```cypher
MATCH ()-[r:Purchased]->() WHERE r.oid>=10003 RETURN r.oid ORDER BY r.oid;
```

还可以在 A 运行第 3 节的两跳查询。此时 Jo 的同购结果有两行，分别是 `(10002,42,Lin,3)`、`(10003,20,Rui,4)`。输入 `:quit` 退出。`graph` 可重开原图。要从初始状态再演示一次，给两个终端的所有命令加相同的 `--dir 新目录`，先执行 `init`。对同一目录重复执行 `append` 或 `product` 会识别已提交的批次。

### 6.5 用原生 lbug CLI 实测 1,000 万笔订单首建和追加 100 笔

本机已准备独立的 Iceberg 数据目录 `build/iceberg-csr-poc/native-cli-10m/lake`，包含 **100 万个用户、20 万个商品、1,000 万笔订单**。订单分布在 10 个 Parquet 文件中，每文件 100 万行。首次操作从空图库开始，使用 `iceberg_stable_csr`。终端 A 的每一条图命令都由原生 lbug 0.23.0 shell 执行。终端 B 使用 PyIceberg 提交湖表变更。

**① 首次体验时，终端 A 打开新的文件型图库。** 如果这个 `graph.db` 已创建过映射表，请直接使用本节末尾的“退出后重新打开已有图库”命令。本机 Homebrew 的 `lbug` 是 0.15.1。下列路径选中内置映射的原生 0.23.0 二进制，并打开查询阶段日志。后面示例中的相对路径都从这个工作目录解析。

```bash
cd "$(git rev-parse --show-toplevel)/build/iceberg-csr-poc/native-cli-10m"
LBUG_ICEBERG_POC_PROFILE=1 ../../partition-demo/tools/shell/lbug graph.db -d 256 -b
```

**② 在 `lbug>` 指定 catalog、创建映射。** `:stats on` 是 shell 命令，单独输入一行。两处 `FROM/TO` 分别指定图里的顶点类型和订单表里的外键列。三条 DDL 完成后，图里已经有映射定义。这次实测在此时还没有生成独立 CSR 文件。

```text
:stats on
```

```cypher
CALL iceberg_poc_catalog='lake/catalog.sqlite';

CREATE NODE TABLE User(uid INT64 PRIMARY KEY, uname STRING)
WITH (storage='ecom.users', format='iceberg_stable_csr');

CREATE NODE TABLE Product(pid INT64 PRIMARY KEY, pname STRING)
WITH (storage='ecom.products', format='iceberg_stable_csr');

CREATE REL TABLE Purchased(FROM User TO Product, oid INT64, amt INT64)
WITH (storage='ecom.orders KEY oid FROM uid TO pid', format='iceberg_stable_csr');
```

**③ 在 A 连续执行两次同一条 Cypher。** 把它作为建表后的第一条图查询，这样首次耗时包含顶点字典和关系索引的建立。第一次会看到 `built baseline edges=10000000` 和 `graph published ms=...`，第二次直接复用同快照索引。两次都返回 `10000000`。保持 A 打开，使用默认自动提交。

```cypher
MATCH ()-[r:Purchased]->() RETURN count(*);
```

**④ 终端 B 向原始 Iceberg 订单表提交 100 行。** 这 100 笔都属于已有用户 `3501001`，分别指向 100 个已存在的商品。命令会打印快照 ID、首尾订单号及 Iceberg 追加耗时。每执行一次都会再追加 100 个新的订单号。

```bash
cd "$(git rev-parse --show-toplevel)/build/iceberg-csr-poc/native-cli-10m"
../../../examples/iceberg-query-csr/native_cli_lake.py append --dir "$PWD"
```

**⑤ 回 A，原连接再执行一次计数。** 此次查询按需读新增订单文件，向已有双向 CSR 补边并发布新版本。

```cypher
MATCH ()-[r:Purchased]->() RETURN count(*);
```

第一次追加后应返回 `10000100`。日志 `order_files=1 order_rows=100 queued=0 promoted=0 edges=10000100` 表明只读取了一个新增订单文件中的 100 行，全部当场连成边。下面这条查询从该用户沿购买关系返回新订单的计数和首尾订单号。

```cypher
MATCH (u:User)-[r:Purchased]->(:Product)
WHERE u.uid=3501001 AND r.oid>=130100003
RETURN count(*) AS n, min(r.oid) AS first_oid, max(r.oid) AS last_oid;
```

首次追加的预期结果为 `100 | 130100003 | 130101290`。在 B 执行以下命令，可独立用 DuckDB 对当前三张 Iceberg 表的固定快照做 JOIN，预期为 `joined_edges=10000100`、`appended_edges=100`、`max_oid=130101290`。

```bash
../../../examples/iceberg-query-csr/native_cli_lake.py check --dir "$PWD"
```

**本机 2026-11-12 单轮参考。** 使用另一份同规模、同随机种子的湖表，从空图库运行上述原生 CLI 命令。计时窗口是发出这次查询到 CLI 打印完整结果。操作系统文件缓存未清空。

| 关系计数查询 | 查询墙钟 | 关系索引构建并发布 `graph published ms` | 返回边数 |
|---|---:|---:|---:|
| 空图库首建 | 19.24 秒 | 17.75 秒 | 10,000,000 |
| 同快照再次查询 | 43.2 毫秒 | 复用 | 10,000,000 |
| 提交 100 笔后的首次查询 | 77.2 毫秒 | 28.6 毫秒 | 10,000,100 |
| 新快照再次查询 | 31.4 毫秒 | 复用 | 10,000,100 |

原生 CLI 显示的查询总耗时要把 `compiling` 和 `executing` 相加。这次首建分别是 18,457.44 毫秒和 783.75 毫秒。规划期间估计映射关系的行数就会触发构建。单独的 `graph published ms` 从关系版本开始构建计到其提交，顶点字典更早开始准备。湖端 100 行的写入与提交另耗时约 1.16 秒。自动衔接两个终端时，从 Iceberg 提交返回到同一 CLI 打印新计数共 78.4 毫秒。关闭后独立库 `graph.db.csr.db` 大小为 340.63 MiB。

要再测一轮增量，在 B 再执行一次 `append`，随后在 A 计数。要重测相同规模的**首建**，可在仓库根目录执行下列命令，得到一份独立的初始湖表。两个终端之后都进入它打印的新目录，按上面的相对路径操作。

```bash
demo_dir=$(mktemp -d "$PWD/build/iceberg-csr-poc/native-cli-10m-repeat-XXXXXXXX")
./examples/iceberg-query-csr/native_cli_lake.py init --dir "$demo_dir"
echo "$demo_dir"
```

**退出后重新打开已有图库。** 在体验目录直接打开原来的 `graph.db`，通过启动脚本设置本次连接的 catalog。

```bash
cd "$(git rev-parse --show-toplevel)/build/iceberg-csr-poc/native-cli-10m"
LBUG_ICEBERG_POC_PROFILE=1 ../../partition-demo/tools/shell/lbug graph.db -d 256 -b -i ../../../examples/iceberg-query-csr/reopen.cypher
```

内核在 catalog 恢复前注册映射格式。`reopen.cypher` 只设置本次连接的 Iceberg catalog，也可直接在提示符输入：

```cypher
CALL iceberg_poc_catalog='lake/catalog.sqlite';
```

在 `lbug>` 中检查订单总数。首次追加 100 笔并正常退出后的实测结果为 `10000100`，查询复用了 `graph.db.csr.db` 中对应快照的 CSR。

```cypher
MATCH ()-[r:Purchased]->() RETURN count(*);
```

### 6.6 验证一跳订单明细的按键下推

在第 6.5 节追加后重开的原生 shell 中执行以下查询。加上 `EXPLAIN` 前缀，可以看到 `ICEBERG_CSR_KEY_LOOKUP (CSR FWD, order key range, DuckDB candidate keys IN)` 及其上方的原生排序算子。切换到新的内核二进制时，按第 6.5 节直接打开原库。

```cypher
MATCH (u:User)-[r:Purchased]->(p:Product)
WHERE u.uid = 3501001 AND r.oid >= 130100003 AND r.oid <= 130101290
RETURN r.oid AS oid, u.uid AS uid, p.pid AS pid, r.amt AS amt,
       u.uname AS uname, p.pname AS pname
ORDER BY oid;
```

1. 稳定字典将用户主键转换为槽位。正向 CSR 游标枚举该用户的购买关系，从每条边读取订单号，用查询中的上下界筛出 100 笔。商品槽位通过商品字典还原为业务键。
2. 映射后端将三个集合分别排序去重，只投影所需的键和属性。开启 `LBUG_ICEBERG_POC_PROFILE=1` 时，三条 `[iceberg_key_batch]` 日志的 `keys` 和 `rows` 均为下表中的数量。

| 湖表 | 读取的列 | 实际下发的键 |
|---|---|---|
| `ecom.orders` | `oid, amt` | `oid IN (...)`，100 个订单号 |
| `ecom.users` | `uid, uname` | `uid IN (3501001)`，1 个买家 |
| `ecom.products` | `pid, pname` | `pid IN (...)`，100 个商品号 |

3. lbug 按键把属性填回每一条购买关系，再按 `oid` 排序。以第 6.5 节体验库的目录快照复测，订单快照为 `8733611364969515752`，用户快照为 `6188207626075984392`，商品快照为 `6752820336901322467`。两种模式的 100 行、6 列及排序均与这三个快照的独立 SQL JOIN 一致。对下发的订单 SQL 做 DuckDB 单线程 `EXPLAIN ANALYZE`，`Total Files Read` 为 1，快照共有 11 个订单数据文件。这里的 100 表示返回行数。文件读取量随实际的数据布局变化。

分别以新原生 CLI 进程执行 `CALL iceberg_poc_key_pushdown=false;` 和 `CALL iceberg_poc_key_pushdown=true;`，每个进程用默认自动提交连续查询 6 次。均使用同一份已建立的 CSR、固定 Iceberg catalog、256 MiB 主库缓冲池和操作系统现有文件缓存。查询时间是 CLI 的编译与执行时间之和，RSS 是该 CLI 进程全程的峰值。

| 一跳订单明细 | 首次查询 | 后续 5 次中位 | 进程峰值 RSS |
|---|---:|---:|---:|
| 关闭键下推，按列读取后回填 | 3,023.70 ms | 2,036.97 ms | 3,475.6 MiB |
| 开启键下推，按命中键回填 | 150.35 ms | 39.32 ms | 139.0 MiB |

### 6.7 从一笔新增订单出发，验证三跳和四跳按键取属性

这次选用第 6.6 节的同一组三张快照，订单数为 10,000,100。第一笔新增订单 `130100003` 属于买家 `3501001`，指向商品 `503`。第三跳询问买过商品 503 的其他人还买过哪些别的商品，并要求第三笔订单金额小于 500。第四跳从这些商品继续找其他购买者，要求第四笔订单金额小于 50。两个问题各自以一笔新增订单为起点。

```cypher
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
```

下面的命令会启动新的 lbug 进程。若第 6.5 节的交互式 CLI 仍开着，先回到那个终端，在 `lbug>` 输入 `:quit`，等它回到系统 shell 提示符、释放图库文件锁后，再在 `native-cli-10m` 目录执行。新进程直接打开原图库，两条查询在同一连接上各用一个新事务。

```bash
LBUG_ICEBERG_POC_PROFILE=1 ../../partition-demo/tools/shell/lbug graph.db -d 256 -b \
  -i ../../../examples/iceberg-query-csr/reopen.cypher \
  < ../../../examples/iceberg-query-csr/native_cli_multihop.cypher
```

如果第一条报错为 `Could not set lock on file : graph.db (Lock is held by PID ...)`，可在该目录运行 `lsof -nP graph.db graph.db.csr.db` 找到占用进程，再用 `ps -p <PID> -o tty=,command=` 定位原终端。释放锁后重新运行同一条命令即可复用已有映射和 CSR。

**执行过程。** `EXPLAIN` 对这两条查询分别显示 `ICEBERG_CSR_PATH_LOOKUP (3 hops, ...)` 和 `(4 hops, ...)`。映射后端从有主键条件的链端点建立游标。正向从用户找商品，反向从商品找购买者，订单号和顶点键的比较在相关键都可用时执行。每找到一条完整候选路径，仅留下属性过滤或输出所需变量的业务键，同时保留路径出现的次数。

内核按 8 MiB 的路径键和行号预算计算一个读取窗口。按本机每个键及行号各占 8 字节计算，仅取末笔订单和末端用户时，一个窗口最多容纳 262,144 条路径。先将窗口内的订单号排序去重，在固定订单快照上查询金额并应用谓词，再用保留下来的路径查需要的用户或商品属性。不同键不超过 2,048 个时用 `IN (...)`，超过时用 DuckDB Appender 将键写入读取器私有的临时表，和一张湖表做 `SEMI JOIN`。相同键对应的每条路径都得到同一属性值。输出仍按每批最多 2,048 行交给 lbug，消费完整个窗口后才继续遍历。简单无排序、无聚合的 `LIMIT` 保留小窗口以便及早结束。8 MiB 只用于计算路径键及行号容量，命中属性、DuckDB 及结果集还会额外占内存。

| 合并窗口前的原生 CLI 明细查询 | 进入金额过滤的路径 | 返回路径 | 首查 | 后续 5 次中位 | 进程峰值 RSS |
|---|---:|---:|---:|---:|---:|
| 三跳，关闭键下推 | 351 | 26 | 4,394 ms | 3,064 ms | 3,531 MiB |
| 三跳，开启键下推 | 351 | 26 | 461 ms | 238 ms | 148 MiB |
| 四跳，关闭键下推 | 17,655 | 147 | 2,816 ms | 2,494 ms | 3,791 MiB |
| 四跳，开启键下推 | 17,655 | 147 | 2,305 ms | 2,171 ms | 181 MiB |

每行使用一个新原生 CLI 进程，以 256 MiB 主库缓冲池挂载已有图库，每次查询启动新的自动提交事务。计时包含 CLI 的编译和执行，操作系统文件缓存沿用，RSS 每 20 ms 采样。两种模式的全部有序明细和单独的 `count(*)` 均与固定快照 JOIN 一致。对照 JOIN 每一步都以 `uid` 或 `pid` 显式连接下一张表，保留每笔订单的键和重复路径，金额条件只作用于对应一跳。

三跳用一批 351 个订单号过滤出 26 笔，再取 26 个商品名。四跳分 9 批发送 17,655 个订单号，过滤出 147 条路径，再批量取姓名、商品名。在计时外重放各自的第一批订单 SQL，DuckDB 的 `Total Files Read` 均为 10，固定快照共有 11 个订单数据文件。这里后续买家的订单号分散在历史文件里，和第 6.6 节只查一批新订单的分布不同。

**适用范围。** 当前路径规则要求单个普通 `MATCH`、同一张持久映射关系表、由单向箭头组成的固定长度直链，且链的一端用 `INT64` 主键字面量等值指定。可返回中间或末端顶点、边的键及 `INT64`、`STRING` 属性，也可做 `count(*)`。支持业务键比较和同一表内部 ID 的等值、不等值比较。每个变量可带一个整型业务属性小于常量的过滤条件。中间跳的业务属性也在完整路径批次形成后按键取值和过滤。纯拓扑、分叉、多关系表混用和可变长度模式会继续走普通图计划。小数据回归覆盖从商品反向起步、重复路径、NULL 金额、跨批、旧事务与新增订单的可见性。

### 6.8 命中 5 万或 30 万个订单号时怎样取金额

早期实现每批最多取 2,048 条候选路径，每批生成一条 `IN (...)` 属性查询。对一个只取第四跳订单金额的操作，若每条路径的订单号都不同，5 万条路径需要 25 次订单扫描，30 万条需要 147 次。以下独立读湖实验比较了这一基线与临时键表。

我们用同一订单快照 `8733611364969515752`（10,000,100 笔订单）比较两种取金额的方法。从买家 `3501001` 的 100 笔新增订单出发，按第 6.7 节的四跳路径和端点排除条件，在 lbug 返回 `DISTINCT d.oid LIMIT 300000`。将这一次得到的 30 万个不同订单号保存，前 5 万个作为小样本。两个方案都使用这份键，读取 `oid, amt`，要求 `amt < 50`。

| 不同订单号 | 分批 `IN` 的湖表查询次数 | 分批 `IN` 用时 | 临时键表用时（1 次湖表查询） | 命中订单数 |
|---:|---:|---:|---:|---:|
| 50,000 | 25 | 5.12 秒 | 0.214 秒 | 398 |
| 300,000 | 147 | 30.77 秒 | 0.217 秒 | 2,369 |

在计时外对 30 万键的第一批、第二批和最后一批分别执行 `EXPLAIN ANALYZE`，每一批的订单扫描都读取了 11 个文件中的 10 个。一次半连接的订单扫描读取了 11 个文件。

计时从拿到候选键开始，到读完所有匹配的订单号和金额结束。两种方案都使用与映射后端相同的 DuckDB C++ 库、单线程、固定快照和已有操作系统缓存，各用新进程交错跑 3 次，表中为中位数。分批方案包含组装每条 SQL、执行和取结果。键表方案包含创建 `TEMP TABLE`、用 DuckDB Appender 写入 `BIGINT` 键、执行一次半连接、取结果并丢弃临时表。两种方案的全部 `(oid, amt)` 均与独立扫描同快照后用内存键集合筛选的结果逐行一致。30 万键时，两个独立读湖进程的最高 RSS 分别为 62.0 MiB 和 68.1 MiB。图遍历和路径结果回填属于这个计时窗口之外的工作。

```sql
CREATE TEMP TABLE csr_candidates(oid BIGINT);
-- 用 DuckDB Appender 批量写入从 CSR 取得的整数键。
SELECT o.oid, o.amt
FROM iceberg_scan('<本事务固定的订单元数据文件>') AS o
SEMI JOIN csr_candidates AS k ON k.oid = o.oid
WHERE o.amt < 50;
```

上表的数字只统计独立读湖实验。临时键表现已接入三跳及以上的 `ICEBERG_CSR_PATH_LOOKUP`。实际执行中使用 `temp.main.csr_lookup_keys(candidate_key BIGINT)`，每次大键集查询都会替换其内容，其生命周期受读事务的 DuckDB 读取器约束。超过窗口容量时分窗口执行，两次窗口可能查到同一键。第 6.3 节的一亿笔订单复测覆盖从原生 CSR 遍历到客户端取完明细的整条路径。

## 7. 源码入口

路径相对于 Ladybug 仓库根目录。函数名对应上面各步，可直接在仓库内检索。

| 要跟哪一步 | 文件和入口 |
|---|---|
| 原外表 MATCH 如何变成多表 SQL | `src/optimizer/foreign_join_push_down_optimizer.cpp` 中的 `ForeignJoinPushDownOptimizer::visitHashJoinReplace`、`finalizeChain`、`buildChainJoinQuery`，以及 `extension/duckdb/src/function/duckdb_scan.cpp` 的 `DuckDBScanFunction::initSharedState` |
| 图映射与原外表从何处分流 | `src/binder/bind/bind_ddl.cpp` 的 `Binder::bindCreateRelTableGroupInfo`，以及 `src/storage/iceberg/mapped_table.cpp` 为三个映射格式注册的 `ColumnarTableHookRegistry` 钩子 |
| 映射登记、业务键与图 ID、普通图扫描 | `src/storage/iceberg/mapped_table.cpp` 中的 `registerIcebergMapping`、`MappedNode::lookupPK`、`MappedEdge::scanInternal`、`project` |
| 固定快照、翻译端点、临时建 CSR | `src/storage/iceberg/lake.cpp` 中的 `LakeRead::Reader`、`mapEndpoints`、`adjacency` |
| 稳定字典、快照版本和缺端点补边 | `src/storage/iceberg/stable_csr.cpp` 中的 `StableNodes::slot`、`StableNodes::key`、`NativeStore::stableNodes`、`NativeStore::stablePin`、`PendingOrders::wake` |
| 写入独立库、增量 CREATE、读取原生邻接 | `src/storage/iceberg/native_csr.cpp` 中的 `ensureSlots`、`copyTopology`、`writeEdges`、`NativeCursor::seek`、`NativeCursor::next` |
| 原生 COPY 怎样分组和写边 | `src/processor/map/map_copy_from.cpp`、`src/processor/operator/partitioner.cpp`、`src/processor/operator/persistent/rel_batch_insert.cpp` 和同目录的 `copy_rel_batch_insert.cpp` |
| 从组头定位邻接段 | `src/graph/on_disk_graph.cpp` 的 `scanFwd`、`scanBwd`，`src/storage/table/csr_chunked_node_group.cpp` 的 `getStartCSROffset`、`getCSRLength`，以及 `src/storage/table/csr_node_group.cpp` |
| Cypher 识别、一跳和两跳取键、结果回填 | `src/planner/query_planner.cpp` 的 `Planner::planQuery`，以及 `src/storage/iceberg/two_hop_lookup.cpp` 的 `translateKeyLookup`、`KeyLookupScan::nextPaths`、`scanKeyLookup` |
| 多跳链的识别、游标与属性批次 | `src/storage/iceberg/path_key_lookup.cpp` 中的 `bindWalk`、`PathScan::next`、`scanPaths`、`tryPlanPathLookup` |
| 生成下推 SQL、通用整列读取、只读新增文件 | `src/storage/iceberg/lake.cpp` 中的 `LakeRead::lookup`、`fetch`、`readAdded` |
| 规模验证 | `examples/iceberg-query-csr/large_build.py` 分批预建基线，`examples/iceberg-query-csr/key_batch_benchmark.py` 比对真实查询、同快照 JOIN 和 DuckDB 扫描量 |
