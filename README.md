# 夜间包裹扫描查询 · 候选索引成本评估 & 快照导出 API

物流数仓工程师在优化夜间包裹扫描查询前，用这套服务判断 **B-tree 候选索引能否降低查询成本**；
同时提供 **某仓一个夜班扫描明细的快照导出 API**，把冻结后的原始记录逐页交给下游入仓。
基于 **FastAPI + psycopg(v3) + PostgreSQL 15 + HypoPG**。评估只做 **EXPLAIN 估算**：
不执行查询（`EXPLAIN` 不带 `ANALYZE`），也不创建真实索引（HypoPG 假想索引仅存于后端内存）。

## 能力与约束（与需求逐条对应）

| 需求 | 实现 |
| --- | --- |
| 提交只读 SELECT + 最多 3 个候选（表名 + 有序列名） | `POST /evaluate`，Pydantic 校验候选数 1~3、标识符合法性、候选不重复 |
| 返回无候选 / 全候选启用的 EXPLAIN JSON、总成本、采用的候选 | 响应含 `baseline_plan`、`hypothetical_plan`、两套 `total_cost`、`adopted_candidates` / `rejected_candidates` |
| 只估算，不执行查询、不建真实索引 | `EXPLAIN (FORMAT JSON)`；`hypopg_create_index()` 只在会话内存造假想索引 |
| 单 API 进程最多复用两条数据库连接 | `TwoConnectionPool`：信号量硬上限 2，单 uvicorn worker；`/health` 可观测；快照构建也只用这个池中的一条租约连接 |
| 并发评估只能看见各自候选 | HypoPG 假想索引是**会话级**状态，一条租约连接一个会话，天然隔离 |
| 每次基线不继承旧候选 | 借出连接先 `ROLLBACK + hypopg_reset` 并验证为 0 才放行；响应字段留证 |
| 报错 / 断连时等真实 DB 工作停止、事务与假想索引清干净后才归还 | 租约上下文统一 `full_cleanup`：取消在途查询 → ROLLBACK → hypopg_reset → 协议状态与计数双重验证；清理不可确认的连接直接关闭丢弃，绝不回队 |
| 迟到清理不取消下一请求、不删其候选 | 清理全程 `asyncio.shield` 屏蔽取消；下一请求只从队列取已验证干净的连接 |
| 失败不返回部分比较 | 任何异常都不产出半成品响应；非法输入 400，内部错误 500 |
| 可实际启动的 Docker Compose，/docs 可提交评估 | `docker compose up` 后访问 <http://localhost:8000/docs> |
| 能改善 / 不能改善计划的索引样例 | `GET /samples`，内置两组样例及配套候选 |
| 可演示创建候选后暂停、断连再复用连接 | 请求头 `X-Demo-Pause-Seconds`（可选 `X-Demo-Pause-Mode`） |

## 快照导出 API（新增）

| 需求 | 实现 |
| --- | --- |
| 提交客户端导出键、仓号、UTC 业务日 | `POST /snapshots`：`{"export_key", "warehouse_id", "business_date"}`（可选 `page_size`） |
| 当日 18:00 至次日 06:00 左闭右开 | 窗口固定为 `business_date 18:00Z`（含）至 `+12h`（不含），响应回显 `window_start`/`window_end` |
| 最多 5000 条原始记录，按 scan_time、scan_id 排序 | 单条 `SELECT ... ORDER BY scan_time, scan_id LIMIT 5001`；多读 1 条用于判定截断（`truncated`），入库仍 ≤ 5000 |
| 每页最多 200 条 | 创建与翻页都限制 1~200；每页返回 `next_cursor`，无下一页时为 `null` |
| 首次请求返回快照号及首批数据 | `POST /snapshots` 返回 `snapshot_id` + `records` + `offset`/`has_more` |
| 后续用游标读取同一批冻结记录 | `GET /snapshots/{snapshot_id}/pages?cursor=...`，记录内容与顺序与首批完全一致 |
| 有效期内同键同参数的并发请求与断连重试复用同一构建结果 | 并发构建合并为**同一个后台 Task**；请求协程被取消（断连）不会取消共享构建，重试直接复用；`reused` 字段标识 |
| 换参数报冲突 | 同 `export_key` 换仓号或业务日 → **409**（`page_size` 只影响本页节奏，不改变冻结集合，故不冲突） |
| 构建失败不留可翻页的半成品 | 先完整读取、再注册快照；任何读取/超时/异常都只返回 500，不产生 `snapshot_id` / 游标 |
| 源表随后变化不影响已冻结页 | 首次构建即把原始记录复制进内存，后续补录/更正/删除都读不到 |
| 快照保留十分钟 | `SNAPSHOT_TTL_SECONDS`（默认 600），后台定期清理 + 读取时按 `monotonic` 再判一次 |
| 到期或服务重启后旧游标明确失效 | 游标带 HMAC 签名；签名有效但快照不在内存 → **410 snapshot_gone**；签名/归属不合法 → **400 invalid_cursor** |
| 保持 /evaluate 可用、单进程两条物理连接上限 | 快照构建复用现有 `TwoConnectionPool` 一条租约连接，物理连接上限仍为 2 |
| 内存有界 | 每个快照 ≤ 5000 行；`SNAPSHOT_MAX_ACTIVE`（默认 32）限制有效快照数，满了返回 **503**，不淘汰仍在有效期内的快照 |

### 使用示例

```bash
# 1) 创建快照（业务日 2026-11-11 → 窗口 11-11 18:00Z ~ 11-12 06:00Z）
curl -s -X POST http://localhost:8000/snapshots \
  -H 'Content-Type: application/json' \
  -d '{"export_key":"nightly-3-20261111","warehouse_id":3,
       "business_date":"2026-11-11","page_size":200}'

# 2) 用返回的 snapshot_id 与 next_cursor 翻页
curl -s 'http://localhost:8000/snapshots/<snapshot_id>/pages?cursor=<next_cursor>'
```

- 同 `export_key` + 同仓 + 同业务日重复调用（包括并发和断连重试）返回同一 `snapshot_id`，
  `reused=true`；换仓号或业务日返回 409。
- 快照 10 分钟后失效，此后翻页返回 410；服务重启后内存快照消失，旧游标同样返回 410。
- 种子数据中可直接演示的窗口：仓 3 / `2026-11-11`（8334 行，会被截断为 5000 行）；
  仓 3 / `2026-11-13` 为空窗口。

`/health` 额外暴露快照观测：`active_snapshots`、`builds_in_flight`、`max_active`、
`total_built`、`total_reused`、`total_expired`、`total_failed`。

## 目录结构

```
.
├── docker-compose.yml          # db(Postgres+HypoPG) + api(FastAPI)，api 单进程
├── db/
│   ├── Dockerfile              # postgres:15-bookworm + postgresql-15-hypopg
│   └── initdb/
│       ├── 01_schema.sql       # package_scan 事实表（仅主键真实索引）
│       └── 02_seed.sql         # 确定性生成 50 万行扫描明细 + ANALYZE
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py             # 路由 / 生命周期 / 异常映射
│       ├── config.py           # 环境变量配置（连接上限、超时、快照 TTL、演示开关）
│       ├── db.py               # 两连接租约池 + 取消安全清理
│       ├── service.py          # HypoPG 评估主流程（EXPLAIN、成本、采纳判定）
│       ├── snapshot.py         # 快照冻结/复用/游标签名/过期失效
│       ├── validation.py       # 只读 SELECT 解析守卫
│       ├── schemas.py          # 请求/响应模型
│       └── samples.py          # 预置样例 SQL 与候选
├── tests/
│   ├── test_api.py             # 评估相关端到端断言（47 项）
│   └── test_snapshot.py        # 快照导出端到端断言（125 项，含翻页/并发/冻结）
└── scripts/demo.sh             # 命令行演示（正常评估 / 断连复用 / 快照翻页）
```

## 启动

```bash
docker compose up --build
# 首次启动会初始化数据库并灌入 50 万行数据，约需十几秒
# - Swagger 文档： http://localhost:8000/docs （可创建快照、翻页、提交评估）
# - 健康检查：     http://localhost:8000/health
# - 预置样例：     http://localhost:8000/samples
# - 快照导出：     POST /snapshots，GET /snapshots/{snapshot_id}/pages
```

## 快速体验

### 1) 能改善计划的候选（warehouse_id 等值 + scan_time 排序）

```bash
curl -s -X POST http://localhost:8000/evaluate \
  -H 'Content-Type: application/json' \
  -d '{
    "sample_id": "nightly_warehouse_scan",
    "candidates": [
      {"table": "public.package_scan", "columns": ["warehouse_id", "scan_time"]}
    ]
  }'
```

基线 `Seq Scan + Sort`（成本约 15920）→ 假想索引下 `Index Scan Backward`（成本约 772），
候选进入 `adopted_candidates`。

### 2) 不能改善计划的候选（谓词无关列 / 全表聚合）

```bash
curl -s -X POST http://localhost:8000/evaluate \
  -H 'Content-Type: application/json' \
  -d '{
    "sample_id": "nightly_warehouse_scan",
    "candidates": [
      {"table": "public.package_scan", "columns": ["destination"]},
      {"table": "public.package_scan", "columns": ["weight_kg"]}
    ]
  }'
```

成本与计划均不变，两个候选进入 `rejected_candidates`。
`nightly_aggregate` 样例（全表 GROUP BY）下任何 B-tree 都不会被采用。

### 3) 暂停 + 断连 + 连接复用演示

```bash
# 终端 A：发起一个创建候选后暂停 30 秒的请求
curl -X POST http://localhost:8000/evaluate \
  -H 'Content-Type: application/json' -H 'X-Demo-Pause-Seconds: 30' \
  -d '{"sample_id":"nightly_warehouse_scan",
       "candidates":[{"table":"public.package_scan","columns":["warehouse_id","scan_time"]}]}'
# 在另一个终端 Ctrl+C / 杀掉 curl 模拟断连；随后再发普通请求，
# /health 中 cancels_performed 增加，total_discarded 保持 0，新请求看不到任何残留候选。
```

- 默认 `X-Demo-Pause-Mode: asyncio`：暂停不占用数据库工作，断连检测即时，
  服务端走完整取消清理后把连接回队复用。
- 设为 `pg_sleep` 时，暂停是一条真实在途 `SELECT pg_sleep(...)`，断连会先
  `conn.cancel()` 停掉服务端查询再清理（需要操作系统允许进程信号，标准容器内核支持）。

### 4) 快照导出与翻页

```bash
# 创建（首次构建），记下 snapshot_id 与 next_cursor
curl -s -X POST http://localhost:8000/snapshots \
  -H 'Content-Type: application/json' \
  -d '{"export_key":"nightly-3-20261111","warehouse_id":3,
       "business_date":"2026-11-11","page_size":200}' | tee /tmp/snap.json

# 用游标翻到下一页
SNAP=$(python3 -c "import json;print(json.load(open('/tmp/snap.json'))['snapshot_id'])")
CUR=$(python3 -c "import json;print(json.load(open('/tmp/snap.json'))['next_cursor'])")
curl -s "http://localhost:8000/snapshots/$SNAP/pages?cursor=$CUR"

# 同键同参数重试 -> 复用（reused=true，同一 snapshot_id）
# 同键换仓号/业务日 -> 409；快照过期或服务重启后旧游标 -> 410
```

也可直接运行 `bash scripts/demo.sh`（含评估与快照翻页两段演示）。

## 安全设计

1. **SQL 只读双保险**：`sqlparse` 词法层拒绝多语句与一切写/DDL 关键字；
   每条连接 `SET default_transaction_read_only = on`，数据库自身再挡一层。
2. **标识符注入防护**：表名/列名经 Pydantic 正则与 psycopg `Identifier` 双重处理。
3. **慢查询兜底**：会话级 `statement_timeout`、`lock_timeout`、
   `idle_in_transaction_session_timeout`；快照构建另有 `SET LOCAL` 与
   `asyncio.wait_for` 双层超时。
4. **脏连接绝不复用**：清理后校验协议事务状态为 IDLE 且假想索引计数为 0，
   不达标即关闭连接并新建，信号量保证任意时刻物理连接数 ≤ 2。
5. **游标防篡改**：快照游标 = base64(JSON) + HMAC-SHA256 截断签名，
   签名不符返回 400；签名有效但快照不存在（过期/重启）返回 410。

## 快照一致性与失效语义（实现要点）

- **窗口**：`business_date` 当天的 18:00Z 起、连续 12 小时，`[start, end)`。
- **原子冻结**：单条 `SELECT` 读取 `LIMIT 5001`，读完后才创建快照对象并注册；
  注册前任何异常都不会留下 `snapshot_id`，因此不会出现“第一页能翻、后面缺页”的半成品。
- **单键单快照**：`export_key` 在有效期内映射到唯一快照号。并发首次请求共享同一个
  后台构建 Task；客户端断连只取消请求协程，不取消共享构建，重试即复用。
- **冲突**：同键换仓号或业务日返回 409，并在响应中列出已有参数与本次参数。
- **冻结语义**：记录在构建时复制进内存，之后源表的补录/更正/删除都不影响任何一页。
- **失效**：TTL 默认 600 秒（`SNAPSHOT_TTL_SECONDS`）；后台定期清理，读取时也会
  按 `monotonic` 再判一次。到期或进程重启后，快照不在内存中，旧游标统一 410。
- **页大小**：创建与翻页均限制 1~200；游标记录创建时的页大小，翻页不传
  `page_size` 时沿用该节奏，显式传入可临时调整本页条数。

## 本地（无 Docker）运行测试

仓库的测试可指向任意已装 HypoPG 的 Postgres：

```bash
pip install -r api/requirements.txt
DB_DSN=postgresql://USER@HOST:PORT/logistics \
DEMO_PAUSE_ENABLED=true \
uvicorn app.main:app --app-dir api --port 8000
python -I tests/test_api.py http://127.0.0.1:8000
python -I tests/test_snapshot.py http://127.0.0.1:8000
# 追加验证“源表变化不影响冻结页”：
TEST_DB_DSN=postgresql://USER@HOST:PORT/logistics \
  python -I tests/test_snapshot.py http://127.0.0.1:8000
```
