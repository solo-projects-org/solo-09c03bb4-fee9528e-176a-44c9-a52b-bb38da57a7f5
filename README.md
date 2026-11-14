# 轨检裂纹事件平台：契约排程 + 事件分发 API

轨检采集车裂纹事件流的两个配套服务，同一镜像、按需启动：

1. **契约排程 API**：提交采集端（发布端）与质检、日报（两个独立部署的订阅端）新旧两版
   的扁平 JSON 字段定义，自动计算三方从全旧版经混跑到全新版的安全升级顺序；不存在安全
   顺序时，指出受阻的版本组合、字段级违规以及发布端可发出即被拒收的示例消息。
2. **事件分发 API**：采集车补传裂纹事件（全局事件号、车号、里程、缺陷等级）时，先持久化，
   再向质检（qc）、日报（daily_report）两个本地接收端逐端投递，数据工程师可逐条核对
   每个端的送达情况（pending / confirmed）。

## 1. 服务组成

```
                 ┌──────────────────────────────┐
   采集车补传 ──▶ │  contract-scheduler (8000)   │
                 │  契约排程 API + 事件分发 API  │
                 │  事件库 SQLite（先持久化）    │
                 └───────┬──────────────┬───────┘
              POST /receive│              │POST /receive
                 ┌───────▼──────┐  ┌──────▼───────────┐
                 │ qc-receiver  │  │ report-receiver  │
                 │ 质检 (8100)  │  │ 日报 (8200)      │
                 │ 本地库 SQLite│  │ 本地库 SQLite    │
                 └──────────────┘  └──────────────────┘
```

三个容器共用同一镜像；接收端通过 `RECEIVER_NAME` 区分实例，数据各自落在命名卷里。

## 2. 事件分发链路设计

### 2.1 接入与逐端状态机

- `POST /api/v1/events` 接收单条事件（`event_id` 全局事件号、`car_id` 车号、
  `mileage_m` 里程、`defect_level` 缺陷等级）。
- **先持久化，再投递**：事件与两个接收端的 `pending` 行在同一事务落库后才外发；
  每次状态变更立即提交（SQLite WAL + `synchronous=FULL`），进程崩溃、容器重启都不丢。
- 每个端独立状态机：`pending`（待送）→ `confirmed`（已确认）。**一端失败只把该端记为
  pending 并自动重试，不妨碍另一端接收与确认**。
- 接收端**保存成功才应答 200**；分发端只有拿到 200 才把该端标记为 confirmed。

### 2.2 幂等与去重

| 场景 | 行为 |
| --- | --- |
| 同号同内容重提 | 幂等接受（200，`deduplicated=true`），只保留一条；顺带对未确认端补送 |
| 同号异内容 | 拒绝（409，返回逐字段差异），已入库记录不受影响 |
| 重投（重试/手动补送/重启补送） | 接收端按全局事件号去重，**只入库一次**，返回 `duplicate_ignored` |
| 已确认端 | 永不再投；状态更新带 `status='pending'` 前置条件，**确认不可撤销** |

### 2.3 回执丢失与重启恢复

- **回执丢失**（接收端已写入但响应未到达）：该端保持 pending；重试时接收端去重，
  不重复计数，确认后 `duplicates_suppressed` 可见去重次数；另一端的确认不受影响。
- **自动重试**：主服务后台循环按 `RETRY_INTERVAL_SECONDS`（默认 5s）重投所有 pending。
- **服务重启**：pending 行持久化在卷里，重启后自动补送；也可用
  `POST /api/v1/events/{event_id}/redeliver` 手动补送（可带 `?endpoint=qc` 指定端）。

## 3. 启动方式

### Docker Compose（推荐，一条命令起全部三个服务）

```bash
docker compose up --build
# 主服务 Swagger:   http://localhost:8000/docs   （/ 自动跳转）
# 质检接收端 Swagger: http://localhost:8100/docs
# 日报接收端 Swagger: http://localhost:8200/docs
```

### 本地直接运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

# 终端 1：主服务
uvicorn app.main:app --host 0.0.0.0 --port 8000
# 终端 2：质检接收端
RECEIVER_NAME=qc DATA_DIR=./data/qc uvicorn app.receiver:app --port 8100
# 终端 3：日报接收端
RECEIVER_NAME=daily_report DATA_DIR=./data/report uvicorn app.receiver:app --port 8200
```

主服务环境变量：`DATA_DIR`（默认 `./data/distributor`）、`QC_RECEIVER_URL`、
`REPORT_RECEIVER_URL`、`RETRY_INTERVAL_SECONDS`（默认 5，0 关闭自动补送）、
`DELIVERY_TIMEOUT_SECONDS`（默认 3）。

### 一键演示

```bash
./run_samples.sh      # 契约排程：两个内置样例打 /api/v1/plan
./demo_dispatch.sh    # 事件分发：正常投递→幂等→单端故障→回执丢失恢复
```

## 4. Swagger 演示：单端故障与回执丢失恢复

全部操作都在 Swagger UI 里完成（无需命令行）：

### 4.1 单端故障（一端中断不妨碍另一端）

1. 打开质检接收端 `http://localhost:8100/docs`，`POST /admin/behavior` 设为
   `{"mode": "down"}`（模拟中断：不保存直接 503；也可以直接
   `docker compose stop qc-receiver`）。
2. 主服务 `http://localhost:8000/docs`，`POST /api/v1/events` 提交事件
   （请求体已内置示例）：响应里 `qc=pending`（带 `last_error`）、
   `daily_report=confirmed`。
3. `GET /api/v1/events/{event_id}` 逐端核对送达情况。
4. 接收端恢复 `{"mode": "normal"}`（或 `docker compose start qc-receiver`）：
   等 ≤5 秒自动补送，或 `POST /api/v1/events/{event_id}/redeliver` 手动补送，
   状态变为双 confirmed；`skipped_confirmed` 显示已确认端未被重复投递。

### 4.2 回执丢失（已写入但回执未到达）

1. 日报接收端 `http://localhost:8200/docs`，`POST /admin/behavior` 设为
   `{"mode": "ack_loss"}`（保存成功后返回 500，模拟回执丢失）。
2. 主服务提交新事件：`daily_report=pending`，但接收端 `GET /stats` 显示
   `stored_events` 已 +1（确实写入了）。
3. 恢复 `{"mode": "normal"}` 后手动补送或等自动重试：
   - 接收端 `GET /stats`：`stored_events` **不再增加**（重投只入库一次），
     `duplicates_suppressed` +1；
   - 主服务 `GET /api/v1/events/{event_id}`：双 confirmed，且 qc 的
     `confirmed_at`、`attempts` 始终未变（确认不被撤销、不被重投）。

### 4.3 服务重启后的补送

1. 让某端处于 down，提交一个事件使其 pending。
2. `docker compose restart contract-scheduler`。
3. 重启后 `GET /api/v1/events/{event_id}`：pending 状态仍在（先持久化的结果）；
   接收端恢复后，后台循环自动补送为 confirmed，接收端不重复计数。

## 5. API 概览

### 主服务（8000）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/events` | 接收单条裂纹事件：先持久化再逐端投递；201 首次入库 / 200 幂等去重 / 409 同号异内容 |
| GET | `/api/v1/events` | 列出全部事件及逐端送达状态 |
| GET | `/api/v1/events/{event_id}` | 单条事件逐端状态查询（数据工程师核对用） |
| POST | `/api/v1/events/{event_id}/redeliver` | 手动补送未确认端（`?endpoint=` 可指定端） |
| GET | `/api/v1/endpoints` | 查看配置的两个本地接收端 |
| POST | `/api/v1/plan` | 契约排程：三方滚动升级安全顺序 |
| POST | `/api/v1/compatibility` | 全部 8 个版本组合兼容性明细 |
| GET | `/health` | 健康检查 |

### 每个接收端（8100 / 8200）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/receive` | 接收事件：保存成功才应答 200；重投去重返回 `duplicate_ignored`；同号异内容 409 |
| GET | `/events` | 查看已保存的事件 |
| GET | `/stats` | 统计：stored_events / receive_requests / duplicates_suppressed / conflicts_rejected |
| POST | `/admin/behavior` | 故障注入：`normal` / `down`（不保存 503）/ `ack_loss`（保存后 500） |
| GET | `/health` | 健康检查 |

## 6. 契约排程：背景与语义约定

滚动升级期间每一方都经历三个阶段：`old → mixed → new`，混合阶段新旧实例同时运行：

| 角色 | mixed 阶段含义 |
| --- | --- |
| 发布端 `producer`（采集端 collector） | 新旧实例都在发消息，因此 **old 与 new 两类消息都可能出现** |
| 订阅端 `subscriber`（质检 qc / 日报 daily_report） | 新旧实例都在收消息，因此 **两类消息都必须能被接收** |

字段契约规则（与需求一一对应）：

1. 扁平 JSON 字段，类型仅允许 `integer`、`string`、`enum`（字符串枚举）；
   `enum` 必须声明非空且唯一的 `enum_values`。
2. 每个字段标明 `required`：发布端必填字段保证发出；**可选字段可能整体缺失**。
3. 枚举字段实际可能取声明值中的**任意一个**。
4. **订阅端忽略未知字段**：发布端多发、订阅端未声明的字段不构成问题。
5. 订阅端 `required` 字段若发布端未声明或声明为可选，则存在“消息缺字段”的可能，
   组合判为不安全（`required_missing`）。
6. 同名字段类型不一致判 `type_mismatch`；订阅端用 `enum` 收窄取值域时，发布端能
   发出的某个值不在订阅端枚举内判 `enum_violation`（无界 `string` 对 `enum` 必违规）。

**安全状态**：任一在运行的发布端版本可能发出的每条消息，都能被两个订阅端的所有
在运行版本接收。升级的每一步（共 6 次单阶段推进）后都必须满足该条件。

## 7. 求解算法

三方 × 三阶段共 27 个状态，从 `(old, old, old)` 到 `(new, new, new)` 做 BFS：

- 每个节点只允许一方沿 `old→mixed→new` 推进一步；
- 新状态通过“全部在运行 (发布端版本 × 订阅端版本) 组合均兼容”才入队；
- 8 个版本组合（2 发布端版本 × 2 订阅端 × 2 订阅端版本）的兼容性结果带缓存；
- BFS 天然给出步数最少的安全顺序（每方两步、共 6 步）；
- 走不到终点即无解：汇总所有不兼容版本组合、字段级违规、拒收反例消息，以及每个
  组合挡住了哪些升级动作；若初始状态本身不安全会单独标明 `initial_state_safe=false`。

反例消息是一条**发布端按自己契约确实可以发出**的扁平 JSON：必填字段齐全，
缺失类字段刻意不出现，枚举/类型冲突字段采用触发违规的具体值。

## 8. 目录结构

```
.
├── app/
│   ├── main.py             # 主服务入口：排程路由 + 分发路由 + 后台补送循环
│   ├── models.py           # 排程 Pydantic 模型与入参校验
│   ├── compat.py           # 版本对兼容性判定与反例消息构造
│   ├── planner.py          # 27 状态空间 BFS 排程
│   ├── dispatch_models.py  # 事件/逐端状态/接收端模型（两端共享的线上契约）
│   ├── store.py            # 分发端 SQLite：事件表 + 逐端投递状态表
│   ├── dispatcher.py       # 分发服务：先持久化再投递、重试、重启补送
│   ├── dispatch_api.py     # 分发 API 路由
│   └── receiver.py         # 本地接收端（qc / daily_report，故障注入）
├── samples/
│   ├── safe_subscriber_first.json   # 排程样例一：有安全顺序（先订阅端后发布端）
│   ├── infeasible_circular.json     # 排程样例二：循环依赖，无解
│   └── crack_event.json             # 裂纹事件示例（分发演示用）
├── tests/                # pytest：兼容规则 / 求解器 / 分发链路 / 接收端 / HTTP 层
├── Dockerfile            # 单镜像：主服务与两个接收端共用
├── docker-compose.yml    # 三服务编排 + 命名卷持久化
├── requirements.txt
├── run_samples.sh        # 排程样例一键演示
└── demo_dispatch.sh      # 分发链路一键演示（故障注入 + 恢复）
```

## 9. 两个内置排程样例解读

### 样例一 `safe_subscriber_first.json` —— 先升级订阅端才能新增等级

- 采集端新版把枚举 `defect_level` 从 `[L1, L2]` 扩展为 `[L1, L2, L3]`；
- 质检、日报的旧版枚举都只认 `[L1, L2]`，收到 `L3` 即 `enum_violation`。

排程结论（6 步）：**qc 旧→混→新、daily_report 旧→混→新，最后 collector 旧→混→新**。
发布端在两个订阅端全部完成升级前不能进入混合阶段——一旦进入就可能发出带 `L3`
的新消息，而仍在运行的旧订阅端会拒收。`/api/v1/compatibility` 可看到
`collector:new -> qc:old` 与 `collector:new -> daily_report:old` 两个组合不兼容，
正是它们把发布端的升级动作挡在后面。

### 样例二 `infeasible_circular.json` —— 循环依赖，无解

- 采集端新版同样新增枚举值 `L3`，旧质检枚举不认（`collector:new → qc:old` 违规，
  反例 `{"defect_level": "L3", ...}`）→ 发布端升级被旧质检挡住；
- 质检新版同时新增必填字段 `inspector_id`，而旧采集端永远不发该字段
  （`collector:old → qc:new` 违规，反例消息里没有 `inspector_id`）→ 质检升级被
  旧发布端挡住。

两边互相等待，BFS 无法到达全新状态，`feasible=false`，响应的 `blocked_pairs`
给出每个受阻组合的违规字段、触发值/缺失说明、拒收反例以及被它挡住的具体升级
动作。要打破死锁，需先在契约层面解耦（例如把 `inspector_id` 改为可选、或让
旧质检先放宽枚举域再分两轮升级）。

## 10. 测试

```bash
pip install pytest httpx
python -m pytest -q
```

42 个用例，覆盖：

- **兼容规则**：未知字段忽略、可选字段缺失、必填缺失、整数/字符串类型冲突、
  枚举越界、无界 string 对 enum、反例消息完整性；
- **求解器**：可解顺序的订阅端优先约束、无解死锁报告、起点不安全、入参校验；
- **分发链路**：先持久化再投递、单端故障隔离、同号同内容幂等、同号异内容 409、
  回执丢失后不重复计数且不撤销另一端确认、重启后 pending 恢复与自动补送、
  逐端状态查询、手动补送、入参校验；
- **接收端**：保存成功才应答、重投只入库一次、冲突拒绝、down/ack_loss 故障注入、
  重启后本地库不丢。
