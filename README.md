# 裂纹事件流契约排程 API

轨道检测裂纹事件流滚动升级的**契约兼容性排程服务**。提交采集端（发布端）与质检、
日报（两个独立部署的订阅端）新旧两版的扁平 JSON 字段定义，自动计算三方从全旧版
经混跑到全新版的安全升级顺序；不存在安全顺序时，指出受阻的版本组合、字段级违规
以及发布端可发出即被拒收的示例消息。

## 1. 背景与语义约定

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

## 2. 求解算法

三方 × 三阶段共 27 个状态，从 `(old, old, old)` 到 `(new, new, new)` 做 BFS：

- 每个节点只允许一方沿 `old→mixed→new` 推进一步；
- 新状态通过“全部在运行 (发布端版本 × 订阅端版本) 组合均兼容”才入队；
- 8 个版本组合（2 发布端版本 × 2 订阅端 × 2 订阅端版本）的兼容性结果带缓存；
- BFS 天然给出步数最少的安全顺序（每方两步、共 6 步）；
- 走不到终点即无解：汇总所有不兼容版本组合、字段级违规、拒收反例消息，以及每个
  组合挡住了哪些升级动作；若初始状态本身不安全会单独标明 `initial_state_safe=false`。

反例消息是一条**发布端按自己契约确实可以发出**的扁平 JSON：必填字段齐全，
缺失类字段刻意不出现，枚举/类型冲突字段采用触发违规的具体值。

## 3. 目录结构

```
.
├── app/
│   ├── main.py        # FastAPI 入口、Swagger 示例挂载
│   ├── models.py      # Pydantic 模型与入参校验
│   ├── compat.py      # 版本对兼容性判定与反例消息构造
│   └── planner.py     # 27 状态空间 BFS 排程
├── samples/
│   ├── safe_subscriber_first.json   # 样例一：有安全顺序（先订阅端后发布端）
│   └── infeasible_circular.json     # 样例二：循环依赖，无解
├── tests/             # pytest：兼容规则 / 求解器 / HTTP 层（23 个用例）
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── run_samples.sh     # 一键用两个样例打 API
```

## 4. 启动方式

### Docker Compose（推荐，本机无需 Python）

```bash
docker compose up --build
# API:      http://localhost:8000
# Swagger:  http://localhost:8000/docs   （/ 会自动跳转）
# 健康检查:  http://localhost:8000/health
```

镜像内带 `HEALTHCHECK`，compose 同步配置了健康检查；以非 root 用户运行。

### 本地直接运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 一键演示两个样例

```bash
./run_samples.sh          # BASE_URL 可改指向其他环境
```

## 5. API 概览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/plan` | 提交三方契约，返回安全升级顺序或无解报告 |
| POST | `/api/v1/compatibility` | 返回全部 8 个版本组合的兼容性明细（含违规与反例） |
| GET | `/health` | 健康检查 |
| GET | `/docs` | Swagger UI，请求体已内置两个样例可一键填充 |

请求体结构（节选）：

```json
{
  "parties": [
    {
      "id": "collector",
      "role": "producer",
      "old_schema": [
        {"name": "defect_level", "type": "enum", "required": true,
         "enum_values": ["L1", "L2"]}
      ],
      "new_schema": [
        {"name": "defect_level", "type": "enum", "required": true,
         "enum_values": ["L1", "L2", "L3"]}
      ]
    }
  ]
}
```

`parties` 必须恰好包含 1 个 `producer` 和 2 个 `subscriber`，三方 `id` 互不相同；
同一份 schema 内字段名不得重复；非法入参返回 422 并带字段级错误说明。

## 6. 两个内置样例解读

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

## 7. 测试

```bash
pip install pytest httpx
python -m pytest -q
```

覆盖：未知字段忽略、可选字段缺失、必填缺失、整数/字符串类型冲突、枚举越界、
无界 string 对 enum、反例消息完整性、可解顺序的订阅端优先约束、无解死锁报告、
起点不安全、入参校验以及两个样例的 HTTP 端到端行为。
