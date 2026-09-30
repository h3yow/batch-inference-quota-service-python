# batch-inference-quota-service

本地化的批量推理/评分服务：接收多个租户提交的批量数据，逐条打分并返回结果。
只依赖本机能力（FastAPI + SQLite），不接外部服务或真实凭据。

## 运行

```bash
uv sync                 # 安装依赖
uv run python main.py   # 启动服务，监听 127.0.0.1:8000，数据落盘 batch_service.db
uv run pytest -q        # 运行全部测试（22 个用例）
```

## 使用方式

```bash
# 1. 注册租户并配置配额（未注册的租户提交会被 403 UNKNOWN_TENANT 拒绝）
curl -X PUT localhost:8000/tenants/t1/quota -H 'content-type: application/json' -d '{
  "max_concurrent_tasks": 2,      # 并发执行数
  "max_submits_per_window": 10,   # 单位时间提交次数
  "window_seconds": 60,
  "max_records_per_task": 100     # 单任务记录数上限
}'

# 2. 提交批量任务（必须带幂等键 + 打分版本）
curl -X POST localhost:8000/tasks -H 'content-type: application/json' -d '{
  "tenant_id": "t1", "idempotency_key": "order-2026-09-30",
  "records": [{"text": "hello"}, {"text": "world"}], "scorer_version": "v1"
}'

# 3. 查询任务与逐条结果
curl localhost:8000/tasks/<task_id>

# 4. 修好失败数据后，只重试失败项（从上次进度续跑）
curl -X POST localhost:8000/tasks/<task_id>/retry
```

记录格式：`{"text": "..."}`；`{"fail": true}` 用于演练单条失败路径。
打分版本：`v1`（长度哈希）、`v2`（字符码哈希），见 `app/scorer.py`，
新版本用 `scorer.register(version, fn)` 注册。

## 幂等规则

- 同一 `(tenant_id, idempotency_key)` 重复提交**相同内容**：返回已有任务（HTTP 200，
  `deduplicated: true`），不重复计算。
- 同一幂等键但**内容不同**（records 或 scorer_version 变了）：HTTP 409
  `IDEMPOTENCY_CONFLICT`，明确拒绝，**绝不覆盖**已有任务。
- 内容指纹 = `sha256(records + scorer_version)`，随任务落库，重启后依然有效。
- 并发下同键提交由数据库唯一约束兜底，全集群只建一个任务。

## 配额规则

| 限制 | 超限行为 | 原因码 / 状态 |
|---|---|---|
| 租户未注册 | 拒绝 403 | `UNKNOWN_TENANT` |
| 单任务记录数 | 拒绝 422 | `TOO_MANY_RECORDS` |
| 单位时间提交次数（滑动窗口） | 拒绝 429 | `RATE_LIMITED` |
| 并发执行数 | 不拒绝，任务置为 `QUEUED` | 有额度后自动提升为 `PENDING` 执行 |

限流计数、配额判定都在 SQLite 事务内完成，多请求并发提交不会互相踩踏；
配额按租户隔离，互不影响。

## 失败与重试

- 单条记录失败只标记该条（`FAILED` + 具体原因），任务继续跑完其余记录，
  最终状态为 `COMPLETED_WITH_FAILURES`。
- `POST /tasks/{id}/retry` 只把失败记录重置为 `PENDING` 续跑，
  已成功记录**不重算**（测试用计数打分器验证）。
- 没有失败记录时重试返回 409 `NO_FAILED_RECORDS`。

## 重启恢复

任务、逐条记录进度、幂等键、限流事件全部落 SQLite（WAL）。
进程重启时 `recover()` 把中断在 `RUNNING` 的任务重置为 `PENDING` 重新调度，
已完成的记录不重跑，未完成任务不丢。

## 版本兼容策略

- 任务提交时锁定 `scorer_version` 并落库，重试/重放永远用提交时的版本，
  打分器升级不影响老任务结论的可复现性。
- 提交时版本不存在：400 `INCOMPATIBLE_VERSION`。
- 版本在提交后被移除（如重启后注册表变化）：执行时任务以
  `INCOMPATIBLE_VERSION` 失败，**明确报错而不是给出错的结果**。

## 日志

运行日志包含 `tenant`、`task_id`、`idem_key`、`version` 以及拒绝/失败原因，例如：

```
INFO batch.service submit tenant=t1 idem_key=k1 task_id=... status=PENDING records=2 version=v1
WARNING batch.quota reject tenant=t1 reason=RATE_LIMITED limit=10/60.0s
WARNING batch.worker record failed task_id=... tenant=t1 index=1 reason=ValueError: ...
INFO batch.service recover task_id=... tenant=t1 idem_key=k1 status=PENDING
```

## 本地验证

```bash
uv run pytest -q
# tests/test_idempotency.py  并发提交、重复幂等、内容冲突不覆盖
# tests/test_quota.py        限流/记录数/并发排队/租户隔离/并发限额精确
# tests/test_retry.py        单条失败隔离、只重试失败项、续跑不重算
# tests/test_recovery.py     重启续跑不丢不重
# tests/test_version.py      版本锁定、可复现、版本下线明确报错
# tests/test_api.py          HTTP 端到端与错误码映射
```

## 结构

```
app/models.py   类型与原因码        app/service.py  编排：幂等+配额+重试+恢复
app/store.py    SQLite 持久化       app/worker.py   后台执行器（逐条落库）
app/quota.py    配额检查            app/scorer.py   版本化打分器注册表
app/api.py      FastAPI 路由        main.py         启动入口
```
