# 本地化批量推理/评分服务

一套只依赖本机能力的批量打分服务：接收多个租户提交的批量数据，按版本化打分器逐条评分并返回结果。不依赖任何外部服务或真实凭据，状态全部持久化在本机 SQLite 文件中。

## 能力概览

- **幂等提交**：同一租户 + 同一幂等键 + 相同请求内容 → 返回原任务，不重复计算；同键不同内容 → 明确拒绝（`idempotency_conflict`），绝不悄悄覆盖。
- **租户配额**：并发执行数、单位时间提交次数、单任务记录数、租户开关四类限制；超限明确拒绝或进入可区分的 `queued` 排队状态。
- **故障隔离与续跑**：单条记录失败不拖垮整个任务，失败项带可区分原因单独标出；修复后可只重试失败项，从上次进度接着跑；服务重启后未完成任务自动恢复，不丢也不重复跑。
- **版本化打分**：任务按提交时的打分版本执行并可复现；版本不兼容时明确报错，绝不给出错误结果。
- **可观测日志**：运行日志包含任务 ID、租户、幂等键以及拒绝/失败原因。

## 目录结构

```
app/
  models.py    # 状态机、拒绝/失败原因枚举、数据模型
  storage.py   # SQLite 持久化（任务、记录、幂等键、提交计数）
  quota.py     # 租户配额（并发槽、频率、记录数、开关）
  scoring.py   # 版本化打分器注册表 + 内置示例打分器
  service.py   # 核心服务：提交/幂等/配额/调度/执行/重试/恢复
  api.py       # FastAPI HTTP 接口
main.py        # 本地启动入口（装配 + uvicorn）
tests/         # pytest 测试
```

## 快速开始

```bash
uv sync                 # 安装依赖
uv run python main.py   # 启动服务，监听 127.0.0.1:8000
```

提交一个批量任务：

```bash
curl -X POST http://127.0.0.1:8000/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "tenant-a",
    "idempotency_key": "batch-2026-09-30-001",
    "records": [{"x": 1.0}, {"x": 2.0}],
    "scorer_version": "1.0"
  }'
# => {"accepted": true, "job_id": "...", "status": "queued", ...}
```

查询结果与重试失败项：

```bash
curl http://127.0.0.1:8000/v1/jobs/<job_id>
curl -X POST http://127.0.0.1:8000/v1/jobs/<job_id>/retry
```

## 使用规则

### 幂等

- 提交必须带 `idempotency_key`，作用域为单个租户（不同租户可用同键）。
- 同键同内容：返回原任务（`duplicated: true`），不重复计算。
- 同键不同内容：HTTP 409，`reject_reason = idempotency_conflict`。
- 请求指纹 = 记录内容 + 打分版本的 SHA-256；被限流/拒绝的请求**不占用**幂等键，修正后可原键重试。

### 配额（`TenantQuota`，按租户配置，见 `main.py`）

| 限制 | 字段 | 超限行为 |
|---|---|---|
| 并发执行数 | `max_concurrent_jobs` | 任务保持 `queued` 排队，槽位释放后自动执行（不拒绝） |
| 单位时间提交次数 | `max_submits_per_window` / `window_seconds` | HTTP 429，`rate_limited` |
| 单任务记录数 | `max_records_per_job` | HTTP 422，`record_limit_exceeded` |
| 租户开关 | `enabled` | HTTP 403，`tenant_disabled` |

并发提交由 SQLite 单连接事务 + 幂等键唯一约束裁决，配额与任务状态不会互相踩踏。

### 失败与重试

- 单条失败原因：`invalid_input`（payload 非法）、`scorer_error`（打分器内部错误）、`version_incompatible`（记录与版本不兼容）。
- 任务终态：`succeeded`（全部成功）、`partial_failed`（部分失败）、`failed`（全部失败或版本不兼容）。
- `POST /v1/jobs/{job_id}/retry` 仅对 `partial_failed` / `failed` 任务有效：把失败记录重置为待跑，已成功记录**不重算**，从上次进度续跑。
- 修复失败项：更新该记录的 payload 后再调用 retry（程序化场景直接改 `records` 表或重新提交新任务）。

### 版本兼容策略

- 打分器按名称注册多个版本，版本号为 `MAJOR.MINOR`；注册时可声明 `min_compatible_version`。
- 解析规则：
  1. 请求版本**精确注册**过 → 用该版本函数，保证老任务按提交时版本复现；
  2. 未注册但 `>= min_compatible_version` → 回落到当前版本；
  3. 否则 → 拒绝/报错（`version_incompatible`），绝不给出错误结果。
- 任务落库时记录 `scorer_version`，执行与重试始终按该版本解析。
- 不指定版本时默认使用当前最新版本。

### 持久化与恢复

- 所有状态存于 `batch_service.db`（SQLite，WAL 模式）。
- 每条记录打分后立即落库；重启时 `running` 任务重置为 `queued` 重新调度，已成功记录跳过——不丢、不重跑。

## 本地验证

```bash
uv run pytest -q        # 全部测试（22 个）
```

测试覆盖：

- `tests/test_idempotency.py`：重复幂等、内容冲突、租户隔离、并发同键/异键提交
- `tests/test_quota.py`：频率限制与恢复、记录数上限、租户禁用、并发槽排队、限流不占幂等键
- `tests/test_retry_and_recovery.py`：单条失败标记与重试续跑、健康任务拒绝重试、重启恢复不丢不重
- `tests/test_versioning.py`：老版本复现、默认当前版本、兼容回落、不兼容拒绝
- `tests/test_api.py`：HTTP 层提交/查询/409/404

手动验证日志（含任务、租户、幂等键、拒绝/失败原因）：

```bash
uv run python main.py   # 另开终端提交请求，观察控制台日志
```
