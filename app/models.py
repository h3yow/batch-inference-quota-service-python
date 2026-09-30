"""核心数据模型与枚举定义。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class JobStatus(str, enum.Enum):
    """任务状态机。"""

    QUEUED = "queued"            # 已通过校验，等待并发槽位
    RUNNING = "running"          # 正在执行
    SUCCEEDED = "succeeded"      # 全部记录成功
    PARTIAL_FAILED = "partial_failed"  # 部分记录失败，可重试失败项
    FAILED = "failed"            # 任务级失败
    REJECTED = "rejected"        # 提交即被拒绝（配额/权限/冲突）


class RecordStatus(str, enum.Enum):
    """单条记录状态。"""

    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class RejectReason(str, enum.Enum):
    """提交被拒绝的可区分原因。"""

    IDEMPOTENCY_CONFLICT = "idempotency_conflict"  # 幂等键相同但内容不同
    RATE_LIMITED = "rate_limited"                  # 单位时间提交次数超限
    CONCURRENCY_QUEUED = "concurrency_queued"      # 并发槽满，进入排队
    RECORD_LIMIT_EXCEEDED = "record_limit_exceeded"  # 单任务记录数超限
    TENANT_DISABLED = "tenant_disabled"            # 租户无权限
    VERSION_INCOMPATIBLE = "version_incompatible"  # 打分版本不兼容


class RecordFailureReason(str, enum.Enum):
    """单条记录失败的可区分原因。"""

    INVALID_INPUT = "invalid_input"          # 输入数据非法
    SCORER_ERROR = "scorer_error"            # 打分器内部错误
    VERSION_INCOMPATIBLE = "version_incompatible"  # 记录与版本不兼容


@dataclass(frozen=True)
class TenantQuota:
    """租户配额配置。"""

    max_concurrent_jobs: int = 2
    max_submits_per_window: int = 10
    window_seconds: float = 60.0
    max_records_per_job: int = 1000
    enabled: bool = True


@dataclass
class ScoreRecord:
    """单条待打分记录。"""

    record_id: str
    payload: dict[str, Any]
    status: RecordStatus = RecordStatus.PENDING
    score: float | None = None
    failure_reason: RecordFailureReason | None = None
    error_detail: str | None = None


@dataclass
class Job:
    """批量任务。"""

    job_id: str
    tenant_id: str
    idempotency_key: str
    request_fingerprint: str
    scorer_version: str
    status: JobStatus = JobStatus.QUEUED
    reject_reason: RejectReason | None = None
    records: list[ScoreRecord] = field(default_factory=list)
    created_at: float = 0.0
    updated_at: float = 0.0
