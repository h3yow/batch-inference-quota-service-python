"""核心数据模型：任务、记录、配额、错误原因等类型定义。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class TaskStatus(str, enum.Enum):
    QUEUED = "QUEUED"        # 已受理，等待并发额度
    PENDING = "PENDING"      # 等待执行
    RUNNING = "RUNNING"      # 执行中
    SUCCEEDED = "SUCCEEDED"  # 全部成功
    COMPLETED_WITH_FAILURES = "COMPLETED_WITH_FAILURES"  # 部分记录失败


class RecordStatus(str, enum.Enum):
    PENDING = "PENDING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class RejectReason(str, enum.Enum):
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"  # 幂等键相同但内容不同
    RATE_LIMITED = "RATE_LIMITED"                  # 单位时间提交次数超限
    TOO_MANY_RECORDS = "TOO_MANY_RECORDS"          # 单任务记录数超限
    UNKNOWN_TENANT = "UNKNOWN_TENANT"              # 租户不存在/无权限
    INCOMPATIBLE_VERSION = "INCOMPATIBLE_VERSION"  # 打分版本不可用
    TASK_NOT_FOUND = "TASK_NOT_FOUND"
    NO_FAILED_RECORDS = "NO_FAILED_RECORDS"


class ServiceError(Exception):
    """业务错误，携带可区分的原因码。"""

    def __init__(self, reason: RejectReason, message: str):
        super().__init__(message)
        self.reason = reason
        self.message = message


@dataclass
class Quota:
    """单个租户的配额配置。"""

    tenant_id: str
    max_concurrent_tasks: int = 2      # 并发执行数
    max_submits_per_window: int = 10   # 单位时间提交次数
    window_seconds: float = 60.0       # 提交限流窗口
    max_records_per_task: int = 100    # 单任务记录数上限


@dataclass
class Task:
    task_id: str
    tenant_id: str
    idempotency_key: str
    request_hash: str
    scorer_version: str
    status: TaskStatus
    total_records: int
    created_at: float
    updated_at: float
    error: str | None = None


@dataclass
class Record:
    task_id: str
    record_index: int
    input: Any
    status: RecordStatus
    output: Any = None
    error: str | None = None


@dataclass
class SubmitResult:
    task: Task
    deduplicated: bool = False  # True 表示命中幂等，返回的是已有任务
