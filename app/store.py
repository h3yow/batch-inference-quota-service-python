"""SQLite 持久化层：任务、记录、幂等键、限流窗口的存取。

所有写操作在单连接 + 事务内完成，保证多线程提交时不互相踩踏；
服务重启后从库中恢复未完成任务。
"""
from __future__ import annotations

import sqlite3
import threading

from app.models import Quota, Record, Task


class Store:
    def __init__(self, db_path: str = ":memory:"):
        self._db_path = db_path
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None

    # ---- 生命周期 ----
    def open(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError

    # ---- 租户配额 ----
    def upsert_quota(self, quota: Quota) -> None:
        raise NotImplementedError

    def get_quota(self, tenant_id: str) -> Quota | None:
        raise NotImplementedError

    # ---- 任务与幂等 ----
    def create_task_if_absent(
        self, task: Task, records: list[Record]
    ) -> tuple[Task, bool]:
        """按 (tenant_id, idempotency_key) 幂等建任务。

        返回 (任务, 是否新建)。若键已存在则返回已有任务，不覆盖。
        """
        raise NotImplementedError

    def get_task(self, task_id: str) -> Task | None:
        raise NotImplementedError

    def get_task_by_key(self, tenant_id: str, idempotency_key: str) -> Task | None:
        raise NotImplementedError

    def update_task_status(self, task_id: str, status, error: str | None = None) -> None:
        raise NotImplementedError

    def list_runnable_tasks(self) -> list[Task]:
        """重启恢复用：返回 PENDING/RUNNING 状态的任务。"""
        raise NotImplementedError

    def count_active_tasks(self, tenant_id: str) -> int:
        """统计租户 RUNNING 状态任务数（并发额度判断）。"""
        raise NotImplementedError

    def list_queued_tasks(self, tenant_id: str) -> list[Task]:
        raise NotImplementedError

    # ---- 记录 ----
    def list_records(self, task_id: str, status=None) -> list[Record]:
        raise NotImplementedError

    def save_record_result(self, record: Record) -> None:
        raise NotImplementedError

    def reset_failed_records(self, task_id: str) -> int:
        """把 FAILED 记录重置为 PENDING，返回重置条数。"""
        raise NotImplementedError

    # ---- 提交限流 ----
    def hit_rate_limit(self, tenant_id: str, window_seconds: float, limit: int, now: float) -> bool:
        """在窗口内计数+1，超限返回 True。事务内完成，并发安全。"""
        raise NotImplementedError
