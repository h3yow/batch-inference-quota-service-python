"""批量任务编排：提交（幂等+配额）、查询、失败重试、重启恢复。"""
from __future__ import annotations

import logging

from app.models import SubmitResult, Task
from app.quota import QuotaManager
from app.store import Store
from app.worker import Worker

logger = logging.getLogger("batch.service")


class BatchService:
    def __init__(self, store: Store, quota: QuotaManager, worker: Worker):
        self._store = store
        self._quota = quota
        self._worker = worker

    def submit(
        self,
        tenant_id: str,
        idempotency_key: str,
        records: list,
        scorer_version: str,
        now: float | None = None,
    ) -> SubmitResult:
        raise NotImplementedError

    def get_task(self, task_id: str) -> Task:
        raise NotImplementedError

    def get_task_view(self, task_id: str) -> dict:
        """任务 + 记录明细。"""
        raise NotImplementedError

    def retry_failed(self, task_id: str) -> Task:
        """只重置失败记录并续跑，已完成记录不重算。"""
        raise NotImplementedError

    def recover(self) -> None:
        """启动时恢复未完成任务：不丢、不重跑已完成记录。"""
        raise NotImplementedError
