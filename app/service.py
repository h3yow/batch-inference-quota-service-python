"""批量任务编排：提交（幂等+配额）、查询、失败重试、重启恢复。"""
from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid

from app import scorer
from app.models import (
    Record, RecordStatus, RejectReason, ServiceError,
    SubmitResult, Task, TaskStatus,
)
from app.quota import QuotaManager
from app.store import Store
from app.worker import Worker

logger = logging.getLogger("batch.service")


def _request_hash(records: list, scorer_version: str) -> str:
    payload = json.dumps(
        {"records": records, "scorer_version": scorer_version},
        sort_keys=True, ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


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
        now = time.time() if now is None else now
        request_hash = _request_hash(records, scorer_version)

        # 1) 幂等键已存在：内容一致直接返回旧任务；不一致明确拒绝，绝不覆盖。
        existing = self._store.get_task_by_key(tenant_id, idempotency_key)
        if existing is not None:
            if existing.request_hash == request_hash:
                logger.info(
                    "dedup tenant=%s idem_key=%s task_id=%s",
                    tenant_id, idempotency_key, existing.task_id,
                )
                return SubmitResult(task=existing, deduplicated=True)
            logger.warning(
                "reject tenant=%s idem_key=%s reason=%s",
                tenant_id, idempotency_key, RejectReason.IDEMPOTENCY_CONFLICT.value,
            )
            raise ServiceError(
                RejectReason.IDEMPOTENCY_CONFLICT,
                f"idempotency key {idempotency_key!r} already used with different content",
            )

        # 2) 版本必须在提交时可用，否则明确报错。
        scorer.get_scorer(scorer_version)

        # 3) 配额校验（权限/记录数/限流），超限抛带原因码的错误。
        self._quota.check_submit(tenant_id, len(records), now)

        # 4) 落库（并发下由唯一约束兜底），再按并发额度决定 PENDING/QUEUED。
        task = Task(
            task_id=uuid.uuid4().hex,
            tenant_id=tenant_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            scorer_version=scorer_version,
            status=TaskStatus.QUEUED,
            total_records=len(records),
            created_at=now,
            updated_at=now,
        )
        recs = [
            Record(task.task_id, i, r, RecordStatus.PENDING)
            for i, r in enumerate(records)
        ]
        task, created = self._store.create_task_if_absent(task, recs)
        if not created:
            # 并发下同键竞争者已抢先落库：内容一致则去重返回，否则冲突。
            if task.request_hash == request_hash:
                return SubmitResult(task=task, deduplicated=True)
            raise ServiceError(
                RejectReason.IDEMPOTENCY_CONFLICT,
                f"idempotency key {idempotency_key!r} already used with different content",
            )

        self._store.promote_queued_tasks()
        task = self._store.get_task(task.task_id) or task
        logger.info(
            "submit tenant=%s idem_key=%s task_id=%s status=%s records=%d version=%s",
            tenant_id, idempotency_key, task.task_id, task.status.value,
            len(records), scorer_version,
        )
        self._worker.notify()
        return SubmitResult(task=task, deduplicated=False)

    def get_task(self, task_id: str) -> Task:
        task = self._store.get_task(task_id)
        if task is None:
            raise ServiceError(RejectReason.TASK_NOT_FOUND, f"task {task_id!r} not found")
        return task

    def get_task_view(self, task_id: str) -> dict:
        task = self.get_task(task_id)
        records = self._store.list_records(task_id)
        return {"task": task, "records": records}

    def retry_failed(self, task_id: str) -> Task:
        """只重置失败记录并续跑，已完成记录不重算，版本仍用提交时的版本。"""
        task = self.get_task(task_id)
        scorer.get_scorer(task.scorer_version)  # 版本不兼容时明确报错
        reset = self._store.reset_failed_records(task_id)
        if reset == 0:
            raise ServiceError(
                RejectReason.NO_FAILED_RECORDS,
                f"task {task_id!r} has no failed records to retry",
            )
        self._store.update_task_status(task_id, TaskStatus.QUEUED)
        self._store.promote_queued_tasks()
        logger.info(
            "retry tenant=%s task_id=%s reset_records=%d version=%s",
            task.tenant_id, task_id, reset, task.scorer_version,
        )
        self._worker.notify()
        return self.get_task(task_id)

    def recover(self) -> None:
        """启动恢复：崩溃时 RUNNING 的任务回到 PENDING，排队任务按额度提升。

        记录级进度已在库中，已完成的记录不会重跑。
        """
        runnable = self._store.list_runnable_tasks()
        for task in runnable:
            if task.status == TaskStatus.RUNNING:
                self._store.update_task_status(task.task_id, TaskStatus.PENDING)
            logger.info(
                "recover task_id=%s tenant=%s idem_key=%s status=%s",
                task.task_id, task.tenant_id, task.idempotency_key, task.status.value,
            )
        self._store.promote_queued_tasks()
        if runnable:
            self._worker.notify()
