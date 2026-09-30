"""后台执行器：按租户并发额度调度任务，逐条打分并落库。

- 只处理 PENDING 记录：重启/重试后续跑时已完成记录绝不重算；
- 单条失败只标记该记录，任务继续跑完剩余记录；
- 打分版本不可用时整个任务以 INCOMPATIBLE_VERSION 失败，不给错误结果。
"""
from __future__ import annotations

import logging
import threading

from app import scorer
from app.models import RecordStatus, RejectReason, ServiceError, Task, TaskStatus
from app.store import Store

logger = logging.getLogger("batch.worker")


class Worker:
    def __init__(self, store: Store):
        self._store = store
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name="batch-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def notify(self) -> None:
        """有新任务或额度释放时唤醒调度循环。"""
        self._wake.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.run_once()
            self._wake.wait(timeout=0.5)
            self._wake.clear()

    def run_once(self) -> None:
        """同步执行一轮调度：提升排队任务，然后榨干所有可执行任务。"""
        while True:
            self._store.promote_queued_tasks()
            runnable = self._store.list_runnable_tasks()
            if not runnable:
                return
            for task in runnable:
                self._run_task(task)

    def _run_task(self, task: Task) -> None:
        logger.info(
            "task start task_id=%s tenant=%s idem_key=%s version=%s",
            task.task_id, task.tenant_id, task.idempotency_key, task.scorer_version,
        )
        try:
            fn = scorer.get_scorer(task.scorer_version)
        except ServiceError as exc:
            logger.error(
                "task abort task_id=%s tenant=%s reason=%s",
                task.task_id, task.tenant_id, exc.reason.value,
            )
            self._store.update_task_status(
                task.task_id, TaskStatus.COMPLETED_WITH_FAILURES,
                error=f"{RejectReason.INCOMPATIBLE_VERSION.value}: {exc.message}",
            )
            return

        self._store.update_task_status(task.task_id, TaskStatus.RUNNING)
        pending = self._store.list_records(task.task_id, RecordStatus.PENDING)
        for record in pending:
            try:
                record.output = fn(record.input)
                record.status = RecordStatus.SUCCEEDED
                record.error = None
            except Exception as exc:  # 单条失败不拖垮整个任务
                record.status = RecordStatus.FAILED
                record.error = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "record failed task_id=%s tenant=%s index=%d reason=%s",
                    task.task_id, task.tenant_id, record.record_index, record.error,
                )
            self._store.save_record_result(record)

        failed = self._store.list_records(task.task_id, RecordStatus.FAILED)
        if failed:
            self._store.update_task_status(
                task.task_id, TaskStatus.COMPLETED_WITH_FAILURES,
                error=f"{len(failed)} record(s) failed",
            )
        else:
            self._store.update_task_status(task.task_id, TaskStatus.SUCCEEDED)
        logger.info(
            "task done task_id=%s tenant=%s failed=%d",
            task.task_id, task.tenant_id, len(failed),
        )
