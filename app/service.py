"""批量任务核心服务：提交、幂等、配额、执行、重试、恢复。"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import uuid
from typing import Any

from app.models import (
    Job,
    JobStatus,
    RecordFailureReason,
    RecordStatus,
    RejectReason,
    ScoreRecord,
)
from app.quota import QuotaManager
from app.scoring import ScorerError, ScorerRegistry, VersionIncompatibleError
from app.storage import Storage

logger = logging.getLogger(__name__)


class SubmitResult:
    """提交结果：成功入队 / 幂等命中 / 明确拒绝。"""

    def __init__(
        self,
        accepted: bool,
        job_id: str | None = None,
        status: str | None = None,
        duplicated: bool = False,
        reject_reason: str | None = None,
        message: str = "",
    ) -> None:
        self.accepted = accepted
        self.job_id = job_id
        self.status = status
        self.duplicated = duplicated
        self.reject_reason = reject_reason
        self.message = message

    def to_dict(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "job_id": self.job_id,
            "status": self.status,
            "duplicated": self.duplicated,
            "reject_reason": self.reject_reason,
            "message": self.message,
        }


def _fingerprint(records: list[dict[str, Any]], scorer_version: str | None) -> str:
    """请求内容指纹：记录与打分版本的规范化哈希。"""
    canonical = json.dumps(
        {"records": records, "scorer_version": scorer_version},
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class BatchService:
    """批量推理/评分服务门面。"""

    def __init__(
        self,
        storage: Storage,
        quota: QuotaManager,
        scorers: ScorerRegistry,
        default_scorer: str = "default",
    ) -> None:
        self._storage = storage
        self._quota = quota
        self._scorers = scorers
        self._default_scorer = default_scorer
        self._stop = threading.Event()
        self._dispatcher: threading.Thread | None = None
        self._running_jobs: set[str] = set()
        self._running_lock = threading.Lock()
        self._job_threads: set[threading.Thread] = set()

    # ---- 提交 ----
    def submit(
        self,
        tenant_id: str,
        idempotency_key: str,
        records: list[dict[str, Any]],
        scorer_version: str | None = None,
    ) -> SubmitResult:
        log_ctx = {"tenant_id": tenant_id, "idempotency_key": idempotency_key}
        quota = self._quota.get_quota(tenant_id)

        # 1. 租户权限
        if not quota.enabled:
            logger.warning("submit rejected: tenant disabled", extra=log_ctx)
            return SubmitResult(
                False,
                status=JobStatus.REJECTED.value,
                reject_reason=RejectReason.TENANT_DISABLED.value,
                message=f"租户 {tenant_id} 已被禁用",
            )

        fp = _fingerprint(records, scorer_version)

        # 2. 幂等：先只读检查，命中直接返回（重复或冲突）
        existing_fp = self._storage.get_fingerprint(tenant_id, idempotency_key)
        if existing_fp is not None:
            return self._idempotent_response(
                tenant_id, idempotency_key, existing_fp, fp
            )

        # 3. 提交频率限制
        now = time.time()
        recent = self._storage.count_submits_since(
            tenant_id, now - quota.window_seconds
        )
        if recent >= quota.max_submits_per_window:
            logger.warning(
                "submit rejected: rate limited",
                extra={**log_ctx, "recent_submits": recent,
                       "max": quota.max_submits_per_window},
            )
            return SubmitResult(
                False,
                status=JobStatus.REJECTED.value,
                reject_reason=RejectReason.RATE_LIMITED.value,
                message=(
                    f"租户 {tenant_id} 在 {quota.window_seconds}s 窗口内"
                    f"已提交 {recent} 次，超过上限 {quota.max_submits_per_window}"
                ),
            )

        # 4. 单任务记录数限制
        if len(records) > quota.max_records_per_job:
            logger.warning(
                "submit rejected: record limit exceeded",
                extra={**log_ctx, "records": len(records),
                       "max": quota.max_records_per_job},
            )
            return SubmitResult(
                False,
                status=JobStatus.REJECTED.value,
                reject_reason=RejectReason.RECORD_LIMIT_EXCEEDED.value,
                message=(
                    f"单任务记录数 {len(records)} 超过上限 "
                    f"{quota.max_records_per_job}"
                ),
            )

        # 5. 打分版本兼容性
        version = scorer_version or self._scorers.current_version(
            self._default_scorer
        )
        try:
            self._scorers.resolve(self._default_scorer, version)
        except VersionIncompatibleError as exc:
            logger.warning(
                "submit rejected: version incompatible",
                extra={**log_ctx, "scorer_version": version},
            )
            return SubmitResult(
                False,
                status=JobStatus.REJECTED.value,
                reject_reason=RejectReason.VERSION_INCOMPATIBLE.value,
                message=str(exc),
            )

        # 6. 原子登记幂等键（并发下同键提交由插入唯一性裁决）
        job_id = uuid.uuid4().hex
        raced_fp = self._storage.try_register_idempotency(
            tenant_id, idempotency_key, fp, job_id
        )
        if raced_fp is not None:
            return self._idempotent_response(
                tenant_id, idempotency_key, raced_fp, fp
            )

        # 7. 落库任务与记录
        self._storage.record_submit(tenant_id, now)
        job = Job(
            job_id=job_id,
            tenant_id=tenant_id,
            idempotency_key=idempotency_key,
            request_fingerprint=fp,
            scorer_version=version,
            status=JobStatus.QUEUED,
            records=[
                ScoreRecord(record_id=f"rec-{i}", payload=r)
                for i, r in enumerate(records)
            ],
            created_at=now,
            updated_at=now,
        )
        self._storage.save_job(job)
        self._storage.save_records(job_id, job.records)
        logger.info(
            "job accepted",
            extra={**log_ctx, "job_id": job_id, "records": len(records),
                   "scorer_version": version},
        )
        return SubmitResult(
            True,
            job_id=job_id,
            status=JobStatus.QUEUED.value,
            message="任务已受理，排队等待执行",
        )

    def _idempotent_response(
        self, tenant_id: str, idempotency_key: str, existing_fp: str, fp: str
    ) -> SubmitResult:
        job_id = self._storage.get_job_id_by_idempotency(tenant_id, idempotency_key)
        if existing_fp == fp:
            job = self._storage.get_job(job_id) if job_id else None
            logger.info(
                "submit duplicated: same idempotency key and content",
                extra={"tenant_id": tenant_id,
                       "idempotency_key": idempotency_key, "job_id": job_id},
            )
            return SubmitResult(
                True,
                job_id=job_id,
                status=job.status.value if job else None,
                duplicated=True,
                message="相同幂等键与内容的请求已存在，返回原任务",
            )
        logger.warning(
            "submit rejected: idempotency conflict",
            extra={"tenant_id": tenant_id, "idempotency_key": idempotency_key,
                   "job_id": job_id},
        )
        return SubmitResult(
            False,
            job_id=job_id,
            status=JobStatus.REJECTED.value,
            reject_reason=RejectReason.IDEMPOTENCY_CONFLICT.value,
            message=(
                f"幂等键 {idempotency_key!r} 已被不同内容的请求占用，"
                "拒绝覆盖已有任务"
            ),
        )

    # ---- 查询 ----
    def get_job(self, job_id: str) -> Job | None:
        return self._storage.get_job(job_id)

    # ---- 重试失败项 ----
    def retry_failed(self, job_id: str) -> SubmitResult:
        job = self._storage.get_job(job_id)
        if job is None:
            return SubmitResult(False, message=f"任务不存在: {job_id}")
        if job.status not in (JobStatus.PARTIAL_FAILED, JobStatus.FAILED):
            return SubmitResult(
                False,
                job_id=job_id,
                status=job.status.value,
                message=f"任务状态 {job.status.value} 不支持重试失败项",
            )
        failed = [r for r in job.records if r.status == RecordStatus.FAILED]
        for r in failed:
            r.status = RecordStatus.PENDING
            r.failure_reason = None
            r.error_detail = None
            self._storage.update_record(job_id, r)
        self._storage.update_job_status(job_id, JobStatus.QUEUED)
        logger.info(
            "job requeued for retry of failed records",
            extra={"tenant_id": job.tenant_id, "job_id": job_id,
                   "idempotency_key": job.idempotency_key,
                   "failed_records": len(failed)},
        )
        return SubmitResult(
            True,
            job_id=job_id,
            status=JobStatus.QUEUED.value,
            message=f"已重新排队，将重试 {len(failed)} 条失败记录",
        )

    # ---- 执行器 ----
    def start(self) -> None:
        """启动后台调度线程并恢复未完成任务。"""
        self._storage.open()
        # 崩溃恢复：上次 RUNNING 的任务重新排队；已成功的记录保持原状，
        # 执行时只跑 PENDING 记录，既不丢也不重复跑。
        for job in self._storage.list_unfinished_jobs():
            if job.status == JobStatus.RUNNING:
                self._storage.update_job_status(job.job_id, JobStatus.QUEUED)
                logger.info(
                    "recovered running job to queued",
                    extra={"tenant_id": job.tenant_id, "job_id": job.job_id,
                           "idempotency_key": job.idempotency_key},
                )
        self._stop.clear()
        self._dispatcher = threading.Thread(
            target=self._dispatch_loop, name="batch-dispatcher", daemon=True
        )
        self._dispatcher.start()

    def stop(self) -> None:
        """停止调度并等待在途任务线程结束（优雅停机）。"""
        self._stop.set()
        if self._dispatcher is not None:
            self._dispatcher.join(timeout=5)
            self._dispatcher = None
        with self._running_lock:
            threads = list(self._job_threads)
        for t in threads:
            t.join(timeout=10)

    def _dispatch_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._dispatch_once()
            except Exception:
                logger.exception("dispatch loop error")
            self._stop.wait(0.05)

    def _dispatch_once(self) -> None:
        for job in self._storage.list_unfinished_jobs():
            if job.status != JobStatus.QUEUED:
                continue
            with self._running_lock:
                if job.job_id in self._running_jobs:
                    continue
            if not self._quota.try_acquire_slot(job.tenant_id):
                continue  # 并发槽满：保持 QUEUED，下轮再试
            with self._running_lock:
                self._running_jobs.add(job.job_id)
            self._storage.update_job_status(job.job_id, JobStatus.RUNNING)
            logger.info(
                "job started",
                extra={"tenant_id": job.tenant_id, "job_id": job.job_id,
                       "idempotency_key": job.idempotency_key},
            )
            t = threading.Thread(
                target=self._run_job, args=(job.job_id,), daemon=True
            )
            with self._running_lock:
                self._job_threads.add(t)
            t.start()

    def _run_job(self, job_id: str) -> None:
        try:
            self._run_job_inner(job_id)
        except Exception:
            logger.exception("job thread crashed", extra={"job_id": job_id})
        finally:
            with self._running_lock:
                self._job_threads.discard(threading.current_thread())

    def _run_job_inner(self, job_id: str) -> None:
        job = self._storage.get_job(job_id)
        assert job is not None
        log_ctx = {"tenant_id": job.tenant_id, "job_id": job_id,
                   "idempotency_key": job.idempotency_key}
        try:
            try:
                score_fn = self._scorers.resolve(
                    self._default_scorer, job.scorer_version
                )
            except VersionIncompatibleError as exc:
                # 执行时版本已不兼容：任务级失败，明确报错
                self._storage.update_job_status(job_id, JobStatus.FAILED)
                logger.error(
                    "job failed: scorer version incompatible at run time",
                    extra={**log_ctx, "scorer_version": job.scorer_version,
                           "error": str(exc)},
                )
                return
            for record in job.records:
                if record.status != RecordStatus.PENDING:
                    continue  # 已成功的记录不重复跑
                self._score_record(job_id, record, score_fn, log_ctx)
            final = self._storage.get_job(job_id)
            assert final is not None
            failed = [r for r in final.records
                      if r.status == RecordStatus.FAILED]
            if not failed:
                status = JobStatus.SUCCEEDED
            elif len(failed) == len(final.records):
                status = JobStatus.FAILED
            else:
                status = JobStatus.PARTIAL_FAILED
            self._storage.update_job_status(job_id, status)
            logger.info(
                "job finished",
                extra={**log_ctx, "status": status.value,
                       "failed_records": len(failed),
                       "total_records": len(final.records)},
            )
        finally:
            self._quota.release_slot(job.tenant_id)
            with self._running_lock:
                self._running_jobs.discard(job_id)

    def _score_record(
        self, job_id: str, record: ScoreRecord, score_fn: Any, log_ctx: dict
    ) -> None:
        try:
            if not isinstance(record.payload, dict):
                raise TypeError("记录 payload 必须是对象")
            record.score = float(score_fn(record.payload))
            record.status = RecordStatus.SUCCEEDED
        except TypeError as exc:
            record.status = RecordStatus.FAILED
            record.failure_reason = RecordFailureReason.INVALID_INPUT
            record.error_detail = str(exc)
        except ScorerError as exc:
            record.status = RecordStatus.FAILED
            record.failure_reason = RecordFailureReason.SCORER_ERROR
            record.error_detail = str(exc)
        except VersionIncompatibleError as exc:
            record.status = RecordStatus.FAILED
            record.failure_reason = RecordFailureReason.VERSION_INCOMPATIBLE
            record.error_detail = str(exc)
        except Exception as exc:  # 兜底：单条失败不拖垮整个任务
            record.status = RecordStatus.FAILED
            record.failure_reason = RecordFailureReason.SCORER_ERROR
            record.error_detail = f"{type(exc).__name__}: {exc}"
        self._storage.update_record(job_id, record)
        if record.status == RecordStatus.FAILED:
            logger.warning(
                "record failed",
                extra={**log_ctx, "record_id": record.record_id,
                       "failure_reason": record.failure_reason.value,
                       "error_detail": record.error_detail},
            )
