"""配额检查：租户存在性（权限）、单任务记录数、提交限流、并发执行数。"""
from __future__ import annotations

import logging

from app.models import Quota, RejectReason, ServiceError
from app.store import Store

logger = logging.getLogger("batch.quota")


class QuotaManager:
    def __init__(self, store: Store):
        self._store = store

    def check_submit(self, tenant_id: str, record_count: int, now: float) -> Quota:
        """校验提交配额，超限抛带原因码的 ServiceError；通过则返回该租户配额。"""
        quota = self._store.get_quota(tenant_id)
        if quota is None:
            logger.warning("reject tenant=%s reason=%s", tenant_id, RejectReason.UNKNOWN_TENANT.value)
            raise ServiceError(
                RejectReason.UNKNOWN_TENANT,
                f"tenant {tenant_id!r} is not registered",
            )
        if record_count > quota.max_records_per_task:
            logger.warning(
                "reject tenant=%s reason=%s records=%d limit=%d",
                tenant_id, RejectReason.TOO_MANY_RECORDS.value,
                record_count, quota.max_records_per_task,
            )
            raise ServiceError(
                RejectReason.TOO_MANY_RECORDS,
                f"task has {record_count} records, limit is {quota.max_records_per_task}",
            )
        if self._store.hit_rate_limit(
            tenant_id, quota.window_seconds, quota.max_submits_per_window, now
        ):
            logger.warning(
                "reject tenant=%s reason=%s limit=%d/%gs",
                tenant_id, RejectReason.RATE_LIMITED.value,
                quota.max_submits_per_window, quota.window_seconds,
            )
            raise ServiceError(
                RejectReason.RATE_LIMITED,
                f"tenant {tenant_id!r} exceeded "
                f"{quota.max_submits_per_window} submits per {quota.window_seconds}s",
            )
        return quota

    def has_concurrency_slot(self, tenant_id: str) -> bool:
        quota = self._store.get_quota(tenant_id)
        limit = quota.max_concurrent_tasks if quota else 1
        return self._store.count_active_tasks(tenant_id) < limit
