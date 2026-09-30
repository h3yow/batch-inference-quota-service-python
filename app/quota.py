"""配额检查：并发执行数、提交限流、单任务记录数。"""
from __future__ import annotations

from app.models import Quota
from app.store import Store

DEFAULT_QUOTA = Quota(tenant_id="__default__")


class QuotaManager:
    def __init__(self, store: Store):
        self._store = store

    def check_submit(self, tenant_id: str, record_count: int, now: float) -> Quota:
        """校验提交配额，超限抛 ServiceError；通过则返回该租户配额。"""
        raise NotImplementedError

    def has_concurrency_slot(self, tenant_id: str) -> bool:
        raise NotImplementedError
