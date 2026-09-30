"""租户配额管理：并发槽位、提交频率、单任务记录数、租户开关。"""
from __future__ import annotations

import logging
import threading

from app.models import TenantQuota

logger = logging.getLogger(__name__)

_DEFAULT_QUOTA = TenantQuota()


class QuotaManager:
    """内存态并发槽计数（频率计数由服务层经持久层完成）。"""

    def __init__(self, quotas: dict[str, TenantQuota] | None = None) -> None:
        self._quotas: dict[str, TenantQuota] = dict(quotas or {})
        self._lock = threading.Lock()
        self._running: dict[str, int] = {}

    def set_quota(self, tenant_id: str, quota: TenantQuota) -> None:
        with self._lock:
            self._quotas[tenant_id] = quota

    def get_quota(self, tenant_id: str) -> TenantQuota:
        with self._lock:
            return self._quotas.get(tenant_id, _DEFAULT_QUOTA)

    def try_acquire_slot(self, tenant_id: str) -> bool:
        """尝试占用一个并发执行槽，成功返回 True。"""
        with self._lock:
            quota = self._quotas.get(tenant_id, _DEFAULT_QUOTA)
            running = self._running.get(tenant_id, 0)
            if running >= quota.max_concurrent_jobs:
                logger.info(
                    "concurrency slot denied",
                    extra={"tenant_id": tenant_id, "running": running,
                           "max": quota.max_concurrent_jobs},
                )
                return False
            self._running[tenant_id] = running + 1
            return True

    def release_slot(self, tenant_id: str) -> None:
        with self._lock:
            running = self._running.get(tenant_id, 0)
            self._running[tenant_id] = max(0, running - 1)

    def running_count(self, tenant_id: str) -> int:
        with self._lock:
            return self._running.get(tenant_id, 0)
