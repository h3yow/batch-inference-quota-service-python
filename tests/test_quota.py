"""配额：提交限流、单任务记录数、并发排队、租户隔离与并发提交不踩踏。"""
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.models import Quota, RejectReason, ServiceError, TaskStatus


def test_rate_limit_rejects_with_reason(service, store, tenant):
    store.upsert_quota(Quota(tenant_id=tenant, max_submits_per_window=3, window_seconds=60))
    for i in range(3):
        service.submit(tenant, f"k{i}", [{"text": "a"}], "v1")
    with pytest.raises(ServiceError) as ei:
        service.submit(tenant, "k3", [{"text": "a"}], "v1")
    assert ei.value.reason == RejectReason.RATE_LIMITED


def test_too_many_records_rejected(service, store, tenant):
    store.upsert_quota(Quota(tenant_id=tenant, max_records_per_task=2))
    with pytest.raises(ServiceError) as ei:
        service.submit(tenant, "k1", [{"text": "a"}] * 3, "v1")
    assert ei.value.reason == RejectReason.TOO_MANY_RECORDS


def test_unknown_tenant_rejected(service):
    with pytest.raises(ServiceError) as ei:
        service.submit("ghost", "k1", [{"text": "a"}], "v1")
    assert ei.value.reason == RejectReason.UNKNOWN_TENANT


def test_concurrency_limit_queues_visibly(service, store, tenant):
    # fixture 里 max_concurrent_tasks=1
    r1 = service.submit(tenant, "k1", [{"text": "a"}], "v1")
    r2 = service.submit(tenant, "k2", [{"text": "b"}], "v1")
    assert r1.task.status == TaskStatus.PENDING
    assert r2.task.status == TaskStatus.QUEUED  # 可区分的排队状态而非拒绝
    service._worker.run_once()
    assert service.get_task(r1.task.task_id).status == TaskStatus.SUCCEEDED
    assert service.get_task(r2.task.task_id).status == TaskStatus.SUCCEEDED


def test_rate_limit_is_per_tenant(service, store, tenant):
    store.upsert_quota(Quota(tenant_id="t2", max_submits_per_window=1, window_seconds=60))
    service.submit("t2", "k1", [{"text": "a"}], "v1")
    with pytest.raises(ServiceError):
        service.submit("t2", "k2", [{"text": "a"}], "v1")
    service.submit(tenant, "k1", [{"text": "a"}], "v1")  # 别的租户不受影响


def test_concurrent_submits_respect_rate_limit(service, store, tenant):
    store.upsert_quota(Quota(tenant_id=tenant, max_submits_per_window=5, window_seconds=60))

    def submit(i):
        try:
            return service.submit(tenant, f"kc{i}", [{"text": "a"}], "v1")
        except ServiceError as e:
            return e

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(submit, range(20)))

    ok = [r for r in results if not isinstance(r, ServiceError)]
    rejected = [r for r in results if isinstance(r, ServiceError)]
    assert len(ok) == 5  # 并发下限额精确生效，不踩踏
    assert all(r.reason == RejectReason.RATE_LIMITED for r in rejected)
