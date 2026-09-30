"""幂等：重复键去重、同键不同内容拒绝、并发同键只建一个任务。"""
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.models import RejectReason, ServiceError


def test_same_key_same_content_deduplicates(service, tenant):
    r1 = service.submit(tenant, "k1", [{"text": "abc"}], "v1")
    r2 = service.submit(tenant, "k1", [{"text": "abc"}], "v1")
    assert not r1.deduplicated
    assert r2.deduplicated
    assert r1.task.task_id == r2.task.task_id


def test_same_key_different_content_rejected(service, tenant):
    service.submit(tenant, "k1", [{"text": "abc"}], "v1")
    with pytest.raises(ServiceError) as ei:
        service.submit(tenant, "k1", [{"text": "different"}], "v1")
    assert ei.value.reason == RejectReason.IDEMPOTENCY_CONFLICT


def test_same_key_different_version_rejected(service, tenant):
    service.submit(tenant, "k1", [{"text": "abc"}], "v1")
    with pytest.raises(ServiceError) as ei:
        service.submit(tenant, "k1", [{"text": "abc"}], "v2")
    assert ei.value.reason == RejectReason.IDEMPOTENCY_CONFLICT


def test_concurrent_same_key_creates_one_task(service, tenant, store):
    def submit(_):
        return service.submit(tenant, "k-race", [{"text": "abc"}], "v1")

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(submit, range(16)))

    task_ids = {r.task.task_id for r in results}
    assert len(task_ids) == 1  # 只建了一个任务，其余全部去重
    assert sum(1 for r in results if not r.deduplicated) == 1


def test_concurrent_conflicting_key_never_overwrites(service, tenant):
    original = service.submit(tenant, "k2", [{"text": "first"}], "v1")

    def submit(i):
        try:
            return service.submit(tenant, "k2", [{"text": f"v{i}"}], "v1")
        except ServiceError as e:
            return e

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(submit, range(16)))

    assert all(
        isinstance(r, ServiceError) and r.reason == RejectReason.IDEMPOTENCY_CONFLICT
        for r in results
    )
    # 原任务内容没有被后到请求覆盖
    assert service.get_task(original.task.task_id).request_hash == original.task.request_hash
