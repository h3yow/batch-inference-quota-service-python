"""配额限制测试：提交频率、单任务记录数、并发槽、租户开关。"""
import time

from app.models import JobStatus, RejectReason, TenantQuota

from conftest import wait_for_status


def test_rate_limit_rejects_and_recovers(service):
    service._quota.set_quota(
        "t-rate", TenantQuota(max_submits_per_window=3, window_seconds=0.3)
    )
    results = [
        service.submit("t-rate", f"rk-{i}", [{"x": 1.0}]) for i in range(4)
    ]
    assert all(r.accepted for r in results[:3])
    assert not results[3].accepted
    assert results[3].reject_reason == RejectReason.RATE_LIMITED.value
    # 窗口过后恢复
    time.sleep(0.35)
    assert service.submit("t-rate", "rk-later", [{"x": 1.0}]).accepted


def test_record_limit_exceeded(service):
    service._quota.set_quota(
        "t-rec", TenantQuota(max_records_per_job=2)
    )
    r = service.submit("t-rec", "rec-key", [{"x": 1.0}] * 3)
    assert not r.accepted
    assert r.reject_reason == RejectReason.RECORD_LIMIT_EXCEEDED.value


def test_tenant_disabled(service):
    service._quota.set_quota("t-off", TenantQuota(enabled=False))
    r = service.submit("t-off", "off-key", [{"x": 1.0}])
    assert not r.accepted
    assert r.reject_reason == RejectReason.TENANT_DISABLED.value


def test_concurrency_limit_queues_visibly(service):
    # 并发槽为 1：第二个任务应处于可区分的 QUEUED 状态而非拒绝
    service._quota.set_quota(
        "t-conc", TenantQuota(max_concurrent_jobs=1)
    )
    r1 = service.submit("t-conc", "c-1", [{"x": 1.0}], scorer_version="1.0")
    r2 = service.submit("t-conc", "c-2", [{"x": 2.0}], scorer_version="1.0")
    assert r1.accepted and r2.accepted
    assert r2.status == JobStatus.QUEUED.value  # 排队状态可区分
    j1 = wait_for_status(service, r1.job_id, {JobStatus.SUCCEEDED})
    j2 = wait_for_status(service, r2.job_id, {JobStatus.SUCCEEDED})
    assert j1.records[0].score == 1.0
    assert j2.records[0].score == 2.0


def test_rate_limit_does_not_consume_idempotency_key(service):
    service._quota.set_quota(
        "t-mix", TenantQuota(max_submits_per_window=1, window_seconds=10.0)
    )
    assert service.submit("t-mix", "mk-1", [{"x": 1.0}]).accepted
    # 被限流的提交不占用幂等键；窗口外重试同键同内容应被受理
    blocked = service.submit("t-mix", "mk-2", [{"x": 2.0}])
    assert not blocked.accepted
    service._quota.set_quota(
        "t-mix", TenantQuota(max_submits_per_window=10, window_seconds=10.0)
    )
    retry = service.submit("t-mix", "mk-2", [{"x": 2.0}])
    assert retry.accepted and not retry.duplicated
