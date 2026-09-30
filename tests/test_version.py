"""版本兼容：提交时锁定版本、结果可复现、版本不可用明确报错。"""
import pytest

from app import scorer
from app.models import RejectReason, ServiceError, TaskStatus


def test_unknown_version_rejected_at_submit(service, tenant):
    with pytest.raises(ServiceError) as ei:
        service.submit(tenant, "k1", [{"text": "a"}], "v999")
    assert ei.value.reason == RejectReason.INCOMPATIBLE_VERSION


def test_result_reproducible_by_submitted_version(service, tenant, worker):
    r1 = service.submit(tenant, "k1", [{"text": "abc"}], "v1")
    r2 = service.submit(tenant, "k2", [{"text": "abc"}], "v2")
    worker.run_once()
    out1 = service.get_task_view(r1.task.task_id)["records"][0].output
    out2 = service.get_task_view(r2.task.task_id)["records"][0].output
    assert out1 == scorer.score("v1", {"text": "abc"})
    assert out2 == scorer.score("v2", {"text": "abc"})
    assert out1 != out2  # 两个版本结果不同，各按各的版本复现


def _legacy_fn(rec):
    if isinstance(rec, dict) and rec.get("fail"):
        raise ValueError("record requested failure")
    return 1.0


def test_removed_version_fails_loudly_on_retry(service, tenant, worker):
    scorer.register("legacy", _legacy_fn)
    r = service.submit(tenant, "k1", [{"fail": True}], "legacy")
    worker.run_once()
    assert service.get_task(r.task.task_id).status == TaskStatus.COMPLETED_WITH_FAILURES

    scorer._REGISTRY.pop("legacy")  # 模拟版本下线
    with pytest.raises(ServiceError) as ei:
        service.retry_failed(r.task.task_id)
    assert ei.value.reason == RejectReason.INCOMPATIBLE_VERSION


def test_removed_version_fails_loudly_at_runtime(service, tenant, worker):
    scorer.register("legacy2", lambda rec: 1.0)
    r = service.submit(tenant, "k1", [{"text": "a"}], "legacy2")
    scorer._REGISTRY.pop("legacy2")  # 提交后、执行前版本下线
    worker.run_once()
    task = service.get_task(r.task.task_id)
    assert task.status == TaskStatus.COMPLETED_WITH_FAILURES
    assert "INCOMPATIBLE_VERSION" in task.error  # 明确报错而不是给错的结果
