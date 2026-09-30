"""打分版本兼容性测试。"""
import pytest

from app.models import JobStatus, RejectReason
from app.scoring import (
    ScorerRegistry,
    VersionIncompatibleError,
    default_scorer_v1,
    default_scorer_v2,
)

from conftest import wait_for_status


def test_old_job_reproducible_at_submission_version(service):
    # 以 v1.0 提交，结论按 v1.0 复现（均值），不受当前 v2.0 影响
    r = service.submit("t1", "v-key-1", [{"x": 1.0}], scorer_version="1.0")
    job = wait_for_status(service, r.job_id, {JobStatus.SUCCEEDED})
    assert job.scorer_version == "1.0"
    assert job.records[0].score == 1.0  # v1 均值，而非 v2 sigmoid(1)


def test_default_version_is_current(service):
    r = service.submit("t1", "v-key-2", [{"x": 1.0}])
    job = wait_for_status(service, r.job_id, {JobStatus.SUCCEEDED})
    assert job.scorer_version == "2.0"
    assert 0.0 < job.records[0].score < 1.0  # sigmoid 输出


def test_unregistered_compatible_version_falls_back(service):
    # 1.5 未注册但 >= 最低兼容 1.0，回落到当前版本执行
    r = service.submit("t1", "v-key-3", [{"x": 1.0}], scorer_version="1.5")
    assert r.accepted
    job = wait_for_status(service, r.job_id, {JobStatus.SUCCEEDED})
    assert 0.0 < job.records[0].score < 1.0


def test_incompatible_version_rejected_at_submit(service):
    # 把最低兼容版本提升到 3.0：未注册的 2.5 低于下限，明确拒绝
    service._scorers.register(
        "default", "3.0", default_scorer_v2, min_compatible_version="3.0"
    )
    r = service.submit("t1", "v-key-4", [{"x": 1.0}], scorer_version="2.5")
    assert not r.accepted
    assert r.reject_reason == RejectReason.VERSION_INCOMPATIBLE.value
    # 已精确注册的老版本仍可复现，不受最低兼容版本提升影响
    r_old = service.submit("t1", "v-key-5", [{"x": 1.0}], scorer_version="1.0")
    assert r_old.accepted
    job = wait_for_status(service, r_old.job_id, {JobStatus.SUCCEEDED})
    assert job.records[0].score == 1.0


def test_unknown_scorer_version_string_rejected(service):
    r = service.submit("t1", "v-key-6", [{"x": 1.0}], scorer_version="abc")
    assert not r.accepted
    assert r.reject_reason == RejectReason.VERSION_INCOMPATIBLE.value


def test_registry_resolve_rules():
    reg = ScorerRegistry()
    reg.register("s", "1.0", default_scorer_v1)
    reg.register("s", "2.0", default_scorer_v2, min_compatible_version="1.0")
    assert reg.resolve("s", "1.0") is default_scorer_v1
    assert reg.resolve("s", "2.0") is default_scorer_v2
    assert reg.resolve("s", "1.9") is default_scorer_v2  # 回落当前版本
    with pytest.raises(VersionIncompatibleError):
        reg.resolve("s", "0.9")
    with pytest.raises(VersionIncompatibleError):
        reg.resolve("missing", "1.0")
