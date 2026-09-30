"""单条失败重试续跑与服务重启恢复测试。"""
import time

from app.models import JobStatus, RecordFailureReason, RecordStatus, TenantQuota
from app.quota import QuotaManager
from app.scoring import ScorerRegistry, default_scorer_v1
from app.service import BatchService
from app.storage import Storage

from conftest import wait_for_status


def test_partial_failure_marks_records_and_retry_resumes(service):
    # 一条非法记录（无数值字段）+ 两条正常记录
    r = service.submit(
        "t1", "retry-key",
        [{"x": 1.0}, {"bad": "no-numbers"}, {"x": 3.0}],
        scorer_version="1.0",
    )
    job = wait_for_status(
        service, r.job_id, {JobStatus.PARTIAL_FAILED, JobStatus.SUCCEEDED}
    )
    assert job.status == JobStatus.PARTIAL_FAILED
    failed = [rec for rec in job.records if rec.status == RecordStatus.FAILED]
    assert len(failed) == 1
    assert failed[0].failure_reason == RecordFailureReason.SCORER_ERROR
    assert failed[0].error_detail  # 有可定位的失败详情
    # 成功项已打分
    ok = [rec for rec in job.records if rec.status == RecordStatus.SUCCEEDED]
    assert len(ok) == 2

    # 修复失败项的 payload 后只重试失败项
    failed_rec = failed[0]
    failed_rec.payload = {"x": 5.0}
    service._storage.update_record(r.job_id, failed_rec)
    retry = service.retry_failed(r.job_id)
    assert retry.accepted
    job2 = wait_for_status(service, r.job_id, {JobStatus.SUCCEEDED})
    scores = {rec.record_id: rec.score for rec in job2.records}
    assert scores[failed_rec.record_id] == 5.0
    # 之前成功的记录分数未被重算覆盖（保持原值）
    assert sorted(s for rid, s in scores.items()
                  if rid != failed_rec.record_id) == [1.0, 3.0]


def test_retry_rejected_for_healthy_job(service):
    r = service.submit("t1", "healthy-key", [{"x": 1.0}], scorer_version="1.0")
    wait_for_status(service, r.job_id, {JobStatus.SUCCEEDED})
    retry = service.retry_failed(r.job_id)
    assert not retry.accepted


def test_restart_resumes_unfinished_without_loss_or_duplicate(tmp_path):
    db = str(tmp_path / "restart.db")
    started = []

    def slow_scorer(payload):
        started.append(payload["x"])
        time.sleep(0.4)
        return payload["x"]

    scorers = ScorerRegistry()
    scorers.register("default", "1.0", slow_scorer)
    storage = Storage(db)
    svc = BatchService(storage, QuotaManager(), scorers)
    svc.start()
    r = svc.submit("t1", "restart-key",
                   [{"x": 1.0}, {"x": 2.0}, {"x": 3.0}], scorer_version="1.0")
    # 等第一条跑完后立刻"崩溃"（不等待任务结束）
    deadline = time.time() + 5
    while time.time() < deadline:
        job = svc.get_job(r.job_id)
        if any(rec.status == RecordStatus.SUCCEEDED for rec in job.records):
            break
        time.sleep(0.02)
    svc.stop()  # 模拟服务停止（任务尚未跑完）
    storage.close()

    # 重启：新实例同一数据库，恢复未完成任务
    storage2 = Storage(db)
    scorers2 = ScorerRegistry()
    scorers2.register("default", "1.0", slow_scorer)
    svc2 = BatchService(storage2, QuotaManager(), scorers2)
    svc2.start()
    job = wait_for_status(svc2, r.job_id, {JobStatus.SUCCEEDED}, timeout=10)
    svc2.stop()
    storage2.close()

    assert [rec.score for rec in job.records] == [1.0, 2.0, 3.0]
    # 已成功的第一条没有被重复执行
    assert started.count(1.0) == 1
