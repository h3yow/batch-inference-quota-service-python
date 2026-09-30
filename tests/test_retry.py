"""失败重试：单条失败隔离、只重试失败项、从上次进度续跑。"""
import pytest

from app.models import RecordStatus, RejectReason, ServiceError, TaskStatus


def test_single_record_failure_does_not_kill_task(service, tenant, worker):
    r = service.submit(tenant, "k1", [{"text": "abc"}, {"fail": True}, {"text": "xy"}], "v1")
    worker.run_once()
    task = service.get_task(r.task.task_id)
    assert task.status == TaskStatus.COMPLETED_WITH_FAILURES
    recs = service.get_task_view(r.task.task_id)["records"]
    assert [rec.status for rec in recs] == [
        RecordStatus.SUCCEEDED, RecordStatus.FAILED, RecordStatus.SUCCEEDED,
    ]
    assert "fail=true" in recs[1].error  # 失败原因可区分


def test_retry_only_reruns_failed_records(service, tenant, worker, counting_scorer):
    r = service.submit(
        tenant, "k1",
        [{"text": "a"}, {"fail": True}, {"text": "b"}, {"fail": True}],
        "counting",
    )
    worker.run_once()
    assert counting_scorer["n"] == 4

    # “修好”数据：把失败记录输入里的 fail 去掉（模拟上游修复后重新提交内容）
    store = service._store
    recs = store.list_records(r.task.task_id, RecordStatus.FAILED)
    for rec in recs:
        rec.input = {"text": "fixed"}
        store._db().execute(
            "UPDATE records SET input=? WHERE task_id=? AND record_index=?",
            (__import__("json").dumps(rec.input), rec.task_id, rec.record_index),
        )
    store._conn.commit()

    service.retry_failed(r.task.task_id)
    worker.run_once()

    task = service.get_task(r.task.task_id)
    assert task.status == TaskStatus.SUCCEEDED
    assert counting_scorer["n"] == 6  # 只重算了 2 条失败记录，成功记录未重跑
    recs = service.get_task_view(r.task.task_id)["records"]
    assert all(rec.status == RecordStatus.SUCCEEDED for rec in recs)
    # 成功记录的输出保持第一次的结果（前 4 次调用的输出）
    assert recs[0].output == 1.0 and recs[2].output == 3.0


def test_retry_without_failed_records_rejected(service, tenant, worker):
    r = service.submit(tenant, "k1", [{"text": "a"}], "v1")
    worker.run_once()
    with pytest.raises(ServiceError) as ei:
        service.retry_failed(r.task.task_id)
    assert ei.value.reason == RejectReason.NO_FAILED_RECORDS


def test_retry_missing_task_rejected(service):
    with pytest.raises(ServiceError) as ei:
        service.retry_failed("no-such-task")
    assert ei.value.reason == RejectReason.TASK_NOT_FOUND
