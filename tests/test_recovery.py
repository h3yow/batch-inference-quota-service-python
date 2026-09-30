"""重启恢复：已提交未跑完的任务继续跑，不丢、不重复。"""
from app.models import Quota, RecordStatus, TaskStatus
from app.quota import QuotaManager
from app.service import BatchService
from app.store import Store
from app.worker import Worker


def _make_service(db_path):
    store = Store(db_path)
    store.open()
    worker = Worker(store)
    return BatchService(store, QuotaManager(store), worker)


def test_restart_resumes_unfinished_task(tmp_path, counting_scorer):
    db = str(tmp_path / "svc.db")
    svc1 = _make_service(db)
    svc1._store.upsert_quota(Quota(tenant_id="t1", max_concurrent_tasks=5))

    # 提交任务后只手动跑“半轮”：直接处理第一条记录，模拟跑到一半崩溃
    r = svc1.submit("t1", "k1", [{"text": "a"}, {"text": "b"}, {"text": "c"}], "counting")
    store1 = svc1._store
    rec = store1.list_records(r.task.task_id, RecordStatus.PENDING)[0]
    rec.output = 42.0
    rec.status = RecordStatus.SUCCEEDED
    store1.save_record_result(rec)
    store1.update_task_status(r.task.task_id, TaskStatus.RUNNING)
    calls_before = counting_scorer["n"]
    svc1._store.close()  # 模拟进程退出（任务停在 RUNNING）

    # 重启：新实例恢复并跑完
    svc2 = _make_service(db)
    svc2.recover()
    svc2._worker.run_once()

    task = svc2.get_task(r.task.task_id)
    assert task.status == TaskStatus.SUCCEEDED
    recs = svc2.get_task_view(r.task.task_id)["records"]
    assert recs[0].output == 42.0  # 崩溃前已完成的记录没有重算
    assert all(rec.status == RecordStatus.SUCCEEDED for rec in recs)
    assert counting_scorer["n"] - calls_before == 2  # 只补跑了剩余 2 条


def test_restart_does_not_duplicate_completed_task(tmp_path):
    db = str(tmp_path / "svc.db")
    svc1 = _make_service(db)
    svc1._store.upsert_quota(Quota(tenant_id="t1"))
    r = svc1.submit("t1", "k1", [{"text": "a"}], "v1")
    svc1._worker.run_once()
    assert svc1.get_task(r.task.task_id).status == TaskStatus.SUCCEEDED
    svc1._store.close()

    svc2 = _make_service(db)
    svc2.recover()
    svc2._worker.run_once()
    task = svc2.get_task(r.task.task_id)
    assert task.status == TaskStatus.SUCCEEDED
    recs = svc2.get_task_view(r.task.task_id)["records"]
    assert len(recs) == 1 and recs[0].output == 0.01  # 结果不变，未重跑


def test_queued_task_survives_restart(tmp_path):
    db = str(tmp_path / "svc.db")
    svc1 = _make_service(db)
    svc1._store.upsert_quota(Quota(tenant_id="t1", max_concurrent_tasks=1))
    svc1.submit("t1", "k1", [{"text": "a"}], "v1")
    r2 = svc1.submit("t1", "k2", [{"text": "b"}], "v1")
    assert r2.task.status == TaskStatus.QUEUED
    svc1._store.close()

    svc2 = _make_service(db)
    svc2.recover()
    svc2._worker.run_once()
    assert svc2.get_task(r2.task.task_id).status == TaskStatus.SUCCEEDED
