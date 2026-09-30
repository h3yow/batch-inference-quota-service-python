"""幂等与并发提交测试。"""
import threading

from app.models import JobStatus, RejectReason

from conftest import wait_for_status


def test_duplicate_idempotency_returns_same_job(service):
    r1 = service.submit("t1", "key-a", [{"x": 1.0}])
    r2 = service.submit("t1", "key-a", [{"x": 1.0}])
    assert r1.accepted and not r1.duplicated
    assert r2.accepted and r2.duplicated
    assert r2.job_id == r1.job_id


def test_same_key_different_content_rejected(service):
    service.submit("t1", "key-b", [{"x": 1.0}])
    r = service.submit("t1", "key-b", [{"x": 2.0}])
    assert not r.accepted
    assert r.reject_reason == RejectReason.IDEMPOTENCY_CONFLICT.value
    assert "覆盖" in r.message or "冲突" in r.message or "占用" in r.message


def test_idempotency_scoped_per_tenant(service):
    r1 = service.submit("t1", "key-c", [{"x": 1.0}])
    r2 = service.submit("t2", "key-c", [{"x": 1.0}])
    assert r1.accepted and r2.accepted
    assert r1.job_id != r2.job_id


def test_concurrent_same_key_submits_single_job(service):
    results = []
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        results.append(service.submit("t1", "key-d", [{"x": 1.0}]))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = [r for r in results if r.accepted]
    assert len(accepted) == 8
    job_ids = {r.job_id for r in accepted}
    assert len(job_ids) == 1  # 只创建了一个任务，其余为幂等命中
    assert sum(1 for r in accepted if not r.duplicated) == 1


def test_concurrent_distinct_submits_no_state_corruption(service):
    results = []
    barrier = threading.Barrier(10)

    def worker(i):
        barrier.wait()
        results.append(
            service.submit("t1", f"key-e-{i}", [{"x": float(i)}])
        )

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(r.accepted for r in results)
    assert len({r.job_id for r in results}) == 10
    for r in results:
        wait_for_status(service, r.job_id, {JobStatus.SUCCEEDED})
