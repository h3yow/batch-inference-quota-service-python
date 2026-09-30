import time

import pytest

from app.models import JobStatus, TenantQuota
from app.quota import QuotaManager
from app.scoring import ScorerRegistry, default_scorer_v1, default_scorer_v2
from app.service import BatchService
from app.storage import Storage


def wait_for_status(service: BatchService, job_id: str,
                    statuses: set[JobStatus], timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = service.get_job(job_id)
        if job.status in statuses:
            return job
        time.sleep(0.02)
    raise AssertionError(
        f"job {job_id} 未在 {timeout}s 内达到 {statuses}，"
        f"当前 {service.get_job(job_id).status}"
    )


@pytest.fixture()
def service(tmp_path):
    storage = Storage(str(tmp_path / "test.db"))
    scorers = ScorerRegistry()
    scorers.register("default", "1.0", default_scorer_v1)
    scorers.register("default", "2.0", default_scorer_v2,
                     min_compatible_version="1.0")
    quota = QuotaManager()
    svc = BatchService(storage, quota, scorers)
    svc.start()
    yield svc
    svc.stop()
    storage.close()
