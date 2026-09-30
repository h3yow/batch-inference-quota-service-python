import logging

import pytest

from app import scorer
from app.models import Quota
from app.quota import QuotaManager
from app.service import BatchService
from app.store import Store
from app.worker import Worker

logging.basicConfig(level=logging.DEBUG)


@pytest.fixture()
def store(tmp_path):
    s = Store(str(tmp_path / "test.db"))
    s.open()
    yield s
    s.close()


@pytest.fixture()
def tenant(store):
    store.upsert_quota(
        Quota(
            tenant_id="t1",
            max_concurrent_tasks=1,
            max_submits_per_window=100,
            window_seconds=60.0,
            max_records_per_task=100,
        )
    )
    return "t1"


@pytest.fixture()
def service(store):
    worker = Worker(store)
    return BatchService(store, QuotaManager(store), worker)


@pytest.fixture()
def worker(service):
    return service._worker


@pytest.fixture()
def counting_scorer():
    """带调用计数的打分器，用于验证“已完成记录不重算”。"""
    calls = {"n": 0}

    def fn(record):
        calls["n"] += 1
        if isinstance(record, dict) and record.get("fail"):
            raise ValueError("record requested failure")
        return float(calls["n"])

    scorer.register("counting", fn)
    yield calls
    scorer._REGISTRY.pop("counting", None)
