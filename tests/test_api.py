"""HTTP 端到端：提交/查询/重试/配额配置，错误码与原因可区分。"""
import pytest
from fastapi.testclient import TestClient

from app.api import create_app


@pytest.fixture()
def client(service, store):
    return TestClient(create_app(service))


def test_end_to_end(client, service, worker):
    assert client.put("/tenants/t1/quota", json={"max_concurrent_tasks": 2}).status_code == 200

    resp = client.post("/tasks", json={
        "tenant_id": "t1", "idempotency_key": "k1",
        "records": [{"text": "abc"}, {"fail": True}], "scorer_version": "v1",
    })
    assert resp.status_code == 201
    task_id = resp.json()["task_id"]

    # 幂等重放：同键同内容返回 200 + 同一任务
    again = client.post("/tasks", json={
        "tenant_id": "t1", "idempotency_key": "k1",
        "records": [{"text": "abc"}, {"fail": True}], "scorer_version": "v1",
    })
    assert again.status_code == 200 and again.json()["task_id"] == task_id

    # 同键不同内容：409 + IDEMPOTENCY_CONFLICT
    conflict = client.post("/tasks", json={
        "tenant_id": "t1", "idempotency_key": "k1",
        "records": [{"text": "changed"}], "scorer_version": "v1",
    })
    assert conflict.status_code == 409
    assert conflict.json()["error"]["reason"] == "IDEMPOTENCY_CONFLICT"

    worker.run_once()
    view = client.get(f"/tasks/{task_id}").json()
    assert view["task"]["status"] == "COMPLETED_WITH_FAILURES"
    assert view["records"][1]["status"] == "FAILED"
    assert "fail=true" in view["records"][1]["error"]


def test_error_codes(client, store):
    from app.models import Quota
    store.upsert_quota(Quota(tenant_id="t9", max_records_per_task=1, max_submits_per_window=1))

    r = client.post("/tasks", json={
        "tenant_id": "ghost", "idempotency_key": "k",
        "records": [], "scorer_version": "v1",
    })
    assert r.status_code == 403 and r.json()["error"]["reason"] == "UNKNOWN_TENANT"

    r = client.post("/tasks", json={
        "tenant_id": "t9", "idempotency_key": "k",
        "records": [{"text": "a"}, {"text": "b"}], "scorer_version": "v1",
    })
    assert r.status_code == 422 and r.json()["error"]["reason"] == "TOO_MANY_RECORDS"

    ok = client.post("/tasks", json={
        "tenant_id": "t9", "idempotency_key": "k1",
        "records": [{"text": "a"}], "scorer_version": "v1",
    })
    assert ok.status_code == 201
    r = client.post("/tasks", json={
        "tenant_id": "t9", "idempotency_key": "k2",
        "records": [{"text": "a"}], "scorer_version": "v1",
    })
    assert r.status_code == 429 and r.json()["error"]["reason"] == "RATE_LIMITED"

    r = client.post("/tasks", json={
        "tenant_id": "t9", "idempotency_key": "k3",
        "records": [{"text": "a"}], "scorer_version": "vX",
    })
    assert r.status_code == 400 and r.json()["error"]["reason"] == "INCOMPATIBLE_VERSION"

    assert client.get("/tasks/nope").status_code == 404
