"""HTTP 接口层测试。"""
import pytest
from fastapi.testclient import TestClient

from app.api import create_app


@pytest.fixture()
def client(service):
    return TestClient(create_app(service))


def test_submit_and_query_job(client):
    resp = client.post("/v1/jobs", json={
        "tenant_id": "t1", "idempotency_key": "http-1",
        "records": [{"x": 2.0}], "scorer_version": "1.0",
    })
    assert resp.status_code == 200
    job_id = resp.json()["job_id"]
    job = client.get(f"/v1/jobs/{job_id}").json()
    assert job["tenant_id"] == "t1"
    assert job["idempotency_key"] == "http-1"


def test_conflict_returns_409(client):
    body = {"tenant_id": "t1", "idempotency_key": "http-2",
            "records": [{"x": 1.0}]}
    assert client.post("/v1/jobs", json=body).status_code == 200
    body["records"] = [{"x": 9.0}]
    resp = client.post("/v1/jobs", json=body)
    assert resp.status_code == 409
    assert resp.json()["detail"]["reject_reason"] == "idempotency_conflict"


def test_missing_job_404(client):
    assert client.get("/v1/jobs/nope").status_code == 404
