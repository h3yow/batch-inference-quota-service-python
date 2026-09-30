"""FastAPI 路由层：提交任务、查询任务、重试失败项、配置租户配额。"""
from __future__ import annotations

from dataclasses import asdict
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.models import Quota, RejectReason, ServiceError
from app.service import BatchService

_STATUS_CODES = {
    RejectReason.IDEMPOTENCY_CONFLICT: 409,
    RejectReason.RATE_LIMITED: 429,
    RejectReason.TOO_MANY_RECORDS: 422,
    RejectReason.UNKNOWN_TENANT: 403,
    RejectReason.INCOMPATIBLE_VERSION: 400,
    RejectReason.TASK_NOT_FOUND: 404,
    RejectReason.NO_FAILED_RECORDS: 409,
}


class SubmitRequest(BaseModel):
    tenant_id: str
    idempotency_key: str
    records: list[Any]
    scorer_version: str


class QuotaRequest(BaseModel):
    max_concurrent_tasks: int = Field(default=2, ge=1)
    max_submits_per_window: int = Field(default=10, ge=1)
    window_seconds: float = Field(default=60.0, gt=0)
    max_records_per_task: int = Field(default=100, ge=1)


def create_app(service: BatchService) -> FastAPI:
    app = FastAPI(title="batch-inference-quota-service")

    @app.exception_handler(ServiceError)
    async def service_error_handler(_: Request, exc: ServiceError) -> JSONResponse:
        return JSONResponse(
            status_code=_STATUS_CODES[exc.reason],
            content={"error": {"reason": exc.reason.value, "message": exc.message}},
        )

    @app.put("/tenants/{tenant_id}/quota")
    def put_quota(tenant_id: str, req: QuotaRequest) -> dict:
        quota = Quota(tenant_id=tenant_id, **req.model_dump())
        service._store.upsert_quota(quota)
        return asdict(quota)

    @app.post("/tasks", status_code=201)
    def submit_task(req: SubmitRequest) -> JSONResponse:
        result = service.submit(
            tenant_id=req.tenant_id,
            idempotency_key=req.idempotency_key,
            records=req.records,
            scorer_version=req.scorer_version,
        )
        body = asdict(result.task)
        body["deduplicated"] = result.deduplicated
        return JSONResponse(status_code=200 if result.deduplicated else 201, content=body)

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str) -> dict:
        view = service.get_task_view(task_id)
        return {
            "task": asdict(view["task"]),
            "records": [asdict(r) for r in view["records"]],
        }

    @app.post("/tasks/{task_id}/retry")
    def retry_task(task_id: str) -> dict:
        return asdict(service.retry_failed(task_id))

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    return app
