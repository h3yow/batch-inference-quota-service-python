"""FastAPI HTTP 接口层。"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.models import Job
from app.service import BatchService


class SubmitRequest(BaseModel):
    tenant_id: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    records: list[dict[str, Any]]
    scorer_version: str | None = None


def _job_view(job: Job) -> dict[str, Any]:
    return {
        "job_id": job.job_id,
        "tenant_id": job.tenant_id,
        "idempotency_key": job.idempotency_key,
        "scorer_version": job.scorer_version,
        "status": job.status.value,
        "reject_reason": job.reject_reason.value if job.reject_reason else None,
        "records": [
            {
                "record_id": r.record_id,
                "status": r.status.value,
                "score": r.score,
                "failure_reason": r.failure_reason.value
                if r.failure_reason
                else None,
                "error_detail": r.error_detail,
            }
            for r in job.records
        ],
    }


def create_app(service: BatchService) -> FastAPI:
    app = FastAPI(title="batch-inference-quota-service")

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/jobs", status_code=200)
    def submit(req: SubmitRequest) -> dict[str, Any]:
        result = service.submit(
            tenant_id=req.tenant_id,
            idempotency_key=req.idempotency_key,
            records=req.records,
            scorer_version=req.scorer_version,
        )
        body = result.to_dict()
        if not result.accepted:
            # 明确拒绝：409 冲突 / 429 限流 / 403 权限 / 422 内容问题
            code = {
                "idempotency_conflict": 409,
                "rate_limited": 429,
                "record_limit_exceeded": 422,
                "tenant_disabled": 403,
                "version_incompatible": 422,
            }.get(result.reject_reason or "", 400)
            raise HTTPException(status_code=code, detail=body)
        return body

    @app.get("/v1/jobs/{job_id}")
    def get_job(job_id: str) -> dict[str, Any]:
        job = service.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return _job_view(job)

    @app.post("/v1/jobs/{job_id}/retry")
    def retry(job_id: str) -> dict[str, Any]:
        result = service.retry_failed(job_id)
        if not result.accepted:
            raise HTTPException(status_code=409, detail=result.to_dict())
        return result.to_dict()

    return app
