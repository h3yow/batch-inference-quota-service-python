"""FastAPI 路由层：提交任务、查询任务、重试失败项。"""
from __future__ import annotations

from fastapi import FastAPI

from app.service import BatchService


def create_app(service: BatchService) -> FastAPI:
    app = FastAPI(title="batch-inference-quota-service")
    # 路由在实现阶段填充
    return app
