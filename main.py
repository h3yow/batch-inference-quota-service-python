"""本地启动入口：装配存储、配额、打分器与 HTTP 服务。"""
from __future__ import annotations

import logging

import uvicorn

from app.api import create_app
from app.models import TenantQuota
from app.quota import QuotaManager
from app.scoring import ScorerRegistry, default_scorer_v1, default_scorer_v2
from app.service import BatchService
from app.storage import Storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def build_service(db_path: str = "batch_service.db") -> BatchService:
    storage = Storage(db_path)
    quota = QuotaManager(
        {
            # 示例租户配额；未配置的租户使用默认值
            "tenant-a": TenantQuota(
                max_concurrent_jobs=2,
                max_submits_per_window=20,
                window_seconds=60.0,
                max_records_per_job=500,
            ),
            "tenant-disabled": TenantQuota(enabled=False),
        }
    )
    scorers = ScorerRegistry()
    scorers.register("default", "1.0", default_scorer_v1)
    scorers.register("default", "2.0", default_scorer_v2,
                     min_compatible_version="1.0")
    service = BatchService(storage, quota, scorers)
    service.start()
    return service


service = build_service()
app = create_app(service)

if __name__ == "__main__":
    try:
        uvicorn.run(app, host="127.0.0.1", port=8000)
    finally:
        service.stop()
