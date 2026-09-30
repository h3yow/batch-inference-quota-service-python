"""本地启动入口：python main.py"""
import logging

import uvicorn

from app.api import create_app
from app.quota import QuotaManager
from app.service import BatchService
from app.store import Store
from app.worker import Worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def build_service(db_path: str = "batch_service.db") -> BatchService:
    store = Store(db_path)
    store.open()
    quota = QuotaManager(store)
    worker = Worker(store)
    service = BatchService(store, quota, worker)
    service.recover()
    worker.start()
    return service


app = create_app(build_service())

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
