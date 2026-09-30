"""后台执行器：按租户并发额度调度任务，逐条打分并落库。"""
from __future__ import annotations

import logging
import threading

from app.store import Store

logger = logging.getLogger("batch.worker")


class Worker:
    def __init__(self, store: Store):
        self._store = store
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        raise NotImplementedError

    def stop(self) -> None:
        raise NotImplementedError

    def notify(self) -> None:
        """有新任务或额度释放时唤醒调度循环。"""
        self._wake.set()

    def run_once(self) -> None:
        """同步执行一轮调度（测试与恢复用）。"""
        raise NotImplementedError
