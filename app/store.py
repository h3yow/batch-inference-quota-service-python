"""SQLite 持久化层：任务、记录、幂等键、限流事件的存取。

所有写操作在单连接 + RLock + 事务内完成，保证多线程并发提交不互相踩踏；
服务重启后从库中恢复未完成任务，已完成记录不会重跑。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time

from app.models import Quota, Record, RecordStatus, Task, TaskStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS quotas (
    tenant_id TEXT PRIMARY KEY,
    max_concurrent_tasks INTEGER NOT NULL,
    max_submits_per_window INTEGER NOT NULL,
    window_seconds REAL NOT NULL,
    max_records_per_task INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    scorer_version TEXT NOT NULL,
    status TEXT NOT NULL,
    total_records INTEGER NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    error TEXT,
    UNIQUE (tenant_id, idempotency_key)
);
CREATE TABLE IF NOT EXISTS records (
    task_id TEXT NOT NULL,
    record_index INTEGER NOT NULL,
    input TEXT NOT NULL,
    status TEXT NOT NULL,
    output TEXT,
    error TEXT,
    PRIMARY KEY (task_id, record_index)
);
CREATE TABLE IF NOT EXISTS rate_events (
    tenant_id TEXT NOT NULL,
    ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rate_events ON rate_events (tenant_id, ts);
"""


class Store:
    def __init__(self, db_path: str = ":memory:"):
        self._db_path = db_path
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None

    # ---- 生命周期 ----
    def open(self) -> None:
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def _db(self) -> sqlite3.Connection:
        assert self._conn is not None, "Store.open() must be called first"
        return self._conn

    # ---- 租户配额 ----
    def upsert_quota(self, quota: Quota) -> None:
        with self._lock, self._db():
            self._db().execute(
                "INSERT OR REPLACE INTO quotas VALUES (?,?,?,?,?)",
                (
                    quota.tenant_id,
                    quota.max_concurrent_tasks,
                    quota.max_submits_per_window,
                    quota.window_seconds,
                    quota.max_records_per_task,
                ),
            )

    def get_quota(self, tenant_id: str) -> Quota | None:
        with self._lock:
            row = self._db().execute(
                "SELECT * FROM quotas WHERE tenant_id=?", (tenant_id,)
            ).fetchone()
        if row is None:
            return None
        return Quota(
            tenant_id=row["tenant_id"],
            max_concurrent_tasks=row["max_concurrent_tasks"],
            max_submits_per_window=row["max_submits_per_window"],
            window_seconds=row["window_seconds"],
            max_records_per_task=row["max_records_per_task"],
        )

    # ---- 任务与幂等 ----
    def create_task_if_absent(
        self, task: Task, records: list[Record]
    ) -> tuple[Task, bool]:
        """按 (tenant_id, idempotency_key) 幂等建任务，同事务写入记录。

        键已存在时返回已有已有的任务，绝不覆盖。
        """
        with self._lock, self._db():
            cur = self._db().execute(
                """INSERT OR IGNORE INTO tasks
                   (task_id, tenant_id, idempotency_key, request_hash,
                    scorer_version, status, total_records, created_at, updated_at, error)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    task.task_id,
                    task.tenant_id,
                    task.idempotency_key,
                    task.request_hash,
                    task.scorer_version,
                    task.status.value,
                    task.total_records,
                    task.created_at,
                    task.updated_at,
                    task.error,
                ),
            )
            created = cur.rowcount == 1
            if created:
                self._db().executemany(
                    "INSERT INTO records (task_id, record_index, input, status) VALUES (?,?,?,?)",
                    [
                        (r.task_id, r.record_index, json.dumps(r.input), r.status.value)
                        for r in records
                    ],
                )
            row = self._db().execute(
                "SELECT * FROM tasks WHERE tenant_id=? AND idempotency_key=?",
                (task.tenant_id, task.idempotency_key),
            ).fetchone()
        return self._row_to_task(row), created

    def get_task(self, task_id: str) -> Task | None:
        with self._lock:
            row = self._db().execute(
                "SELECT * FROM tasks WHERE task_id=?", (task_id,)
            ).fetchone()
        return self._row_to_task(row) if row else None

    def get_task_by_key(self, tenant_id: str, idempotency_key: str) -> Task | None:
        with self._lock:
            row = self._db().execute(
                "SELECT * FROM tasks WHERE tenant_id=? AND idempotency_key=?",
                (tenant_id, idempotency_key),
            ).fetchone()
        return self._row_to_task(row) if row else None

    def update_task_status(self, task_id: str, status: TaskStatus, error: str | None = None) -> None:
        with self._lock, self._db():
            self._db().execute(
                "UPDATE tasks SET status=?, error=?, updated_at=? WHERE task_id=?",
                (status.value, error, time.time(), task_id),
            )

    def list_runnable_tasks(self) -> list[Task]:
        """重启恢复用：PENDING/RUNNING 的任务都需要（重新）调度。"""
        with self._lock:
            rows = self._db().execute(
                "SELECT * FROM tasks WHERE status IN (?,?) ORDER BY created_at",
                (TaskStatus.PENDING.value, TaskStatus.RUNNING.value),
            ).fetchall()
        return [self._row_to_task(r) for r in rows]

    def count_active_tasks(self, tenant_id: str) -> int:
        """在途任务数（PENDING + RUNNING），用于并发额度判断。"""
        with self._lock:
            row = self._db().execute(
                "SELECT COUNT(*) c FROM tasks WHERE tenant_id=? AND status IN (?,?)",
                (tenant_id, TaskStatus.PENDING.value, TaskStatus.RUNNING.value),
            ).fetchone()
        return int(row["c"])

    def list_queued_tasks(self, tenant_id: str) -> list[Task]:
        with self._lock:
            rows = self._db().execute(
                "SELECT * FROM tasks WHERE tenant_id=? AND status=? ORDER BY created_at",
                (tenant_id, TaskStatus.QUEUED.value),
            ).fetchall()
        return [self._row_to_task(r) for r in rows]

    def promote_queued_tasks(self) -> list[Task]:
        """把有空闲并发额度的 QUEUED 任务提升为 PENDING，返回被提升的任务。"""
        promoted: list[Task] = []
        with self._lock, self._db():
            rows = self._db().execute(
                "SELECT * FROM tasks WHERE status=? ORDER BY created_at",
                (TaskStatus.QUEUED.value,),
            ).fetchall()
            for row in rows:
                task = self._row_to_task(row)
                quota = self.get_quota(task.tenant_id)
                limit = quota.max_concurrent_tasks if quota else 1
                active = self.count_active_tasks(task.tenant_id)
                if active < limit:
                    self._db().execute(
                        "UPDATE tasks SET status=?, updated_at=? WHERE task_id=? AND status=?",
                        (TaskStatus.PENDING.value, time.time(), task.task_id,
                         TaskStatus.QUEUED.value),
                    )
                    task.status = TaskStatus.PENDING
                    promoted.append(task)
        return promoted

    # ---- 记录 ----
    def list_records(self, task_id: str, status: RecordStatus | None = None) -> list[Record]:
        with self._lock:
            if status is None:
                rows = self._db().execute(
                    "SELECT * FROM records WHERE task_id=? ORDER BY record_index",
                    (task_id,),
                ).fetchall()
            else:
                rows = self._db().execute(
                    "SELECT * FROM records WHERE task_id=? AND status=? ORDER BY record_index",
                    (task_id, status.value),
                ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def save_record_result(self, record: Record) -> None:
        with self._lock, self._db():
            self._db().execute(
                "UPDATE records SET status=?, output=?, error=? "
                "WHERE task_id=? AND record_index=?",
                (
                    record.status.value,
                    json.dumps(record.output) if record.output is not None else None,
                    record.error,
                    record.task_id,
                    record.record_index,
                ),
            )

    def reset_failed_records(self, task_id: str) -> int:
        with self._lock, self._db():
            cur = self._db().execute(
                "UPDATE records SET status=?, error=NULL WHERE task_id=? AND status=?",
                (RecordStatus.PENDING.value, task_id, RecordStatus.FAILED.value),
            )
            return cur.rowcount

    # ---- 提交限流（滑动窗口，事务内计数，并发安全）----
    def hit_rate_limit(self, tenant_id: str, window_seconds: float, limit: int, now: float) -> bool:
        with self._lock, self._db():
            self._db().execute(
                "DELETE FROM rate_events WHERE tenant_id=? AND ts<?",
                (tenant_id, now - window_seconds),
            )
            row = self._db().execute(
                "SELECT COUNT(*) c FROM rate_events WHERE tenant_id=?", (tenant_id,)
            ).fetchone()
            if int(row["c"]) >= limit:
                return True
            self._db().execute(
                "INSERT INTO rate_events (tenant_id, ts) VALUES (?,?)", (tenant_id, now)
            )
            return False

    # ---- 行转换 ----
    @staticmethod
    def _row_to_task(row: sqlite3.Row) -> Task:
        return Task(
            task_id=row["task_id"],
            tenant_id=row["tenant_id"],
            idempotency_key=row["idempotency_key"],
            request_hash=row["request_hash"],
            scorer_version=row["scorer_version"],
            status=TaskStatus(row["status"]),
            total_records=row["total_records"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            error=row["error"],
        )

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> Record:
        return Record(
            task_id=row["task_id"],
            record_index=row["record_index"],
            input=json.loads(row["input"]),
            status=RecordStatus(row["status"]),
            output=json.loads(row["output"]) if row["output"] is not None else None,
            error=row["error"],
        )
