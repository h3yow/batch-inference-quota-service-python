"""基于 SQLite 的本地持久化层：任务、记录、幂等键、配额计数。

所有写操作在单连接 + 事务内完成，保证并发提交时不互相踩踏；
重启后可从磁盘恢复未完成任务。
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading

from app.models import (
    Job,
    JobStatus,
    RecordFailureReason,
    RecordStatus,
    RejectReason,
    ScoreRecord,
)

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    job_id TEXT NOT NULL,
    PRIMARY KEY (tenant_id, idempotency_key)
);
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    scorer_version TEXT NOT NULL,
    status TEXT NOT NULL,
    reject_reason TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS records (
    job_id TEXT NOT NULL,
    record_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    score REAL,
    failure_reason TEXT,
    error_detail TEXT,
    PRIMARY KEY (job_id, record_id)
);
CREATE TABLE IF NOT EXISTS submit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL,
    ts REAL NOT NULL
);
"""


class Storage:
    """SQLite 持久化，线程安全（单连接 + 可重入锁串行化）。"""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._db_path = db_path
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None

    def open(self) -> None:
        with self._lock:
            if self._conn is None:
                self._conn = sqlite3.connect(
                    self._db_path, check_same_thread=False
                )
                self._conn.row_factory = sqlite3.Row
                self._conn.execute("PRAGMA journal_mode=WAL")
                self._conn.executescript(_SCHEMA)
                self._conn.commit()
                logger.info("storage opened", extra={"db_path": self._db_path})

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("storage not opened")
        return self._conn

    # ---- 幂等键 ----
    def get_fingerprint(self, tenant_id: str, idempotency_key: str) -> str | None:
        with self._lock:
            row = self._require_conn().execute(
                "SELECT fingerprint FROM idempotency_keys "
                "WHERE tenant_id=? AND idempotency_key=?",
                (tenant_id, idempotency_key),
            ).fetchone()
            return row["fingerprint"] if row else None

    def try_register_idempotency(
        self, tenant_id: str, idempotency_key: str, fingerprint: str, job_id: str
    ) -> str | None:
        """原子登记幂等键；已存在则返回已登记指纹，否则返回 None。"""
        with self._lock:
            conn = self._require_conn()
            cur = conn.execute(
                "INSERT OR IGNORE INTO idempotency_keys "
                "(tenant_id, idempotency_key, fingerprint, job_id) "
                "VALUES (?,?,?,?)",
                (tenant_id, idempotency_key, fingerprint, job_id),
            )
            conn.commit()
            if cur.rowcount == 1:
                return None
            return self.get_fingerprint(tenant_id, idempotency_key)

    def get_job_id_by_idempotency(self, tenant_id: str, idempotency_key: str) -> str | None:
        with self._lock:
            row = self._require_conn().execute(
                "SELECT job_id FROM idempotency_keys "
                "WHERE tenant_id=? AND idempotency_key=?",
                (tenant_id, idempotency_key),
            ).fetchone()
            return row["job_id"] if row else None

    # ---- 任务 ----
    def save_job(self, job: Job) -> None:
        with self._lock:
            conn = self._require_conn()
            conn.execute(
                "INSERT OR REPLACE INTO jobs "
                "(job_id, tenant_id, idempotency_key, request_fingerprint, "
                " scorer_version, status, reject_reason, created_at, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    job.job_id,
                    job.tenant_id,
                    job.idempotency_key,
                    job.request_fingerprint,
                    job.scorer_version,
                    job.status.value,
                    job.reject_reason.value if job.reject_reason else None,
                    job.created_at,
                    job.updated_at,
                ),
            )
            conn.commit()

    def update_job_status(
        self, job_id: str, status: JobStatus, reject_reason: object = None
    ) -> None:
        with self._lock:
            conn = self._require_conn()
            reason = (
                reject_reason.value if isinstance(reject_reason, RejectReason) else None
            )
            conn.execute(
                "UPDATE jobs SET status=?, reject_reason=?, "
                "updated_at=strftime('%s','now') WHERE job_id=?",
                (status.value, reason, job_id),
            )
            conn.commit()

    def get_job(self, job_id: str) -> Job | None:
        with self._lock:
            row = self._require_conn().execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if row is None:
                return None
            return self._row_to_job(row)

    def list_unfinished_jobs(self) -> list[Job]:
        with self._lock:
            rows = self._require_conn().execute(
                "SELECT * FROM jobs WHERE status IN (?,?) ORDER BY created_at",
                (JobStatus.QUEUED.value, JobStatus.RUNNING.value),
            ).fetchall()
            return [self._row_to_job(r) for r in rows]

    def _row_to_job(self, row: sqlite3.Row) -> Job:
        job = Job(
            job_id=row["job_id"],
            tenant_id=row["tenant_id"],
            idempotency_key=row["idempotency_key"],
            request_fingerprint=row["request_fingerprint"],
            scorer_version=row["scorer_version"],
            status=JobStatus(row["status"]),
            reject_reason=RejectReason(row["reject_reason"])
            if row["reject_reason"]
            else None,
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
        job.records = self.get_records(job.job_id)
        return job

    # ---- 记录 ----
    def save_records(self, job_id: str, records: list[ScoreRecord]) -> None:
        with self._lock:
            conn = self._require_conn()
            conn.executemany(
                "INSERT OR REPLACE INTO records "
                "(job_id, record_id, payload, status, score, failure_reason, "
                " error_detail) VALUES (?,?,?,?,?,?,?)",
                [
                    (
                        job_id,
                        r.record_id,
                        json.dumps(r.payload, ensure_ascii=False),
                        r.status.value,
                        r.score,
                        r.failure_reason.value if r.failure_reason else None,
                        r.error_detail,
                    )
                    for r in records
                ],
            )
            conn.commit()

    def update_record(self, job_id: str, record: ScoreRecord) -> None:
        self.save_records(job_id, [record])

    def get_records(self, job_id: str) -> list[ScoreRecord]:
        with self._lock:
            rows = self._require_conn().execute(
                "SELECT * FROM records WHERE job_id=? ORDER BY rowid", (job_id,)
            ).fetchall()
            return [
                ScoreRecord(
                    record_id=r["record_id"],
                    payload=json.loads(r["payload"]),
                    status=RecordStatus(r["status"]),
                    score=r["score"],
                    failure_reason=RecordFailureReason(r["failure_reason"])
                    if r["failure_reason"]
                    else None,
                    error_detail=r["error_detail"],
                )
                for r in rows
            ]

    # ---- 提交频率计数 ----
    def count_submits_since(self, tenant_id: str, since_ts: float) -> int:
        with self._lock:
            row = self._require_conn().execute(
                "SELECT COUNT(*) AS c FROM submit_events "
                "WHERE tenant_id=? AND ts>=?",
                (tenant_id, since_ts),
            ).fetchone()
            return int(row["c"])

    def record_submit(self, tenant_id: str, ts: float) -> None:
        with self._lock:
            conn = self._require_conn()
            conn.execute(
                "INSERT INTO submit_events (tenant_id, ts) VALUES (?,?)",
                (tenant_id, ts),
            )
            conn.commit()
