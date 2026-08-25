"""Blocking SQLite implementation; callers offload every method."""

from __future__ import annotations

import builtins
import json
import sqlite3
import uuid
from contextlib import closing
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from opai_oci_transfer.errors import sanitize_error_detail
from opai_oci_transfer.models import (
    BlobProgress,
    CopyError,
    CopyJob,
    ImageSnapshot,
    JobConflictError,
    JobNotFoundError,
    JobState,
    PruneResult,
    TerminalState,
)

TERMINAL = frozenset({"completed", "failed", "cancelled"})
ACTIVE = frozenset(
    {"queued", "planning", "copying", "retry_wait", "publishing", "verifying", "cancel_requested"}
)
JOB_COLS = "id,source,resolved_source,destination,state,completed_bytes,expected_bytes,network_bytes,completed_blobs,total_blobs,bytes_per_second,run_count,consecutive_failures,next_retry_at,last_progress_at,snapshot_digest,error_code,error_message,created_at,updated_at,started_at,completed_at,worker_id,lease_expires_at,heartbeat_at"


def now() -> datetime:
    return datetime.now(UTC)


def timestamp(value: datetime | None = None) -> str:
    return (value or now()).isoformat()


def safe_error(value: object, maximum: int = 1000) -> str:
    return sanitize_error_detail(value, maximum)


class SQLiteQueueStore:
    def __init__(self, path: Path, *, busy_timeout_ms: int = 5000) -> None:
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.busy_timeout_ms = int(busy_timeout_ms)
        self._initialize()
        self.path.chmod(0o600)

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=self.busy_timeout_ms / 1000, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        db.execute("PRAGMA foreign_keys=ON")
        return db

    def _initialize(self) -> None:
        with closing(self._connect()) as db, db:
            if str(db.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower() != "wal":
                raise RuntimeError("SQLite WAL mode is required")
            db.execute("PRAGMA synchronous=FULL")
            db.executescript("""
            CREATE TABLE IF NOT EXISTS copy_jobs (
              id TEXT PRIMARY KEY, source TEXT NOT NULL, resolved_source TEXT,
              destination TEXT NOT NULL, state TEXT NOT NULL,
              platforms_json TEXT NOT NULL, source_provider_id TEXT NOT NULL,
              destination_provider_id TEXT NOT NULL, copy_referrers INTEGER NOT NULL,
              copy_digest_tags INTEGER NOT NULL, replacement_policy TEXT NOT NULL,
              completed_bytes INTEGER NOT NULL DEFAULT 0, expected_bytes INTEGER,
              network_bytes INTEGER NOT NULL DEFAULT 0, completed_blobs INTEGER NOT NULL DEFAULT 0,
              total_blobs INTEGER, bytes_per_second INTEGER, run_count INTEGER NOT NULL DEFAULT 0,
              consecutive_failures INTEGER NOT NULL DEFAULT 0, next_retry_at TEXT,
              last_progress_at TEXT NOT NULL, snapshot_digest TEXT, snapshot_json TEXT,
              error_code TEXT, error_message TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
              started_at TEXT, completed_at TEXT, worker_id TEXT, claim_token TEXT,
              lease_expires_at TEXT, heartbeat_at TEXT, integrity_failures INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS copy_blobs (
              job_id TEXT NOT NULL REFERENCES copy_jobs(id) ON DELETE CASCADE,
              digest TEXT NOT NULL, media_type TEXT NOT NULL, size INTEGER, offset INTEGER NOT NULL DEFAULT 0,
              network_offset INTEGER NOT NULL DEFAULT 0, disposition TEXT NOT NULL DEFAULT 'pending',
              created_at TEXT NOT NULL, updated_at TEXT NOT NULL, completed_at TEXT,
              PRIMARY KEY(job_id,digest)
            );
            CREATE TABLE IF NOT EXISTS copy_errors (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              job_id TEXT NOT NULL REFERENCES copy_jobs(id) ON DELETE CASCADE,
              occurred_at TEXT NOT NULL, error_code TEXT NOT NULL, message TEXT NOT NULL,
              retryable INTEGER NOT NULL, http_status INTEGER, retry_after REAL
            );
            CREATE INDEX IF NOT EXISTS copy_jobs_claim ON copy_jobs(state,next_retry_at,created_at);
            CREATE INDEX IF NOT EXISTS copy_jobs_destination ON copy_jobs(destination,state);
            """)

    @staticmethod
    def _job(row: sqlite3.Row) -> CopyJob:
        return CopyJob(**{name: row[name] for name in CopyJob.__dataclass_fields__})

    def enqueue(self, values: dict[str, Any]) -> CopyJob:
        current, job_id = timestamp(), str(uuid.uuid4())
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute(
                "SELECT * FROM copy_jobs WHERE destination=? ORDER BY created_at DESC",
                (values["destination"],),
            ).fetchall()
            for row in rows:
                if row["state"] in ACTIVE:
                    equivalent = (
                        all(
                            row[key] == values[key]
                            for key in (
                                "source",
                                "destination",
                                "source_provider_id",
                                "destination_provider_id",
                                "copy_referrers",
                                "copy_digest_tags",
                                "replacement_policy",
                            )
                        )
                        and row["platforms_json"] == values["platforms_json"]
                    )
                    if equivalent:
                        db.commit()
                        return self._job(row)
                    raise JobConflictError(
                        f"an active copy already targets this destination: {row['id']}"
                    )
            db.execute(
                """INSERT INTO copy_jobs(
              id,source,destination,state,platforms_json,source_provider_id,destination_provider_id,
              copy_referrers,copy_digest_tags,replacement_policy,last_progress_at,created_at,updated_at)
              VALUES(?,?,?,'queued',?,?,?,?,?,?,?,?,?)""",
                (
                    job_id,
                    values["source"],
                    values["destination"],
                    values["platforms_json"],
                    values["source_provider_id"],
                    values["destination_provider_id"],
                    values["copy_referrers"],
                    values["copy_digest_tags"],
                    values["replacement_policy"],
                    current,
                    current,
                    current,
                ),
            )
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
        return self.get(job_id)

    def get(self, job_id: str) -> CopyJob:
        with closing(self._connect()) as db:
            row = db.execute(f"SELECT {JOB_COLS} FROM copy_jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise JobNotFoundError(job_id)
        return self._job(row)

    def details(self, job_id: str) -> dict[str, Any]:
        with closing(self._connect()) as db:
            row = db.execute("SELECT * FROM copy_jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise JobNotFoundError(job_id)
        return dict(row)

    def list(self, limit: int, state: JobState | None) -> list[CopyJob]:
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be 1..1000")
        sql = f"SELECT {JOB_COLS} FROM copy_jobs"
        args: tuple[Any, ...]
        if state is None:
            args = (limit,)
        else:
            sql += " WHERE state=?"
            args = (state, limit)
        sql += " ORDER BY created_at DESC LIMIT ?"
        with closing(self._connect()) as db:
            return [self._job(row) for row in db.execute(sql, args)]

    def claim(self, worker: str, lease_seconds: int) -> tuple[CopyJob, str] | None:
        current_dt, token = now(), uuid.uuid4().hex
        current = timestamp(current_dt)
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE copy_jobs SET state='cancelled',completed_at=?,updated_at=?,worker_id=NULL,claim_token=NULL,lease_expires_at=NULL WHERE state='cancel_requested' AND (worker_id IS NULL OR lease_expires_at<?)",
                (current, current, current),
            )
            row = db.execute(
                """SELECT id FROM copy_jobs WHERE state='queued'
              OR (state='retry_wait' AND next_retry_at<=?)
              OR (state IN ('planning','copying','publishing','verifying') AND lease_expires_at<?)
              ORDER BY created_at,id LIMIT 1""",
                (current, current),
            ).fetchone()
            if row is None:
                db.commit()
                return None
            db.execute(
                """UPDATE copy_jobs SET state=CASE WHEN snapshot_json IS NULL THEN 'planning' ELSE 'copying' END,
              worker_id=?,claim_token=?,lease_expires_at=?,heartbeat_at=?,run_count=run_count+1,
              started_at=COALESCE(started_at,?),updated_at=?,next_retry_at=NULL,error_code=NULL,error_message=NULL WHERE id=?""",
                (
                    worker,
                    token,
                    timestamp(current_dt + timedelta(seconds=lease_seconds)),
                    current,
                    current,
                    current,
                    row["id"],
                ),
            )
            db.commit()
            return self.get(row["id"]), token
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def heartbeat(self, job_id: str, token: str, lease_seconds: int) -> bool:
        current = now()
        with closing(self._connect()) as db, db:
            cur = db.execute(
                "UPDATE copy_jobs SET lease_expires_at=?,heartbeat_at=? WHERE id=? AND claim_token=? AND state NOT IN ('completed','failed','cancelled')",
                (
                    timestamp(current + timedelta(seconds=lease_seconds)),
                    timestamp(current),
                    job_id,
                    token,
                ),
            )
            return cur.rowcount == 1

    def save_snapshot(self, job_id: str, token: str, snapshot: ImageSnapshot) -> bool:
        current = timestamp()
        raw = json.dumps(asdict(snapshot), separators=(",", ":"), sort_keys=True)
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            cur = db.execute(
                """UPDATE copy_jobs SET resolved_source=?,snapshot_digest=?,snapshot_json=?,
              expected_bytes=?,total_blobs=?,state='copying',updated_at=? WHERE id=? AND claim_token=? AND state='planning'""",
                (
                    snapshot.resolved_source,
                    snapshot.digest,
                    raw,
                    snapshot.expected_bytes,
                    len(snapshot.blobs),
                    current,
                    job_id,
                    token,
                ),
            )
            if cur.rowcount != 1:
                db.rollback()
                return False
            for blob in snapshot.blobs:
                db.execute(
                    "INSERT INTO copy_blobs(job_id,digest,media_type,size,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                    (job_id, blob.digest, blob.media_type, blob.size, current, current),
                )
            db.commit()
            return True
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def snapshot(self, job_id: str) -> dict[str, Any] | None:
        with closing(self._connect()) as db:
            row = db.execute("SELECT snapshot_json FROM copy_jobs WHERE id=?", (job_id,)).fetchone()
        return json.loads(row[0]) if row and row[0] else None

    def progress(self, job_id: str, token: str, event: dict[str, Any]) -> bool:
        digest, current = event.get("digest"), timestamp()
        if not digest:
            phase = event.get("phase")
            if phase not in {"copying", "publishing", "verifying"}:
                return True
            with closing(self._connect()) as db, db:
                return (
                    db.execute(
                        "UPDATE copy_jobs SET state=?,updated_at=? WHERE id=? AND claim_token=?",
                        (phase, current, job_id, token),
                    ).rowcount
                    == 1
                )
        offset = max(0, int(event.get("offset", 0)))
        disposition = event.get("disposition", "transferring")
        network = max(
            0, int(event.get("network_bytes", offset if disposition == "transferring" else 0))
        )
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT offset,network_offset,size,disposition FROM copy_blobs WHERE job_id=? AND digest=?",
                (job_id, digest),
            ).fetchone()
            if row is None:
                db.rollback()
                return False
            size = row["size"]
            logical = max(row["offset"], offset)
            if disposition in {"completed", "reused", "mounted"} and size is not None:
                logical = size
            network = max(row["network_offset"], network)
            completed_at = current if disposition in {"completed", "reused", "mounted"} else None
            db.execute(
                "UPDATE copy_blobs SET offset=?,network_offset=?,disposition=?,updated_at=?,completed_at=COALESCE(completed_at,?) WHERE job_id=? AND digest=?",
                (logical, network, disposition, current, completed_at, job_id, digest),
            )
            agg = db.execute(
                "SELECT COALESCE(SUM(offset),0),COALESCE(SUM(network_offset),0),SUM(CASE WHEN disposition IN ('completed','reused','mounted') THEN 1 ELSE 0 END) FROM copy_blobs WHERE job_id=?",
                (job_id,),
            ).fetchone()
            cur = db.execute(
                "UPDATE copy_jobs SET completed_bytes=?,network_bytes=?,completed_blobs=?,bytes_per_second=?,last_progress_at=CASE WHEN ? OR ? THEN ? ELSE last_progress_at END,updated_at=? WHERE id=? AND claim_token=?",
                (
                    agg[0],
                    agg[1],
                    agg[2],
                    event.get("bytes_per_second"),
                    logical > row["offset"],
                    completed_at is not None
                    and row["disposition"] not in {"completed", "reused", "mounted"},
                    current,
                    current,
                    job_id,
                    token,
                ),
            )
            db.commit()
            return cur.rowcount == 1
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def finish(
        self,
        job_id: str,
        token: str,
        state: TerminalState,
        code: str | None = None,
        message: str | None = None,
    ) -> bool:
        current = timestamp()
        with closing(self._connect()) as db, db:
            cur = db.execute(
                "UPDATE copy_jobs SET state=?,error_code=?,error_message=?,completed_at=?,updated_at=?,worker_id=NULL,claim_token=NULL,lease_expires_at=NULL WHERE id=? AND claim_token=?",
                (
                    state,
                    code,
                    safe_error(message) if message else None,
                    current,
                    current,
                    job_id,
                    token,
                ),
            )
            return cur.rowcount == 1

    def fail_or_retry(
        self,
        job_id: str,
        token: str,
        *,
        code: str,
        message: str,
        retryable: bool,
        delay: float | None,
        http_status: int | None = None,
        integrity_failure: bool = False,
    ) -> bool:
        current = now()
        safe = safe_error(message)
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "INSERT INTO copy_errors(job_id,occurred_at,error_code,message,retryable,http_status,retry_after) VALUES(?,?,?,?,?,?,?)",
                (job_id, timestamp(current), code, safe, int(retryable), http_status, delay),
            )
            state = "retry_wait" if retryable else "failed"
            cur = db.execute(
                "UPDATE copy_jobs SET state=?,consecutive_failures=consecutive_failures+1,integrity_failures=integrity_failures+?,next_retry_at=?,error_code=?,error_message=?,completed_at=?,updated_at=?,worker_id=NULL,claim_token=NULL,lease_expires_at=NULL WHERE id=? AND claim_token=?",
                (
                    state,
                    int(integrity_failure),
                    timestamp(current + timedelta(seconds=delay or 0)) if retryable else None,
                    code,
                    safe,
                    None if retryable else timestamp(current),
                    timestamp(current),
                    job_id,
                    token,
                ),
            )
            db.commit()
            return cur.rowcount == 1
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def errors(self, job_id: str) -> builtins.list[CopyError]:
        self.get(job_id)
        with closing(self._connect()) as db:
            rows = db.execute(
                "SELECT id,job_id,occurred_at,error_code,message,retryable,http_status,retry_after FROM copy_errors WHERE job_id=? ORDER BY id",
                (job_id,),
            )
            return [
                CopyError(**(dict(row) | {"retryable": bool(row["retryable"])})) for row in rows
            ]

    def blobs(self, job_id: str) -> builtins.list[BlobProgress]:
        self.get(job_id)
        with closing(self._connect()) as db:
            rows = db.execute(
                "SELECT digest,media_type,size,offset,disposition,created_at,updated_at,completed_at FROM copy_blobs WHERE job_id=? ORDER BY digest",
                (job_id,),
            )
            return [BlobProgress(**dict(row)) for row in rows]

    def cancel(self, job_id: str) -> CopyJob:
        job = self.get(job_id)
        if job.state in TERMINAL:
            return job
        current = timestamp()
        state = "cancelled" if job.state in {"queued", "retry_wait"} else "cancel_requested"
        with closing(self._connect()) as db, db:
            db.execute(
                "UPDATE copy_jobs SET state=?,updated_at=?,completed_at=CASE WHEN ?='cancelled' THEN ? ELSE completed_at END WHERE id=?",
                (state, current, state, current, job_id),
            )
        return self.get(job_id)

    def retry(self, job_id: str) -> CopyJob:
        job = self.get(job_id)
        if job.state != "failed":
            raise JobConflictError("job is not retryable")
        with closing(self._connect()) as db, db:
            db.execute(
                "UPDATE copy_jobs SET state='queued',error_code=NULL,error_message=NULL,completed_at=NULL,next_retry_at=NULL,updated_at=? WHERE id=?",
                (timestamp(), job_id),
            )
        return self.get(job_id)

    def dismiss(self, job_id: str) -> None:
        """Delete a terminal job and all cascading transfer history."""
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT state FROM copy_jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise JobNotFoundError(job_id)
            if row["state"] not in TERMINAL:
                raise JobConflictError("only terminal copy jobs can be dismissed")
            db.execute("DELETE FROM copy_jobs WHERE id=?", (job_id,))
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def cancellation_requested(self, job_id: str, token: str) -> bool:
        with closing(self._connect()) as db:
            row = db.execute(
                "SELECT state,claim_token FROM copy_jobs WHERE id=?", (job_id,)
            ).fetchone()
        return row is None or row["claim_token"] != token or row["state"] == "cancel_requested"

    def release_claim(self, job_id: str, token: str) -> bool:
        """Release one interrupted claim without representing user cancellation."""
        current = timestamp()
        with closing(self._connect()) as db, db:
            cursor = db.execute(
                "UPDATE copy_jobs SET state=CASE WHEN snapshot_json IS NULL THEN 'queued' ELSE 'retry_wait' END,next_retry_at=?,worker_id=NULL,claim_token=NULL,lease_expires_at=NULL,heartbeat_at=NULL,updated_at=? WHERE id=? AND claim_token=? AND state IN ('planning','copying','publishing','verifying')",
                (current, current, job_id, token),
            )
        return cursor.rowcount == 1

    def release(self, worker_prefix: str) -> None:
        """Defensively release all claims owned by this manager process."""
        current = timestamp()
        with closing(self._connect()) as db, db:
            db.execute(
                "UPDATE copy_jobs SET state=CASE WHEN snapshot_json IS NULL THEN 'queued' ELSE 'retry_wait' END,next_retry_at=?,worker_id=NULL,claim_token=NULL,lease_expires_at=NULL,heartbeat_at=NULL,updated_at=? WHERE (worker_id=? OR worker_id LIKE ?) AND state IN ('planning','copying','publishing','verifying')",
                (current, current, worker_prefix, worker_prefix + "-%"),
            )

    def prune(
        self, states: tuple[TerminalState, ...] | None, older_than: str | None, dry_run: bool
    ) -> PruneResult:
        if not states and older_than is None:
            raise ValueError("prune requires states or older_than")
        if states and any(state not in TERMINAL for state in states):
            raise ValueError("prune states must be terminal")
        where: builtins.list[str] = ["state IN ('completed','failed','cancelled')"]
        args: builtins.list[str] = []
        if states:
            where.append("state IN (" + ",".join("?" for _ in states) + ")")
            args.extend(states)
        if older_than is not None:
            where.append("completed_at<?")
            args.append(older_than)
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            ids = tuple(
                row[0]
                for row in db.execute("SELECT id FROM copy_jobs WHERE " + " AND ".join(where), args)
            )
            errors = blobs = snapshots = 0
            if ids:
                marks = ",".join("?" for _ in ids)
                errors = db.execute(
                    f"SELECT COUNT(*) FROM copy_errors WHERE job_id IN ({marks})", ids
                ).fetchone()[0]
                blobs = db.execute(
                    f"SELECT COUNT(*) FROM copy_blobs WHERE job_id IN ({marks})", ids
                ).fetchone()[0]
                snapshots = db.execute(
                    f"SELECT COUNT(*) FROM copy_jobs WHERE id IN ({marks}) AND snapshot_json IS NOT NULL",
                    ids,
                ).fetchone()[0]
                if not dry_run:
                    db.execute(f"DELETE FROM copy_jobs WHERE id IN ({marks})", ids)
            db.commit()
            return PruneResult(ids, len(ids), errors, blobs, snapshots, dry_run)
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()
