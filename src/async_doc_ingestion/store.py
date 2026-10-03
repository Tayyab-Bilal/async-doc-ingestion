"""SQLite persistence. Every stage reads and writes here, so any worker can resume any job.

A connection per call (WAL mode) keeps the store safe to share between threads and sweeps.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime, timedelta

from .models import NO_CONTENT, STAGES, Job, SubmitRequest

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs(
  job_id TEXT PRIMARY KEY, attachment_id TEXT UNIQUE NOT NULL, tenant_id TEXT NOT NULL,
  status TEXT NOT NULL, stage TEXT, progress TEXT NOT NULL, payload_hash TEXT NOT NULL,
  content_hash TEXT NOT NULL, ocr_job_id TEXT, attempts INTEGER NOT NULL DEFAULT 0,
  indexed INTEGER NOT NULL DEFAULT 0, note TEXT, request_json TEXT NOT NULL, sweep_token TEXT,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS blobs(attachment_id TEXT PRIMARY KEY, content BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS extracted(
  attachment_id TEXT PRIMARY KEY, text TEXT NOT NULL, pages INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS tags(
  attachment_id TEXT NOT NULL, kind TEXT NOT NULL, value TEXT NOT NULL,
  PRIMARY KEY(attachment_id, kind, value));
CREATE TABLE IF NOT EXISTS doc_meta(
  attachment_id TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
  PRIMARY KEY(attachment_id, key));
CREATE TABLE IF NOT EXISTS links(
  a TEXT NOT NULL, b TEXT NOT NULL, score REAL NOT NULL, PRIMARY KEY(a, b));
CREATE TABLE IF NOT EXISTS chat_attachments(
  attachment_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, filename TEXT NOT NULL,
  mime_type TEXT NOT NULL, text TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS job_history(
  id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL, attachment_id TEXT NOT NULL,
  event TEXT NOT NULL, payload_hash TEXT NOT NULL, source_url TEXT NOT NULL, at TEXT NOT NULL);
"""
DERIVED = ("blobs", "extracted", "tags", "doc_meta")  # rebuilt from source, safe to wipe
_UPDATABLE = {"status", "stage", "ocr_job_id", "indexed", "note", "attempts"}


def utcnow() -> datetime:
    return datetime.now(UTC)


class Store:
    def __init__(self, path: str) -> None:
        self.path = path
        with self._conn() as c:
            c.execute("PRAGMA journal_mode=WAL")
            c.executescript(SCHEMA)

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        with closing(sqlite3.connect(self.path, timeout=30)) as c:
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA synchronous=OFF")  # Simplification: no fsync keeps tests fast; production uses NORMAL or stricter
            with c:  # commit or roll back
                yield c

    # ---- jobs -------------------------------------------------------------------------------
    def _job(self, row: sqlite3.Row | None) -> Job | None:
        if row is None:
            return None
        d = dict(row)
        d["progress"] = json.loads(d["progress"])
        d["indexed"] = bool(d["indexed"])
        return Job(**{k: d[k] for k in Job.model_fields})

    def get_job(self, job_id: str) -> Job | None:
        with self._conn() as c:
            return self._job(c.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone())

    def get_job_by_attachment(self, attachment_id: str) -> Job | None:
        with self._conn() as c:
            row = c.execute("SELECT * FROM jobs WHERE attachment_id=?", (attachment_id,)).fetchone()
            return self._job(row)

    def get_request(self, attachment_id: str) -> SubmitRequest:
        with self._conn() as c:
            row = c.execute(
                "SELECT request_json FROM jobs WHERE attachment_id=?", (attachment_id,)
            ).fetchone()
        return SubmitRequest.model_validate_json(row["request_json"])

    def get_content_hash(self, attachment_id: str) -> str | None:
        with self._conn() as c:
            row = c.execute(
                "SELECT content_hash FROM jobs WHERE attachment_id=?", (attachment_id,)
            ).fetchone()
        return row["content_hash"] if row else None

    def create_job(self, req: SubmitRequest, payload_hash: str, content_hash: str) -> Job:
        job_id, now = str(uuid.uuid4()), utcnow().isoformat()
        with self._conn() as c:
            c.execute(
                "INSERT INTO jobs(job_id, attachment_id, tenant_id, status, progress, payload_hash,"
                " content_hash, request_json, created_at, updated_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?)",
                (job_id, str(req.attachment_id), req.tenant_id, "queued",
                 json.dumps(dict.fromkeys(STAGES, "pending")), payload_hash, content_hash,
                 req.model_dump_json(), now, now),
            )
            self._history(c, job_id, req, payload_hash, "created")
        return self.get_job(job_id)  # type: ignore[return-value]

    def reset_job(self, job_id: str, req: SubmitRequest, payload_hash: str, content_hash: str,
                  wipe: bool) -> None:
        """Restart a job in place. `wipe` drops derived state (and the OCR job id: new content)
        but never `job_history`, which is the audit trail of what was paid for."""
        attachment_id = str(req.attachment_id)
        with self._conn() as c:
            if wipe:
                for table in DERIVED:
                    c.execute(f"DELETE FROM {table} WHERE attachment_id=?", (attachment_id,))
                c.execute("DELETE FROM links WHERE a=? OR b=?", (attachment_id, attachment_id))
            c.execute(
                "UPDATE jobs SET status='queued', stage=NULL, progress=?, payload_hash=?,"
                " content_hash=?, request_json=?, indexed=0, attempts=0, note=NULL,"
                " sweep_token=NULL, ocr_job_id=CASE WHEN ? THEN NULL ELSE ocr_job_id END,"
                " updated_at=? WHERE job_id=?",
                (json.dumps(dict.fromkeys(STAGES, "pending")), payload_hash, content_hash,
                 req.model_dump_json(), int(wipe), utcnow().isoformat(), job_id),
            )
            self._history(c, job_id, req, payload_hash, "reingest_wipe" if wipe else "reingest")

    def update_job(self, job_id: str, **fields: object) -> None:
        assert fields.keys() <= _UPDATABLE, fields
        sets = ", ".join(f"{k}=?" for k in fields) + ", updated_at=?"
        with self._conn() as c:
            c.execute(f"UPDATE jobs SET {sets} WHERE job_id=?",
                      (*fields.values(), utcnow().isoformat(), job_id))

    def set_stage(self, job_id: str, stage: str, state: str) -> None:
        with self._conn() as c:
            row = c.execute("SELECT progress FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            progress = json.loads(row["progress"])
            progress[stage] = state
            c.execute("UPDATE jobs SET progress=?, stage=?, updated_at=? WHERE job_id=?",
                      (json.dumps(progress), stage, utcnow().isoformat(), job_id))

    def finish(self, job_id: str, status: str, note: str) -> None:
        """Terminal early exit: everything not yet done is skipped, so the poll view is honest."""
        with self._conn() as c:
            row = c.execute("SELECT progress FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            progress = {s: v if v == "done" else "skipped"
                        for s, v in json.loads(row["progress"]).items()}
            c.execute("UPDATE jobs SET status=?, note=?, progress=?, updated_at=? WHERE job_id=?",
                      (status, note, json.dumps(progress), utcnow().isoformat(), job_id))

    def _history(self, c: sqlite3.Connection, job_id: str, req: SubmitRequest, h: str,
                 event: str) -> None:
        c.execute(
            "INSERT INTO job_history(job_id, attachment_id, event, payload_hash, source_url, at)"
            " VALUES(?,?,?,?,?,?)",
            (job_id, str(req.attachment_id), event, h, req.source_url, utcnow().isoformat()),
        )

    def history(self, attachment_id: str) -> list[dict]:
        with self._conn() as c:
            rows = c.execute("SELECT * FROM job_history WHERE attachment_id=? ORDER BY id",
                             (attachment_id,)).fetchall()
        return [dict(r) for r in rows]

    # ---- derived data -----------------------------------------------------------------------
    def put_blob(self, attachment_id: str, content: bytes) -> None:
        with self._conn() as c:
            c.execute("INSERT OR REPLACE INTO blobs VALUES(?,?)", (attachment_id, content))

    def get_blob(self, attachment_id: str) -> bytes:
        with self._conn() as c:
            return c.execute("SELECT content FROM blobs WHERE attachment_id=?",
                             (attachment_id,)).fetchone()["content"]

    def put_extracted(self, attachment_id: str, text: str, pages: int) -> None:
        with self._conn() as c:
            c.execute("INSERT OR REPLACE INTO extracted VALUES(?,?,?)", (attachment_id, text, pages))

    def get_extracted(self, attachment_id: str) -> dict | None:
        with self._conn() as c:
            row = c.execute("SELECT text, pages FROM extracted WHERE attachment_id=?",
                            (attachment_id,)).fetchone()
        return dict(row) if row else None

    def replace_tags(self, attachment_id: str, kind: str, values: list[str]) -> None:
        with self._conn() as c:
            c.execute("DELETE FROM tags WHERE attachment_id=? AND kind=?", (attachment_id, kind))
            c.executemany("INSERT OR IGNORE INTO tags VALUES(?,?,?)",
                          [(attachment_id, kind, v) for v in values])

    def get_tags(self, attachment_id: str, kind: str) -> set[str]:
        with self._conn() as c:
            rows = c.execute("SELECT value FROM tags WHERE attachment_id=? AND kind=?",
                             (attachment_id, kind)).fetchall()
        return {r["value"] for r in rows}

    def put_meta(self, attachment_id: str, key: str, value: object) -> None:
        with self._conn() as c:
            c.execute("INSERT OR REPLACE INTO doc_meta VALUES(?,?,?)",
                      (attachment_id, key, json.dumps(value, default=str)))

    def get_meta(self, attachment_id: str, key: str) -> object | None:
        with self._conn() as c:
            row = c.execute("SELECT value FROM doc_meta WHERE attachment_id=? AND key=?",
                            (attachment_id, key)).fetchone()
        return json.loads(row["value"]) if row else None

    def tenant_attachments(self, tenant_id: str, exclude: str) -> list[str]:
        """Link candidates: same tenant only, and only docs that got as far as keywords."""
        with self._conn() as c:
            rows = c.execute(
                "SELECT DISTINCT j.attachment_id FROM jobs j JOIN tags t USING(attachment_id)"
                " WHERE j.tenant_id=? AND j.attachment_id!=? AND t.kind='keyword'",
                (tenant_id, exclude)).fetchall()
        return [r["attachment_id"] for r in rows]

    def replace_links(self, attachment_id: str, links: list[tuple[str, float]]) -> None:
        with self._conn() as c:
            c.execute("DELETE FROM links WHERE a=? OR b=?", (attachment_id, attachment_id))
            for other, score in links:
                c.executemany("INSERT INTO links VALUES(?,?,?)",
                              [(attachment_id, other, score), (other, attachment_id, score)])

    def get_links(self, attachment_id: str) -> dict[str, float]:
        with self._conn() as c:
            rows = c.execute("SELECT b, score FROM links WHERE a=?", (attachment_id,)).fetchall()
        return {r["b"]: r["score"] for r in rows}

    def delete_attachment(self, attachment_id: str) -> dict[str, int]:
        """Remove every row for an attachment (history included) and report what went."""
        counts: dict[str, int] = {}
        with self._conn() as c:
            for table in (*DERIVED, "jobs", "job_history"):
                counts[table] = c.execute(f"DELETE FROM {table} WHERE attachment_id=?",
                                          (attachment_id,)).rowcount
            counts["links"] = c.execute("DELETE FROM links WHERE a=? OR b=?",
                                        (attachment_id, attachment_id)).rowcount
        return counts

    # ---- chat attachments (extracted once; queries read this table, never the file) -------------
    def put_chat(self, tenant_id: str, attachment_id: str, filename: str, mime_type: str,
                 text: str, created_at: datetime | None = None) -> None:
        with self._conn() as c:
            c.execute("INSERT OR REPLACE INTO chat_attachments VALUES(?,?,?,?,?,?)",
                      (attachment_id, tenant_id, filename, mime_type, text,
                       (created_at or utcnow()).isoformat()))

    def get_chat(self, tenant_id: str, attachment_id: str) -> dict | None:
        """Tenant-scoped: another tenant's attachment id is simply not found."""
        with self._conn() as c:
            row = c.execute("SELECT * FROM chat_attachments WHERE attachment_id=? AND tenant_id=?",
                            (attachment_id, tenant_id)).fetchone()
        return dict(row) if row else None

    def expired_chat(self, cutoff: datetime) -> list[dict]:
        with self._conn() as c:
            rows = c.execute("SELECT attachment_id, tenant_id FROM chat_attachments"
                             " WHERE created_at<?", (cutoff.isoformat(),)).fetchall()
        return [dict(r) for r in rows]

    def delete_chat(self, attachment_id: str) -> int:
        with self._conn() as c:
            return c.execute("DELETE FROM chat_attachments WHERE attachment_id=?",
                             (attachment_id,)).rowcount

    # ---- sweep claim ------------------------------------------------------------------------
    def claim_unindexed(self, now: datetime, min_age: timedelta, max_attempts: int,
                        batch: int) -> list[str]:
        """Atomically claim completed-but-unindexed jobs for one sweeper.

        One UPDATE both selects and marks rows, so two racing sweeps can never claim the same job
        (SQLite serialises writers). Attempts are bumped here, at claim time, so a sweeper that
        dies mid-way still counts against the cap. Postgres would use
        `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING id`.
        """
        token = uuid.uuid4().hex
        with self._conn() as c:
            c.execute(
                "UPDATE jobs SET sweep_token=?, attempts=attempts+1 WHERE sweep_token IS NULL"
                " AND job_id IN (SELECT job_id FROM jobs WHERE status='completed' AND indexed=0"
                "  AND sweep_token IS NULL AND IFNULL(note,'')!=? AND updated_at<=?"
                "  AND attempts<? ORDER BY created_at LIMIT ?)",
                (token, NO_CONTENT, (now - min_age).isoformat(), max_attempts, batch),
            )
            rows = c.execute("SELECT job_id FROM jobs WHERE sweep_token=?", (token,)).fetchall()
        return [r["job_id"] for r in rows]

    def release_claims(self, job_ids: list[str]) -> None:
        with self._conn() as c:
            c.executemany("UPDATE jobs SET sweep_token=NULL WHERE job_id=?",
                          [(j,) for j in job_ids])
