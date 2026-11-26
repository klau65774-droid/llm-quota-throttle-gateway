"""SQLite WAL state persistence for quota reservations and OTLP trace runs."""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

USD_SCALE = 100_000_000
_RESERVATION_TABLE = """
    CREATE TABLE IF NOT EXISTS reservations (
        reservation_id TEXT PRIMARY KEY,
        trace_id TEXT NOT NULL,
        tenant_id TEXT NOT NULL,
        model TEXT NOT NULL,
        status TEXT NOT NULL,
        reserved_tokens INTEGER NOT NULL,
        estimated_prompt_tokens INTEGER NOT NULL,
        max_completion_tokens INTEGER NOT NULL,
        actual_tokens INTEGER,
        refunded_tokens INTEGER NOT NULL DEFAULT 0,
        estimated_cost_usd REAL NOT NULL,
        actual_cost_usd REAL,
        remaining_rpm INTEGER NOT NULL,
        remaining_tpm INTEGER NOT NULL,
        provider_used TEXT NOT NULL,
        redaction_hits INTEGER NOT NULL DEFAULT 0,
        idempotency_key TEXT,
        created_at_ms INTEGER NOT NULL,
        settled_at_ms INTEGER,
        tpm_hold TEXT,
        UNIQUE(tenant_id, idempotency_key)
    )
"""


def idempotency_fingerprint(key: str) -> str:
    """Persist a stable fingerprint instead of caller-supplied key text."""
    return sha256(key.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class StoredReservation:
    """Persisted reservation ledger row."""

    reservation_id: str
    trace_id: str
    tenant_id: str
    model: str
    status: str
    reserved_tokens: int
    estimated_prompt_tokens: int
    max_completion_tokens: int
    actual_tokens: int | None
    refunded_tokens: int
    estimated_cost_usd: float
    actual_cost_usd: float | None
    remaining_rpm: int
    remaining_tpm: int
    provider_used: str
    redaction_hits: int
    idempotency_key: str | None
    created_at_ms: int
    settled_at_ms: int | None
    tpm_hold: str | None = None


class GatewayStorage:
    """Thread-safe SQLite WAL persistence backend."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self._db_path), timeout=5, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        try:
            self._init_schema()
        except BaseException:
            self._conn.close()
            raise

    def _init_schema(self) -> None:
        journal = self._conn.execute("PRAGMA journal_mode=WAL").fetchone()
        if journal is None or journal[0] != "wal":
            raise RuntimeError("SQLite WAL mode is required")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self.write_transaction():
            version = self._conn.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1, 2):
                raise RuntimeError("Unsupported SQLite schema version")
            legacy = (
                version == 0
                and self._conn.execute(
                    "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'reservations'"
                ).fetchone()
                is not None
            )
            if legacy:
                self._conn.execute("ALTER TABLE reservations RENAME TO legacy_reservations")
            self._conn.execute(_RESERVATION_TABLE)
            if legacy:
                self._conn.create_function("idempotency_fingerprint", 1, idempotency_fingerprint)
                self._conn.execute(
                    """
                    INSERT INTO reservations
                    SELECT reservation_id, trace_id, tenant_id, model, status, reserved_tokens,
                        estimated_prompt_tokens, max_completion_tokens, actual_tokens,
                        refunded_tokens, estimated_cost_usd, actual_cost_usd, remaining_rpm,
                        remaining_tpm, provider_used, redaction_hits,
                        CASE WHEN idempotency_key IS NOT NULL AND idempotency_key <> ''
                             THEN idempotency_fingerprint(idempotency_key) END,
                        created_at_ms, settled_at_ms, NULL
                    FROM legacy_reservations
                    """
                )
                self._conn.execute("DROP TABLE legacy_reservations")
            elif version == 1:
                self._conn.execute("ALTER TABLE reservations ADD COLUMN tpm_hold TEXT")
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_reservations_tenant_status
                ON reservations (tenant_id, status, created_at_ms DESC)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_reservations_tenant_time
                ON reservations (tenant_id, created_at_ms DESC)
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trace_runs (
                    run_id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL
                )
                """
            )
            self._conn.execute("PRAGMA user_version=2")

    @contextmanager
    def write_transaction(self) -> Iterator[None]:
        """Serialize the full transaction and release the WAL writer on failure."""
        with self._lock:
            if self._conn.in_transaction:
                yield
                return
            with self._conn:
                self._conn.execute("BEGIN IMMEDIATE")
                yield

    def close(self) -> None:
        """Close underlying SQLite connection."""
        with self._lock:
            self._conn.close()

    @staticmethod
    def _row_to_reservation(row: sqlite3.Row) -> StoredReservation:
        return StoredReservation(
            reservation_id=str(row["reservation_id"]),
            trace_id=str(row["trace_id"]),
            tenant_id=str(row["tenant_id"]),
            model=str(row["model"]),
            status=str(row["status"]),
            reserved_tokens=int(row["reserved_tokens"]),
            estimated_prompt_tokens=int(row["estimated_prompt_tokens"]),
            max_completion_tokens=int(row["max_completion_tokens"]),
            actual_tokens=int(row["actual_tokens"]) if row["actual_tokens"] is not None else None,
            refunded_tokens=int(row["refunded_tokens"]),
            estimated_cost_usd=float(row["estimated_cost_usd"]),
            actual_cost_usd=(
                float(row["actual_cost_usd"]) if row["actual_cost_usd"] is not None else None
            ),
            remaining_rpm=int(row["remaining_rpm"]),
            remaining_tpm=int(row["remaining_tpm"]),
            provider_used=str(row["provider_used"]),
            redaction_hits=int(row["redaction_hits"]),
            idempotency_key=(
                str(row["idempotency_key"]) if row["idempotency_key"] is not None else None
            ),
            created_at_ms=int(row["created_at_ms"]),
            settled_at_ms=(int(row["settled_at_ms"]) if row["settled_at_ms"] is not None else None),
            tpm_hold=str(row["tpm_hold"]) if row["tpm_hold"] is not None else None,
        )

    def get_by_idempotency_key(self, tenant_id: str, fingerprint: str) -> StoredReservation | None:
        """Fetch a reservation by tenant and key fingerprint."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM reservations WHERE tenant_id = ? AND idempotency_key = ?",
                (tenant_id, fingerprint),
            ).fetchone()
            return self._row_to_reservation(row) if row is not None else None

    def tenant_spend_units(self, tenant_id: str, start_ms: int, end_ms: int) -> int:
        """Read estimated or finalized charges for the selected tenant budget window."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT COALESCE(SUM(CAST(ROUND(
                    COALESCE(actual_cost_usd, estimated_cost_usd) * ?
                ) AS INTEGER)), 0)
                FROM reservations WHERE tenant_id = ? AND created_at_ms >= ? AND created_at_ms < ?
                """,
                (USD_SCALE, tenant_id, start_ms, end_ms),
            ).fetchone()
            return int(row[0])

    def create_reservation(
        self, record: StoredReservation, *, trace_payload: dict[str, Any] | None = None
    ) -> StoredReservation:
        """Insert a new reservation row atomically."""
        with self.write_transaction():
            self._conn.execute(
                """
                INSERT INTO reservations (
                    reservation_id, trace_id, tenant_id, model, status,
                    reserved_tokens, estimated_prompt_tokens, max_completion_tokens,
                    actual_tokens, refunded_tokens, estimated_cost_usd, actual_cost_usd,
                    remaining_rpm, remaining_tpm, provider_used, redaction_hits,
                    idempotency_key, created_at_ms, settled_at_ms, tpm_hold
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.reservation_id,
                    record.trace_id,
                    record.tenant_id,
                    record.model,
                    record.status,
                    record.reserved_tokens,
                    record.estimated_prompt_tokens,
                    record.max_completion_tokens,
                    record.actual_tokens,
                    record.refunded_tokens,
                    record.estimated_cost_usd,
                    record.actual_cost_usd,
                    record.remaining_rpm,
                    record.remaining_tpm,
                    record.provider_used,
                    record.redaction_hits,
                    record.idempotency_key,
                    record.created_at_ms,
                    record.settled_at_ms,
                    record.tpm_hold,
                ),
            )
            if trace_payload is not None:
                self.save_trace_run(
                    run_id=record.reservation_id,
                    trace_id=record.trace_id,
                    payload=trace_payload,
                    created_at_ms=record.created_at_ms,
                )
            return record

    def get_reservation(self, reservation_id: str) -> StoredReservation | None:
        """Fetch reservation by primary ID."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM reservations WHERE reservation_id = ?",
                (reservation_id,),
            ).fetchone()
            return self._row_to_reservation(row) if row is not None else None

    def update_settlement(
        self,
        *,
        reservation_id: str,
        status: str,
        actual_tokens: int,
        refunded_tokens: int,
        actual_cost_usd: float,
        remaining_tpm: int,
        settled_at_ms: int,
    ) -> StoredReservation | None:
        """Persist final settlement state for a reservation."""
        with self.write_transaction():
            cursor = self._conn.execute(
                """
                UPDATE reservations
                SET status = ?, actual_tokens = ?, refunded_tokens = ?,
                    actual_cost_usd = ?, remaining_tpm = ?, settled_at_ms = ?
                WHERE reservation_id = ? AND status = 'reserved'
                """,
                (
                    status,
                    actual_tokens,
                    refunded_tokens,
                    actual_cost_usd,
                    remaining_tpm,
                    settled_at_ms,
                    reservation_id,
                ),
            )
            return self.get_reservation(reservation_id) if cursor.rowcount == 1 else None

    def list_reservations(
        self,
        *,
        tenant_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> tuple[list[StoredReservation], int]:
        """List reservations with optional tenant/status filter."""
        clauses: list[str] = []
        params: list[Any] = []
        if tenant_id:
            clauses.append("tenant_id = ?")
            params.append(tenant_id)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock, self._conn:
            self._conn.execute("BEGIN")
            total_row = self._conn.execute(
                f"SELECT COUNT(*) AS cnt FROM reservations {where_sql}",
                params,
            ).fetchone()
            total = int(total_row["cnt"]) if total_row is not None else 0
            rows = self._conn.execute(
                f"SELECT * FROM reservations {where_sql} "
                "ORDER BY created_at_ms DESC, reservation_id LIMIT ?",
                [*params, limit],
            ).fetchall()
            return [self._row_to_reservation(r) for r in rows], total

    def save_trace_run(
        self, *, run_id: str, trace_id: str, payload: dict[str, Any], created_at_ms: int
    ) -> None:
        """Persist OTLP trace payload keyed by run_id."""
        with self.write_transaction():
            self._conn.execute(
                """
                INSERT INTO trace_runs (run_id, trace_id, payload_json, created_at_ms)
                VALUES (?, ?, ?, ?)
                """,
                (run_id, trace_id, json.dumps(payload), created_at_ms),
            )

    def get_trace_run(self, run_id: str) -> dict[str, Any] | None:
        """Retrieve persisted OTLP trace payload by run_id."""
        with self._lock:
            row = self._conn.execute(
                "SELECT payload_json FROM trace_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if row is None:
                return None
            parsed = json.loads(str(row["payload_json"]))
            return dict(parsed) if isinstance(parsed, dict) else None
