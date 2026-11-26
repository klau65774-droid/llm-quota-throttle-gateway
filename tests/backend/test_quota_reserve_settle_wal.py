"""Tests for Dual RPM+TPM Reserve -> Settle/Refund and SQLite WAL persistence."""

from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from threading import Barrier

import pytest
from llm_gateway.compat import HardenedMemoryStore, OversizedRequestError
from llm_gateway.quota import (
    DualQuotaCoordinator,
    IdempotencyConflictError,
    QuotaExceededError,
    QuotaLimits,
    ReservationConflictError,
    ReservationNotFoundError,
)
from llm_gateway.storage import GatewayStorage, StoredReservation

_TEST_CLOCK_NS = 2_000_000_000_000


def _reserve_tokens(
    coord: DualQuotaCoordinator,
    tenant_id: str,
    tokens: int,
    *,
    model: str = "gpt-4o-mini",
    idempotency_key: str | None = None,
    now_ns: int = _TEST_CLOCK_NS,
) -> StoredReservation:
    return coord.reserve(
        tenant_id=tenant_id,
        model=model,
        estimated_prompt_tokens=0,
        max_completion_tokens=tokens,
        idempotency_key=idempotency_key,
        now_ns=now_ns,
    )


def test_reserve_settle_refund_and_wal_persistence(tmp_path: Path) -> None:
    """Unused max_completion_tokens are refunded back to TPM pool and persisted in WAL."""
    db_file = tmp_path / "gateway.sqlite3"
    store = HardenedMemoryStore(max_keys=128)
    storage = GatewayStorage(db_file)
    coord = DualQuotaCoordinator(
        store=store,
        storage=storage,
        limits=QuotaLimits(rpm_limit=10, tpm_limit=2000, window_seconds=60, usd_budget_limit=5.0),
    )

    try:
        r1 = coord.reserve(
            tenant_id="tenant-a",
            model="gpt-4o-mini",
            estimated_prompt_tokens=200,
            max_completion_tokens=1600,
            idempotency_key="idem-001",
            now_ns=1_000_000_000_000,
        )
        assert r1.reserved_tokens == 1800
        assert r1.remaining_tpm == 200

        # Duplicate idempotency key returns same record without double charging
        r1_dup = coord.reserve(
            tenant_id="tenant-a",
            model="gpt-4o-mini",
            estimated_prompt_tokens=200,
            max_completion_tokens=1600,
            idempotency_key="idem-001",
            now_ns=1_000_000_000_000,
        )
        assert r1_dup.reservation_id == r1.reservation_id

        # Second request requesting 1000 tokens is blocked before settle
        with pytest.raises(QuotaExceededError):
            coord.reserve(
                tenant_id="tenant-a",
                model="gpt-4o-mini",
                estimated_prompt_tokens=500,
                max_completion_tokens=500,
                now_ns=1_000_000_000_000,
            )

        # Settle r1 with only 100 completion tokens -> refunds 1500 tokens!
        settled = coord.settle(
            reservation_id=r1.reservation_id,
            actual_prompt_tokens=200,
            actual_completion_tokens=100,
            provider_status=200,
            now_ns=1_000_000_000_000,
        )
        assert settled.status == "refunded"
        assert settled.refunded_tokens == 1500
        assert settled.actual_tokens == 300

        # Double settle raises ReservationConflictError
        with pytest.raises(ReservationConflictError):
            coord.settle(
                reservation_id=r1.reservation_id,
                actual_prompt_tokens=200,
                actual_completion_tokens=100,
            )

        # Unknown reservation raises ReservationNotFoundError
        with pytest.raises(ReservationNotFoundError):
            coord.settle(
                reservation_id="resv_missing",
                actual_prompt_tokens=10,
                actual_completion_tokens=10,
            )

        # Now the previously blocked 1000-token request fits thanks to refund!
        r2 = coord.reserve(
            tenant_id="tenant-a",
            model="gpt-4o-mini",
            estimated_prompt_tokens=500,
            max_completion_tokens=500,
            now_ns=1_000_000_000_000,
        )
        assert r2.status == "reserved"
    finally:
        storage.close()

    # Re-open SQLite WAL file cleanly to confirm durability
    reopened = GatewayStorage(db_file)
    try:
        rows, total = reopened.list_reservations(tenant_id="tenant-a")
        assert total == 2
        assert len(rows) == 2
    finally:
        reopened.close()


def test_atomic_rollback_when_tpm_fails(tmp_path: Path) -> None:
    """A tenant TPM denial restores its RPM and the previously admitted global usage."""
    storage = GatewayStorage(tmp_path / "rollback.sqlite3")
    try:
        coord = DualQuotaCoordinator(
            store=HardenedMemoryStore(max_keys=64),
            storage=storage,
            limits=QuotaLimits(
                rpm_limit=2, tpm_limit=1_000, window_seconds=60, usd_budget_limit=5.0
            ),
        )
        r_ok = _reserve_tokens(coord, "tenant-a", 800)
        assert r_ok.remaining_rpm == 1

        with pytest.raises(QuotaExceededError):
            _reserve_tokens(coord, "tenant-a", 500)

        r_after = _reserve_tokens(coord, "tenant-a", 200)
        assert r_after.remaining_rpm == 0
        assert r_after.remaining_tpm == 0
        for tenant_id in ("tenant-b", "tenant-c", "tenant-d"):
            assert _reserve_tokens(coord, tenant_id, 1_000).remaining_tpm == 0
            assert _reserve_tokens(coord, tenant_id, 0).remaining_rpm == 0
        assert storage.list_reservations()[1] == 8
    finally:
        storage.close()


def test_failed_reservation_write_and_oversize_leave_all_quota_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = GatewayStorage(tmp_path / "reserve-write.sqlite3")
    coord = DualQuotaCoordinator(
        store=HardenedMemoryStore(max_keys=64),
        storage=storage,
        limits=QuotaLimits(rpm_limit=1, tpm_limit=1_000),
    )

    def unavailable(*args: object, **kwargs: object) -> StoredReservation:
        raise sqlite3.OperationalError("temporary database write failure")

    try:
        with pytest.raises(OversizedRequestError):
            _reserve_tokens(coord, "tenant-a", 1_001)
        with monkeypatch.context() as patch:
            patch.setattr(storage, "create_reservation", unavailable)
            with pytest.raises(sqlite3.OperationalError):
                _reserve_tokens(coord, "tenant-a", 1_000, idempotency_key="retry-key")
        assert storage.list_reservations()[1] == 0

        saved = _reserve_tokens(coord, "tenant-a", 1_000, idempotency_key="retry-key")
        assert saved.remaining_tpm == saved.remaining_rpm == 0
        for tenant_id in ("tenant-b", "tenant-c", "tenant-d"):
            assert _reserve_tokens(coord, tenant_id, 1_000).remaining_tpm == 0
        assert storage.list_reservations()[1] == 4
    finally:
        storage.close()


def test_budget_denial_preserves_quota_and_spend_survives_a_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "budget.sqlite3"
    limits = QuotaLimits(rpm_limit=2, tpm_limit=1_000, usd_budget_limit=0.0002)
    clock_ns = 120_000_000_000
    monkeypatch.setattr("llm_gateway.quota.time.time_ns", lambda: clock_ns)
    storage = GatewayStorage(db_path)
    try:
        coord = DualQuotaCoordinator(store=HardenedMemoryStore(), storage=storage, limits=limits)
        with pytest.raises(QuotaExceededError):
            _reserve_tokens(coord, "tenant-a", 21, model="gpt-4o")
        approved = _reserve_tokens(coord, "tenant-a", 20, model="gpt-4o")
        assert approved.estimated_cost_usd == pytest.approx(0.0002)
        assert (approved.remaining_rpm, approved.remaining_tpm) == (1, 980)
    finally:
        storage.close()

    reopened = GatewayStorage(db_path)
    try:
        coord = DualQuotaCoordinator(store=HardenedMemoryStore(), storage=reopened, limits=limits)
        with pytest.raises(QuotaExceededError):
            _reserve_tokens(coord, "tenant-a", 1, model="gpt-4o")
        coord.settle(
            reservation_id=approved.reservation_id,
            actual_prompt_tokens=0,
            actual_completion_tokens=15,
            now_ns=_TEST_CLOCK_NS,
        )
        assert _reserve_tokens(coord, "tenant-a", 5, model="gpt-4o").remaining_rpm == 1
        with pytest.raises(QuotaExceededError):
            _reserve_tokens(coord, "tenant-a", 1, model="gpt-4o")
        assert _reserve_tokens(coord, "tenant-b", 20, model="gpt-4o").remaining_rpm == 1

        clock_ns += 60_000_000_000
        assert (
            _reserve_tokens(
                coord, "tenant-a", 20, model="gpt-4o", now_ns=_TEST_CLOCK_NS + 60_000_000_000
            ).status
            == "reserved"
        )
    finally:
        reopened.close()


def test_idempotent_replays_are_tenant_scoped_and_keep_original_inputs(tmp_path: Path) -> None:
    storage = GatewayStorage(tmp_path / "idempotency.sqlite3")
    coord = DualQuotaCoordinator(
        store=HardenedMemoryStore(), storage=storage, limits=QuotaLimits(rpm_limit=2, tpm_limit=100)
    )
    start = Barrier(4, timeout=5)

    def reserve_shared() -> str:
        start.wait()
        return _reserve_tokens(coord, "tenant-a", 60, idempotency_key="replay-key").reservation_id

    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            replays = [executor.submit(reserve_shared) for _ in range(4)]
            replay_ids = {result.result(timeout=5) for result in replays}
        assert len(replay_ids) == 1
        other = _reserve_tokens(coord, "tenant-b", 60, idempotency_key="replay-key")
        assert other.reservation_id not in replay_ids
        assert other.tenant_id == "tenant-b"
        assert other.idempotency_key and other.idempotency_key != "replay-key"

        with pytest.raises(IdempotencyConflictError):
            _reserve_tokens(coord, "tenant-a", 59, idempotency_key="replay-key")
        remaining = _reserve_tokens(coord, "tenant-a", 40)
        assert remaining.remaining_rpm == remaining.remaining_tpm == 0
        assert storage.list_reservations()[1] == 3
    finally:
        storage.close()


def test_concurrent_settlement_refunds_once_after_a_failed_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_path = tmp_path / "settle-write.sqlite3"
    storage = GatewayStorage(db_path)
    store = HardenedMemoryStore()
    limits = QuotaLimits(rpm_limit=4, tpm_limit=100)
    coord = DualQuotaCoordinator(store=store, storage=storage, limits=limits)
    start = Barrier(4, timeout=5)

    def unavailable(*args: object, **kwargs: object) -> StoredReservation:
        raise sqlite3.OperationalError("temporary database write failure")

    try:
        first = _reserve_tokens(coord, "tenant-a", 60)
        _reserve_tokens(coord, "tenant-a", 20)

        def settle(coordinator: DualQuotaCoordinator) -> StoredReservation:
            return coordinator.settle(
                reservation_id=first.reservation_id,
                actual_prompt_tokens=0,
                actual_completion_tokens=40,
                now_ns=_TEST_CLOCK_NS,
            )

        with monkeypatch.context() as patch:
            patch.setattr(storage, "update_settlement", unavailable)
            with pytest.raises(sqlite3.OperationalError):
                settle(coord)
        unchanged = storage.get_reservation(first.reservation_id)
        assert unchanged is not None and unchanged.status == "reserved"
        with pytest.raises(QuotaExceededError):
            _reserve_tokens(coord, "tenant-a", 21)

        def settle_shared(coordinator: DualQuotaCoordinator) -> StoredReservation | None:
            start.wait()
            try:
                return settle(coordinator)
            except ReservationConflictError:
                return None

        with closing(GatewayStorage(db_path)) as second_connection:
            second = DualQuotaCoordinator(store=store, storage=second_connection, limits=limits)
            with ThreadPoolExecutor(max_workers=4) as executor:
                callers = [executor.submit(settle_shared, runner) for runner in (coord, second) * 2]
                finalized = [result for caller in callers if (result := caller.result(timeout=5))]
        assert len(finalized) == 1
        assert finalized[0].refunded_tokens == 20
        assert finalized[0].remaining_tpm == 40
        with pytest.raises(QuotaExceededError):
            _reserve_tokens(coord, "tenant-a", 41)
        assert _reserve_tokens(coord, "tenant-a", 40).remaining_tpm == 0
    finally:
        storage.close()


def test_large_settlement_overrun_remains_charged_until_refill(tmp_path: Path) -> None:
    storage = GatewayStorage(tmp_path / "overrun.sqlite3")
    coord = DualQuotaCoordinator(
        store=HardenedMemoryStore(),
        storage=storage,
        limits=QuotaLimits(rpm_limit=10, tpm_limit=100, window_seconds=100),
    )
    try:
        reservation = _reserve_tokens(coord, "tenant-a", 80)
        settled = coord.settle(
            reservation_id=reservation.reservation_id,
            actual_prompt_tokens=0,
            actual_completion_tokens=500,
            now_ns=_TEST_CLOCK_NS,
        )
        assert settled.actual_tokens == 500
        assert settled.refunded_tokens == settled.remaining_tpm == 0
        with pytest.raises(QuotaExceededError):
            _reserve_tokens(coord, "tenant-a", 1, now_ns=_TEST_CLOCK_NS + 301_000_000_000)
        assert (
            _reserve_tokens(
                coord, "tenant-a", 1, now_ns=_TEST_CLOCK_NS + 401_000_000_000
            ).remaining_tpm
            == 0
        )
    finally:
        storage.close()


def test_late_refund_does_not_credit_a_refilled_bucket(tmp_path: Path) -> None:
    limits = QuotaLimits(rpm_limit=10, tpm_limit=10, window_seconds=60)
    storage = GatewayStorage(tmp_path / "late-refund.sqlite3")
    coord = DualQuotaCoordinator(
        store=HardenedMemoryStore(max_keys=64),
        storage=storage,
        limits=limits,
    )
    try:
        early = _reserve_tokens(coord, "tenant-a", 8)
        later = _TEST_CLOCK_NS + 60_000_000_000
        assert _reserve_tokens(coord, "tenant-a", 10, now_ns=later).remaining_tpm == 0
        settled = coord.settle(
            reservation_id=early.reservation_id,
            actual_prompt_tokens=0,
            actual_completion_tokens=0,
            now_ns=later,
        )
        assert settled.refunded_tokens == 8
        assert settled.remaining_tpm == 0
    finally:
        storage.close()


def test_legacy_ledger_migrates_keys_and_keeps_existing_reservations(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.sqlite3"
    legacy_id = "resv_" + "a" * 16
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(
            """
            CREATE TABLE reservations (
                reservation_id TEXT PRIMARY KEY, trace_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL, model TEXT NOT NULL, status TEXT NOT NULL,
                reserved_tokens INTEGER NOT NULL, estimated_prompt_tokens INTEGER NOT NULL,
                max_completion_tokens INTEGER NOT NULL, actual_tokens INTEGER,
                refunded_tokens INTEGER NOT NULL DEFAULT 0,
                estimated_cost_usd REAL NOT NULL, actual_cost_usd REAL,
                remaining_rpm INTEGER NOT NULL, remaining_tpm INTEGER NOT NULL,
                provider_used TEXT NOT NULL, redaction_hits INTEGER NOT NULL DEFAULT 0,
                idempotency_key TEXT UNIQUE, created_at_ms INTEGER NOT NULL, settled_at_ms INTEGER
            )
            """
        )
        conn.execute(
            """
            INSERT INTO reservations VALUES (
                ?, ?, 'tenant-a', 'gpt-4o-mini', 'reserved', 10, 0, 10, NULL,
                0, 0.000006, NULL, 1, 90, 'openai-primary', 0, ?, 1, NULL
            )
            """,
            (legacy_id, "a" * 32, "legacy-key"),
        )

    with closing(GatewayStorage(db_path)) as storage:
        coord = DualQuotaCoordinator(store=HardenedMemoryStore(), storage=storage)
        replay = _reserve_tokens(coord, "tenant-a", 10, idempotency_key="legacy-key")
        assert replay.reservation_id == legacy_id
        assert replay.created_at_ms == 1
        assert replay.idempotency_key and replay.idempotency_key != "legacy-key"
        new = _reserve_tokens(coord, "tenant-b", 10, idempotency_key="legacy-key")
        assert new.reservation_id != legacy_id
        assert storage.list_reservations()[1] == 2
