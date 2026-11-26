"""Dual RPM+TPM hierarchical quota control and Reserve -> Settle/Refund ledger."""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from llm_gateway.compat import (
    FixedSlidingWindowLimiter,
    HardenedMemoryStore,
    MonotonicTokenBucketLimiter,
    OversizedRequestError,
    StoreCapacityError,
)
from llm_gateway.config import MODEL_SPECS, ModelSpec
from llm_gateway.redaction import RedactionMode, evaluate_redaction
from llm_gateway.storage import (
    USD_SCALE,
    GatewayStorage,
    StoredReservation,
    idempotency_fingerprint,
)


class QuotaExceededError(RuntimeError):
    """Raised when RPM, TPM, or USD quota is exhausted."""

    def __init__(
        self,
        message: str,
        *,
        error_code: str = "QUOTA_EXCEEDED",
        retry_after_seconds: float = 1.0,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.retry_after_seconds = retry_after_seconds


class RedactionBlockedError(RuntimeError):
    """Raised when strict redaction mode blocks a payload."""


class ReservationNotFoundError(LookupError):
    """Raised when settling an unknown reservation ID."""


class ReservationConflictError(RuntimeError):
    """Raised when settling an already finalized reservation."""


class IdempotencyConflictError(RuntimeError):
    """The key belongs to a reservation with different pricing or token inputs."""


@dataclass(frozen=True, slots=True)
class QuotaLimits:
    """Configurable hierarchical limits for Global, Tenant, and Model tiers."""

    rpm_limit: int = 120
    tpm_limit: int = 120_000
    window_seconds: int = 60
    usd_budget_limit: float = 25.0


def compute_token_cost_usd(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    """Compute exact USD cost for prompt + completion token counts."""
    spec: ModelSpec | None = MODEL_SPECS.get(model)
    if spec is None:
        spec = MODEL_SPECS["gpt-4o-mini"]
    prompt_cost = (max(0, prompt_tokens) / 1000.0) * spec.prompt_usd_per_1k
    completion_cost = (max(0, completion_tokens) / 1000.0) * spec.completion_usd_per_1k
    return round(prompt_cost + completion_cost, 8)


class DualQuotaCoordinator:
    """Coordinates atomic hierarchical RPM + TPM + USD Reserve -> Settle/Refund."""

    def __init__(
        self,
        *,
        store: HardenedMemoryStore,
        storage: GatewayStorage,
        limits: QuotaLimits | None = None,
        redaction_mode: RedactionMode = "mask",
    ) -> None:
        self._store = store
        self._storage = storage
        self._limits = limits or QuotaLimits()
        self._redaction_mode: RedactionMode = redaction_mode
        self._rpm_limiter = FixedSlidingWindowLimiter(store)
        self._tpm_limiter = MonotonicTokenBucketLimiter(store)
        self._lock = threading.RLock()

    def _scope_keys(self, tenant_id: str, model: str) -> tuple[tuple[str, str, int, int], ...]:
        global_rpm = max(self._limits.rpm_limit * 4, self._limits.rpm_limit)
        global_tpm = max(self._limits.tpm_limit * 4, self._limits.tpm_limit)
        return (
            ("global", "global", global_rpm, global_tpm),
            ("tenant", f"tenant:{tenant_id}", self._limits.rpm_limit, self._limits.tpm_limit),
            (
                "model",
                f"tenant:{tenant_id}:model:{model}",
                self._limits.rpm_limit,
                self._limits.tpm_limit,
            ),
        )

    def reserve(
        self,
        *,
        tenant_id: str,
        model: str,
        estimated_prompt_tokens: int,
        max_completion_tokens: int,
        payload_text: str | None = None,
        idempotency_key: str | None = None,
        trace_id: str | None = None,
        provider_override: str | None = None,
        now_ns: int | None = None,
        build_trace: Callable[[StoredReservation], dict[str, Any]] | None = None,
    ) -> StoredReservation:
        """Atomically evaluate redaction and hierarchical quotas and persist the reservation."""
        if estimated_prompt_tokens < 0 or max_completion_tokens < 0:
            raise ValueError("Token estimates must be nonnegative")
        redaction_hits = 0
        if payload_text:
            red_res = evaluate_redaction(payload_text, mode=self._redaction_mode)
            redaction_hits = red_res.total_hits
            if not red_res.allowed:
                raise RedactionBlockedError("Payload blocked by secret/PII redaction policy")

        reserved_tokens = estimated_prompt_tokens + max_completion_tokens
        est_cost = compute_token_cost_usd(model, estimated_prompt_tokens, max_completion_tokens)
        scopes = self._scope_keys(tenant_id, model)
        keys = [f"{kind}:{key}" for _, key, _, _ in scopes for kind in ("rpm", "tpm")]
        fingerprint = idempotency_fingerprint(idempotency_key) if idempotency_key else None
        with self._lock, self._store.rollback_on_error(keys), self._storage.write_transaction():
            if fingerprint is not None:
                existing = self._storage.get_by_idempotency_key(tenant_id, fingerprint)
                if existing is not None:
                    if (
                        existing.model != model
                        or existing.estimated_prompt_tokens != estimated_prompt_tokens
                        or existing.max_completion_tokens != max_completion_tokens
                    ):
                        raise IdempotencyConflictError(
                            "Key already used with different reservation inputs"
                        )
                    return existing

            if reserved_tokens > self._limits.tpm_limit:
                raise OversizedRequestError("Reserved tokens exceed the per-request TPM capacity")

            now = time.monotonic_ns() if now_ns is None else now_ns
            created_at_ms = time.time_ns() // 1_000_000
            min_rem_rpm = self._limits.rpm_limit
            min_rem_tpm = self._limits.tpm_limit
            hold_epochs: list[int] = []
            try:
                for scope_name, scope_key, rpm_limit, tpm_limit in scopes:
                    rpm_dec = self._rpm_limiter.check_and_consume(
                        f"rpm:{scope_key}",
                        cost=1,
                        limit=rpm_limit,
                        period_seconds=self._limits.window_seconds,
                        now_ns=now,
                    )
                    if not rpm_dec.allowed:
                        raise QuotaExceededError(
                            f"RPM limit exceeded at {scope_name} scope",
                            retry_after_seconds=rpm_dec.retry_after_seconds,
                        )
                    min_rem_rpm = min(min_rem_rpm, rpm_dec.remaining)

                    tpm_dec = self._tpm_limiter.check_and_consume(
                        f"tpm:{scope_key}",
                        cost=reserved_tokens,
                        capacity=tpm_limit,
                        refill_per_sec=tpm_limit / self._limits.window_seconds,
                        now_ns=now,
                    )
                    if not tpm_dec.allowed:
                        raise QuotaExceededError(
                            f"TPM token budget exceeded at {scope_name} scope",
                            retry_after_seconds=tpm_dec.retry_after_seconds,
                        )
                    min_rem_tpm = min(min_rem_tpm, tpm_dec.remaining)
                    hold_epochs.append(tpm_dec.bucket_epoch)
            except StoreCapacityError as exc:
                raise QuotaExceededError(
                    "Quota store is at capacity",
                    retry_after_seconds=float(self._limits.window_seconds),
                ) from exc

            period_ms = self._limits.window_seconds * 1_000
            start_ms = created_at_ms // period_ms * period_ms
            end_ms = start_ms + period_ms
            spent = self._storage.tenant_spend_units(tenant_id, start_ms, end_ms)
            budget_units = round(self._limits.usd_budget_limit * USD_SCALE)
            if spent + round(est_cost * USD_SCALE) > budget_units:
                raise QuotaExceededError(
                    "Tenant USD budget exhausted",
                    retry_after_seconds=(end_ms - created_at_ms) / 1_000,
                )

            spec = MODEL_SPECS.get(model, MODEL_SPECS["gpt-4o-mini"])
            reservation = StoredReservation(
                reservation_id=f"resv_{uuid.uuid4().hex}",
                trace_id=trace_id or uuid.uuid4().hex,
                tenant_id=tenant_id,
                model=model,
                status="reserved",
                reserved_tokens=reserved_tokens,
                estimated_prompt_tokens=estimated_prompt_tokens,
                max_completion_tokens=max_completion_tokens,
                actual_tokens=None,
                refunded_tokens=0,
                estimated_cost_usd=est_cost,
                actual_cost_usd=None,
                remaining_rpm=min_rem_rpm,
                remaining_tpm=min_rem_tpm,
                provider_used=provider_override or spec.primary_provider,
                redaction_hits=redaction_hits,
                idempotency_key=fingerprint,
                created_at_ms=created_at_ms,
                settled_at_ms=None,
                tpm_hold=self._store.bucket_epoch_scope + ":" + ",".join(map(str, hold_epochs)),
            )
            return self._storage.create_reservation(
                reservation, trace_payload=build_trace(reservation) if build_trace else None
            )

    def settle(
        self,
        *,
        reservation_id: str,
        actual_prompt_tokens: int,
        actual_completion_tokens: int,
        provider_status: int = 200,
        now_ns: int | None = None,
    ) -> StoredReservation:
        """Settle once and adjust TPM for the observed usage."""
        if actual_prompt_tokens < 0 or actual_completion_tokens < 0:
            raise ValueError("Actual token counts must be nonnegative")
        actual_total = actual_prompt_tokens + actual_completion_tokens
        with self._lock:
            existing = self._storage.get_reservation(reservation_id)
            if existing is None:
                raise ReservationNotFoundError("Reservation not found")
            if existing.status != "reserved":
                raise ReservationConflictError("Reservation already finalized")

            delta = existing.reserved_tokens - actual_total
            actual_cost = compute_token_cost_usd(
                existing.model, actual_prompt_tokens, actual_completion_tokens
            )
            new_status = "refunded" if provider_status >= 400 or delta > 0 else "settled"
            scopes = self._scope_keys(existing.tenant_id, existing.model)
            keys = [f"tpm:{key}" for _, key, _, _ in scopes]
            scope, _, epochs = (existing.tpm_hold or "").partition(":")
            hold_epochs = (
                [int(epoch) for epoch in epochs.split(",")]
                if scope == self._store.bucket_epoch_scope
                else [0] * len(scopes)
            )
            with self._store.rollback_on_error(keys), self._storage.write_transaction():
                now = time.monotonic_ns() if now_ns is None else now_ns
                try:
                    remaining = min(
                        self._tpm_limiter.adjust(
                            f"tpm:{key}",
                            tokens_delta=delta,
                            capacity=tpm_limit,
                            refill_per_sec=tpm_limit / self._limits.window_seconds,
                            now_ns=now,
                            refund_epoch=epoch,
                        )
                        for (_, key, _, tpm_limit), epoch in zip(scopes, hold_epochs, strict=True)
                    )
                except StoreCapacityError as exc:
                    raise QuotaExceededError(
                        "Quota store is at capacity",
                        retry_after_seconds=float(self._limits.window_seconds),
                    ) from exc
                updated = self._storage.update_settlement(
                    reservation_id=reservation_id,
                    status=new_status,
                    actual_tokens=actual_total,
                    refunded_tokens=max(0, delta),
                    actual_cost_usd=actual_cost,
                    remaining_tpm=max(0, int(remaining)),
                    settled_at_ms=time.time_ns() // 1_000_000,
                )
                if updated is None:
                    raise ReservationConflictError("Reservation already finalized")
                return updated
