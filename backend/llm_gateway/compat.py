"""Hardened monotonic rate limiters, bounded store, and upstream baseline bridge."""

from __future__ import annotations

import math
import threading
import time
import uuid
from collections import OrderedDict
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from upstream_baseline.throttled import MemoryStore as UpstreamMemoryStore
from upstream_baseline.throttled import RateLimiterType as UpstreamRateLimiterType
from upstream_baseline.throttled import Throttled as UpstreamThrottled
from upstream_baseline.throttled import rate_limiter as upstream_rate_limiter
from upstream_baseline.throttled.exceptions import DataError as UpstreamDataError
from upstream_baseline.throttled.rate_limiter.sliding_window import MemoryLimitAtomicAction
from upstream_baseline.throttled.store.memory import MemoryStoreBackend

__all__ = [
    "FixedSlidingWindowLimiter",
    "HardenedMemoryStore",
    "LimiterDecision",
    "MonotonicGCRALimiter",
    "MonotonicTokenBucketLimiter",
    "OversizedRequestError",
    "StoreCapacityError",
    "StoreMetricsSnapshot",
    "UpstreamDataError",
    "UpstreamMemoryStore",
    "UpstreamProbeOutcome",
    "UpstreamRateLimiterType",
    "UpstreamThrottled",
    "run_upstream_baseline_workload",
    "upstream_rate_limiter",
]


class OversizedRequestError(ValueError):
    """Raised when a single request cost exceeds bucket capacity."""


class StoreCapacityError(RuntimeError):
    """The store is full of live rate-limit state."""


@dataclass(frozen=True, slots=True)
class _StoreEntry:
    value: Any
    expires_ns: int
    active_throttled: bool


@dataclass(frozen=True, slots=True)
class StoreMetricsSnapshot:
    """Telemetry from HardenedMemoryStore capacity sweeps and evictions."""

    current_keys: int
    max_keys: int
    expired_swept_total: int
    idle_evicted_total: int
    active_evicted_total: int


class HardenedMemoryStore:
    """Thread-safe monotonic memory store with expired-first eviction protection."""

    def __init__(self, max_keys: int = 2048) -> None:
        if max_keys < 1:
            raise ValueError("max_keys must be >= 1")
        self.max_keys = max_keys
        self._lock = threading.RLock()
        self._entries: OrderedDict[str, _StoreEntry] = OrderedDict()
        self.bucket_epoch_scope = uuid.uuid4().hex
        self._bucket_epoch = 0
        self.expired_swept_total = 0
        self.idle_evicted_total = 0
        self.active_evicted_total = 0

    @property
    def lock(self) -> threading.RLock:
        """Expose re-entrant lock for composite atomic limiter updates."""
        return self._lock

    def next_bucket_epoch(self) -> int:
        """Assign a unique generation for a bucket that starts at full capacity."""
        with self._lock:
            self._bucket_epoch += 1
            return self._bucket_epoch

    def _sweep_expired_locked(self, now_ns: int) -> int:
        expired_keys = [
            key
            for key, entry in self._entries.items()
            if entry.expires_ns > 0 and entry.expires_ns <= now_ns
        ]
        for key in expired_keys:
            self._entries.pop(key, None)
        self.expired_swept_total += len(expired_keys)
        return len(expired_keys)

    def _evict_for_capacity_locked(self, now_ns: int) -> None:
        if len(self._entries) < self.max_keys:
            return
        self._sweep_expired_locked(now_ns)
        if len(self._entries) < self.max_keys:
            return
        # Prefer evicting non-active / unthrottled keys first so attackers cannot
        # flush active tenant rate-limit states with throwaway keys.
        idle_key = next(
            (key for key, entry in self._entries.items() if not entry.active_throttled),
            None,
        )
        if idle_key is not None:
            self._entries.pop(idle_key, None)
            self.idle_evicted_total += 1
            return
        raise StoreCapacityError("Live quota state is at capacity")

    @contextmanager
    def rollback_on_error(self, keys: Iterable[str]) -> Iterator[None]:
        """Keep a composite operation atomic over its rate-limit keys."""
        with self._lock:
            before = {key: self._entries.get(key) for key in keys}
            try:
                yield
            except BaseException:
                for key, entry in before.items():
                    if entry is None:
                        self._entries.pop(key, None)
                    else:
                        self._entries[key] = entry
                raise

    def get(self, key: str, now_ns: int | None = None) -> Any | None:
        """Return entry value if present and unexpired."""
        with self._lock:
            now = time.monotonic_ns() if now_ns is None else now_ns
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.expires_ns > 0 and entry.expires_ns <= now:
                self._entries.pop(key, None)
                self.expired_swept_total += 1
                return None
            self._entries.move_to_end(key)
            return entry.value

    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: float,
        *,
        active_throttled: bool = False,
        now_ns: int | None = None,
    ) -> None:
        """Store or update entry with bounded expired-first eviction."""
        with self._lock:
            now = time.monotonic_ns() if now_ns is None else now_ns
            expires_ns = now + math.ceil(ttl_seconds * 1_000_000_000) if ttl_seconds > 0 else 0
            if key not in self._entries:
                self._evict_for_capacity_locked(now)
            self._entries[key] = _StoreEntry(
                value=value,
                expires_ns=expires_ns,
                active_throttled=active_throttled,
            )
            self._entries.move_to_end(key)

    def clear(self) -> None:
        """Clear all keys cleanly."""
        with self._lock:
            self._entries.clear()

    def snapshot(self, now_ns: int | None = None) -> StoreMetricsSnapshot:
        """Return store eviction telemetry."""
        now = time.monotonic_ns() if now_ns is None else now_ns
        with self._lock:
            self._sweep_expired_locked(now)
            return StoreMetricsSnapshot(
                current_keys=len(self._entries),
                max_keys=self.max_keys,
                expired_swept_total=self.expired_swept_total,
                idle_evicted_total=self.idle_evicted_total,
                active_evicted_total=self.active_evicted_total,
            )


@dataclass(frozen=True, slots=True)
class LimiterDecision:
    """Outcome from evaluating a rate limiter."""

    allowed: bool
    remaining: int
    retry_after_seconds: float
    current_used: int
    phantom_burn_prevented: int = 0
    bucket_epoch: int = 0


@dataclass(frozen=True, slots=True)
class _SlidingWindowState:
    window_index: int
    prev_count: int
    curr_count: int


class FixedSlidingWindowLimiter:
    """Sliding window counter that validates limits BEFORE mutating quota."""

    def __init__(self, store: HardenedMemoryStore) -> None:
        self._store = store

    def check_and_consume(
        self,
        key: str,
        *,
        cost: int,
        limit: int,
        period_seconds: int,
        now_ns: int | None = None,
    ) -> LimiterDecision:
        """Check weighted sliding count and commit cost only when admitted."""
        if cost < 0:
            raise ValueError("cost must be >= 0")
        if limit < 0 or period_seconds <= 0:
            raise ValueError("limit must be >= 0 and period_seconds > 0")
        if cost > limit:
            raise OversizedRequestError(f"cost {cost} exceeds window limit {limit}")

        period_ns = period_seconds * 1_000_000_000

        with self._store.lock:
            now = time.monotonic_ns() if now_ns is None else now_ns
            curr_idx, offset_ns = divmod(now, period_ns)
            raw = self._store.get(key, now_ns=now)
            if isinstance(raw, _SlidingWindowState) and raw.window_index == curr_idx:
                state = raw
            elif isinstance(raw, _SlidingWindowState) and raw.window_index == curr_idx - 1:
                state = _SlidingWindowState(curr_idx, raw.curr_count, 0)
            else:
                state = _SlidingWindowState(curr_idx, 0, 0)

            weighted_before = state.prev_count * (period_ns - offset_ns)
            weighted_before += state.curr_count * period_ns
            weighted_after = weighted_before + cost * period_ns
            used_before = -(-weighted_before // period_ns)
            limit_ns = limit * period_ns
            if weighted_after > limit_ns:
                if state.curr_count + cost <= limit:
                    wait_ns = -(-(weighted_after - limit_ns) // state.prev_count)
                else:
                    next_deficit = (state.curr_count + cost - limit) * period_ns
                    wait_ns = period_ns - offset_ns - (-next_deficit // state.curr_count)
                return LimiterDecision(
                    allowed=False,
                    remaining=max(0, limit - used_before),
                    retry_after_seconds=max(1, -(-wait_ns // 1_000_000)) / 1000,
                    current_used=used_before,
                    phantom_burn_prevented=cost,
                )

            if cost:
                updated = _SlidingWindowState(curr_idx, state.prev_count, state.curr_count + cost)
                self._save(key, updated, period_ns, now)
            used_after = -(-weighted_after // period_ns)
            return LimiterDecision(
                allowed=True,
                remaining=max(0, limit - used_after),
                retry_after_seconds=0.0,
                current_used=used_after,
                phantom_burn_prevented=0,
            )

    def _save(self, key: str, state: _SlidingWindowState, period_ns: int, now: int) -> None:
        protected = state.curr_count > 0 or state.prev_count > 0
        expiry_window = state.window_index + (2 if state.curr_count else 1)
        self._store.set(
            key,
            state,
            ttl_seconds=max(0.000000001, (expiry_window * period_ns - now) / 1_000_000_000),
            active_throttled=protected,
            now_ns=now,
        )

    def refund(
        self,
        key: str,
        *,
        tokens: int,
        limit: int,
        period_seconds: int,
        now_ns: int | None = None,
    ) -> int:
        """Refund unused units cleanly without going negative."""
        if tokens <= 0:
            return 0
        if limit < 0 or period_seconds <= 0:
            raise ValueError("limit must be >= 0 and period_seconds > 0")
        period_ns = period_seconds * 1_000_000_000
        with self._store.lock:
            now = time.monotonic_ns() if now_ns is None else now_ns
            curr_idx = now // period_ns
            raw = self._store.get(key, now_ns=now)
            if not isinstance(raw, _SlidingWindowState) or raw.window_index not in (
                curr_idx,
                curr_idx - 1,
            ):
                return 0
            if raw.window_index == curr_idx - 1:
                reclaimed = min(tokens, raw.curr_count)
                updated = _SlidingWindowState(curr_idx, raw.curr_count - reclaimed, 0)
            else:
                reclaimed = min(tokens, raw.curr_count)
                updated = _SlidingWindowState(curr_idx, raw.prev_count, raw.curr_count - reclaimed)
            self._save(key, updated, period_ns, now)
            return reclaimed


@dataclass(frozen=True, slots=True)
class _BucketState:
    tokens: float
    last_refill_ns: int
    epoch: int


class MonotonicTokenBucketLimiter:
    """High-resolution monotonic token bucket with sub-second accuracy & fail-fast."""

    def __init__(self, store: HardenedMemoryStore) -> None:
        self._store = store

    def _available(
        self, key: str, capacity: int, refill_per_sec: float, now: int
    ) -> tuple[float, int, int]:
        raw = self._store.get(key, now_ns=now)
        if not isinstance(raw, _BucketState):
            return float(capacity), now, 0
        now = max(now, raw.last_refill_ns)
        elapsed_sec = (now - raw.last_refill_ns) / 1_000_000_000
        available = min(float(capacity), raw.tokens + elapsed_sec * refill_per_sec)
        return available, now, raw.epoch if available < capacity else 0

    def _save(
        self,
        key: str,
        available: float,
        capacity: int,
        refill_per_sec: float,
        now: int,
        epoch: int,
    ) -> None:
        self._store.set(
            key,
            _BucketState(available, now, epoch if available < capacity else 0),
            ttl_seconds=max(0.000000001, (capacity - available) / refill_per_sec),
            active_throttled=available < capacity,
            now_ns=now,
        )

    def check_and_consume(
        self,
        key: str,
        *,
        cost: int,
        capacity: int,
        refill_per_sec: float,
        now_ns: int | None = None,
    ) -> LimiterDecision:
        """Consume tokens atomically using monotonic nanoseconds."""
        if cost < 0 or capacity < 0 or not math.isfinite(refill_per_sec) or refill_per_sec <= 0:
            raise ValueError("invalid bucket parameters")
        if cost > capacity:
            raise OversizedRequestError(
                f"cost {cost} exceeds bucket capacity {capacity}; failing fast instead of spinning"
            )
        with self._store.lock:
            now = time.monotonic_ns() if now_ns is None else now_ns
            available, now, epoch = self._available(key, capacity, refill_per_sec, now)

            if available < float(cost):
                deficit = float(cost) - available
                retry_sec = deficit / refill_per_sec
                return LimiterDecision(
                    allowed=False,
                    remaining=max(0, int(math.floor(available))),
                    retry_after_seconds=max(0.001, math.ceil(retry_sec * 1000) / 1000),
                    current_used=capacity - int(math.floor(available)),
                    phantom_burn_prevented=cost,
                )

            new_tokens = available - float(cost)
            if cost:
                epoch = epoch or self._store.next_bucket_epoch()
                self._save(key, new_tokens, capacity, refill_per_sec, now, epoch)
            return LimiterDecision(
                allowed=True,
                remaining=max(0, int(math.floor(new_tokens))),
                retry_after_seconds=0.0,
                current_used=capacity - int(math.floor(new_tokens)),
                phantom_burn_prevented=0,
                bucket_epoch=epoch,
            )

    def refund(
        self,
        key: str,
        *,
        tokens: int,
        capacity: int,
        refill_per_sec: float,
        now_ns: int | None = None,
    ) -> int:
        """Credit unused tokens back into the monotonic token bucket up to capacity."""
        if tokens <= 0:
            return 0
        with self._store.lock:
            now = time.monotonic_ns() if now_ns is None else now_ns
            available, now, epoch = self._available(key, capacity, refill_per_sec, now)
            credited = min(float(tokens), float(capacity) - available)
            if credited:
                self._save(key, available + credited, capacity, refill_per_sec, now, epoch)
            return int(math.floor(credited))

    def adjust(
        self,
        key: str,
        *,
        tokens_delta: int,
        capacity: int,
        refill_per_sec: float,
        now_ns: int | None = None,
        refund_epoch: int | None = None,
    ) -> float:
        """Adjust for settlement, preserving debt until the bucket refills."""
        with self._store.lock:
            now = time.monotonic_ns() if now_ns is None else now_ns
            available, now, epoch = self._available(key, capacity, refill_per_sec, now)
            if tokens_delta > 0 and refund_epoch is not None and refund_epoch != epoch:
                return available
            adjusted = min(float(capacity), available + tokens_delta)
            if adjusted != available:
                epoch = epoch or self._store.next_bucket_epoch()
                self._save(key, adjusted, capacity, refill_per_sec, now, epoch)
            return adjusted


class MonotonicGCRALimiter:
    """Nanosecond-exact Generic Cell Rate Algorithm limiter with zero rounding drop."""

    def __init__(self, store: HardenedMemoryStore) -> None:
        self._store = store

    def check_and_consume(
        self,
        key: str,
        *,
        cost: int,
        capacity: int,
        emission_interval_sec: float,
        now_ns: int | None = None,
    ) -> LimiterDecision:
        """Advance Theoretical Arrival Time atomically in exact integer nanoseconds."""
        if (
            cost < 0
            or capacity < 1
            or not math.isfinite(emission_interval_sec)
            or emission_interval_sec <= 0
        ):
            raise ValueError("invalid GCRA parameters")
        if cost > capacity:
            raise OversizedRequestError(f"cost {cost} exceeds burst capacity {capacity}")

        emission_ns = max(1, int(round(emission_interval_sec * 1_000_000_000)))
        delay_tolerance_ns = emission_ns * capacity

        with self._store.lock:
            now = time.monotonic_ns() if now_ns is None else now_ns
            tat_raw = self._store.get(key, now_ns=now)
            tat_ns = int(tat_raw) if isinstance(tat_raw, int) else now
            new_tat_ns = max(tat_ns, now) + cost * emission_ns
            allow_at_ns = new_tat_ns - delay_tolerance_ns
            if allow_at_ns > now:
                retry_sec = (allow_at_ns - now) / 1_000_000_000.0
                used_units = min(capacity, int(math.ceil(max(0, tat_ns - now) / emission_ns)))
                return LimiterDecision(
                    allowed=False,
                    remaining=max(0, capacity - used_units),
                    retry_after_seconds=round(max(0.001, retry_sec), 3),
                    current_used=used_units,
                    phantom_burn_prevented=cost,
                )

            if cost:
                self._store.set(
                    key,
                    new_tat_ns,
                    ttl_seconds=(new_tat_ns - now) / 1_000_000_000,
                    active_throttled=True,
                    now_ns=now,
                )
            used_after = min(capacity, int(max(0, new_tat_ns - now) // emission_ns))
            return LimiterDecision(
                allowed=True,
                remaining=max(0, capacity - used_after),
                retry_after_seconds=0.0,
                current_used=used_after,
                phantom_burn_prevented=0,
            )


@dataclass(frozen=True, slots=True)
class UpstreamProbeOutcome:
    """Honest unmodified upstream evaluation output (never masks failures)."""

    admitted: int
    rejected: int
    errors: int
    upstream_supported: bool
    upstream_error: str | None
    phantom_burn_units: int
    evicted_active_keys: int
    admitted_by_tenant: dict[str, int]
    decision_latencies_ms: list[float]


class _ReplayMemoryBackend(MemoryStoreBackend):  # type: ignore[misc]
    """Supply a virtual clock and observe evictions in the real upstream store."""

    now_ms = 0
    active_evictions = 0

    def expire(self, key: str, timeout: int) -> None:
        self.expire_info[key] = self.now_ms / 1_000 + timeout

    def ttl(self, key: str) -> int:
        expiry = self.expire_info.get(key)
        if expiry is None:
            return -1 if self.exists(key) else -2
        seconds_left = expiry - self.now_ms / 1_000
        return math.ceil(seconds_left) if seconds_left > 0 else -2

    def check_and_evict(self, key: str) -> None:
        candidate = None
        client = self.get_client()
        if len(client) >= self.max_size and key not in client:
            candidate = next(iter(client))
        live = candidate is not None and not self.has_expired(candidate)
        super().check_and_evict(key)
        if live and candidate not in client:
            self.active_evictions += 1


def run_upstream_baseline_workload(
    *,
    events: list[dict[str, Any]],
    rpm_limit: int,
    tpm_limit: int,
    window_seconds: int,
) -> UpstreamProbeOutcome:
    """Execute workload against verbatim upstream throttled-py v3.5.0 without masking errors."""
    backend = _ReplayMemoryBackend()
    limiter = MemoryLimitAtomicAction(backend=backend)

    admitted = 0
    rejected = 0
    errors = 0
    phantom_burn_units = 0
    latencies_ms: list[float] = []
    admitted_by_tenant: dict[str, int] = {}
    upstream_error = "Upstream replay provides RPM admission only"
    try:
        UpstreamThrottled(quota=f"{rpm_limit}/min,{tpm_limit}/min")
    except UpstreamDataError:
        upstream_error += "; dual quota rejected with DataError"

    for idx, ev in enumerate(events):
        tenant = str(ev.get("tenant_id", "default"))
        key = f"throttled:v1:sliding_window:tenant:{tenant}:period"
        backend.now_ms = idx
        window = idx // (window_seconds * 1_000)
        current_key = f"{key}:{window}"
        previous_key = f"{key}:{window - 1}"
        client = backend.get_client()
        before = 0 if backend.has_expired(current_key) else int(client[current_key])

        t0 = time.perf_counter_ns()
        try:
            limited, _, _ = limiter.do(
                [current_key, previous_key],
                [window_seconds, rpm_limit, 1, idx],
            )
            if limited:
                rejected += 1
                after = int(client.get(current_key, 0))
                phantom_burn_units += max(0, after - before)
            else:
                admitted += 1
                admitted_by_tenant[tenant] = admitted_by_tenant.get(tenant, 0) + 1
        except UpstreamDataError:
            errors += 1
        finally:
            latencies_ms.append((time.perf_counter_ns() - t0) / 1_000_000.0)
    if errors:
        upstream_error += "; upstream RPM evaluation reported a DataError"

    return UpstreamProbeOutcome(
        admitted=admitted,
        rejected=rejected,
        errors=errors,
        upstream_supported=False,
        upstream_error=upstream_error,
        phantom_burn_units=phantom_burn_units,
        evicted_active_keys=backend.active_evictions,
        admitted_by_tenant=admitted_by_tenant,
        decision_latencies_ms=latencies_ms,
    )
