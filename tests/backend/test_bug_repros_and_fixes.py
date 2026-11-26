"""Regression tests reproducing upstream throttled-py flaws and verifying fixes."""

from __future__ import annotations

import time

import pytest
from llm_gateway.compat import (
    FixedSlidingWindowLimiter,
    HardenedMemoryStore,
    MonotonicTokenBucketLimiter,
    OversizedRequestError,
    StoreCapacityError,
    UpstreamDataError,
    UpstreamMemoryStore,
    UpstreamRateLimiterType,
    UpstreamThrottled,
    upstream_rate_limiter,
)


def test_bug1_phantom_quota_burn_fixed() -> None:
    """Denied requests must NOT increment sliding window usage counters."""
    # Upstream line 102 writes cost into current_key on a fresh window BEFORE checking limited,
    # permanently poisoning subsequent smaller requests even though nothing was admitted.
    up_store = UpstreamMemoryStore()
    up_limiter = UpstreamThrottled(
        using=UpstreamRateLimiterType.SLIDING_WINDOW.value,
        quota=upstream_rate_limiter.per_min(5),
        store=up_store,
    )
    first_try = up_limiter.limit("burn-user", cost=6)
    assert first_try.limited
    poisoned_follow_up = up_limiter.limit("burn-user", cost=1)
    assert poisoned_follow_up.limited  # Phantom burn: 0 admitted, yet cost=1 blocked!

    # Enhanced FixedSlidingWindowLimiter validates cost BEFORE writing usage
    store = HardenedMemoryStore(max_keys=128)
    limiter = FixedSlidingWindowLimiter(store)
    t0 = 1_000_000_000_000
    assert limiter.check_and_consume("k1", cost=3, limit=5, period_seconds=60, now_ns=t0).allowed
    denied = limiter.check_and_consume("k1", cost=3, limit=5, period_seconds=60, now_ns=t0)
    assert not denied.allowed
    assert denied.phantom_burn_prevented == 3
    assert denied.current_used == 3

    # Subsequent valid cost=2 STILL succeeds because denied cost=3 never mutated usage!
    valid_follow_up = limiter.check_and_consume("k1", cost=2, limit=5, period_seconds=60, now_ns=t0)
    assert valid_follow_up.allowed
    assert valid_follow_up.remaining == 0


def test_bug2_upstream_rejects_multi_quota_string() -> None:
    """Upstream Throttled fails on comma-separated dual RPM+TPM definitions."""
    with pytest.raises(UpstreamDataError):
        UpstreamThrottled(quota="10/min,5000/min")


def test_bug3_subsecond_monotonic_refill_and_oversized_fail_fast() -> None:
    """TokenBucket refills at sub-second granularity and rejects cost > capacity immediately."""
    store = HardenedMemoryStore(max_keys=64)
    tb = MonotonicTokenBucketLimiter(store)
    t0 = 10_000_000_000_000

    # Drain bucket of capacity 10 at 4 tokens/sec
    assert tb.check_and_consume(
        "subsec", cost=10, capacity=10, refill_per_sec=4.0, now_ns=t0
    ).allowed
    # After 500ms -> exactly 2.0 tokens refilled (upstream int sec floor lost this completely)
    d_500ms = tb.check_and_consume(
        "subsec", cost=2, capacity=10, refill_per_sec=4.0, now_ns=t0 + 500_000_000
    )
    assert d_500ms.allowed

    # Oversized cost > capacity fails fast in microseconds instead of spin-waiting
    t_wall = time.perf_counter_ns()
    with pytest.raises(OversizedRequestError):
        tb.check_and_consume("subsec", cost=11, capacity=10, refill_per_sec=4.0, now_ns=t0)
    elapsed_ms = (time.perf_counter_ns() - t_wall) / 1_000_000.0
    assert elapsed_ms < 25.0


def test_bug4_cache_thrashing_eviction_shield() -> None:
    """Flooding throwaway keys must sweep expired/idle entries before active throttled keys."""
    store = HardenedMemoryStore(max_keys=32)
    limiter = FixedSlidingWindowLimiter(store)
    t0 = 5_000_000_000_000

    # Exhaust throttled tenant quota
    for _ in range(3):
        assert limiter.check_and_consume(
            "vip-exhausted", cost=1, limit=3, period_seconds=60, now_ns=t0
        ).allowed

    # Flood 100 short-lived throwaway keys (1s TTL), advancing 2s so earlier ones expire
    for i in range(100):
        store.set(
            f"noise:{i}",
            1,
            ttl_seconds=1.0,
            active_throttled=False,
            now_ns=t0 + (i * 100_000_000),
        )

    # Throttled bucket MUST still be present and denied
    t_check = t0 + 10_500_000_000
    still_blocked = limiter.check_and_consume(
        "vip-exhausted", cost=1, limit=3, period_seconds=60, now_ns=t_check
    )
    assert not still_blocked.allowed
    snap = store.snapshot(now_ns=t_check)
    assert snap.active_evicted_total == 0
    assert snap.expired_swept_total > 0 or snap.idle_evicted_total > 0


def test_refund_stays_bounded_after_window_rollover() -> None:
    store = HardenedMemoryStore(max_keys=8)
    limiter = FixedSlidingWindowLimiter(store)
    key = "window"
    start = 60_000_000_000
    limit, period = 5, 60
    assert limiter.check_and_consume(
        key, cost=5, limit=limit, period_seconds=period, now_ns=start
    ).allowed

    rollover = start + 60_000_000_000
    denied = limiter.check_and_consume(
        key, cost=1, limit=limit, period_seconds=period, now_ns=rollover
    )
    assert not denied.allowed
    assert denied.current_used == 5
    assert denied.retry_after_seconds == pytest.approx(12.0)

    next_admission = rollover + 12_000_000_000
    assert limiter.check_and_consume(
        key, cost=1, limit=limit, period_seconds=period, now_ns=next_admission
    ).allowed
    assert limiter.refund(
        key, tokens=20, limit=limit, period_seconds=period, now_ns=next_admission
    ) == 1
    assert limiter.refund(
        key, tokens=20, limit=limit, period_seconds=period, now_ns=next_admission
    ) == 0
    assert limiter.check_and_consume(
        key, cost=1, limit=limit, period_seconds=period, now_ns=next_admission
    ).current_used == 5


def test_sliding_window_keeps_exact_integer_usage_at_fractional_boundary() -> None:
    limiter = FixedSlidingWindowLimiter(HardenedMemoryStore())
    start = 22_000_000_000
    at_boundary = 35_000_000_000
    assert limiter.check_and_consume(
        "exact", cost=77, limit=77, period_seconds=11, now_ns=start
    ).allowed
    # Two seconds into the next 11-second window, 9/11 * 77 is exactly 63.
    previous = limiter.check_and_consume(
        "exact", cost=0, limit=77, period_seconds=11, now_ns=at_boundary
    )
    assert (previous.current_used, previous.remaining) == (63, 14)
    boundary = limiter.check_and_consume(
        "exact", cost=14, limit=77, period_seconds=11, now_ns=at_boundary
    )
    assert boundary.allowed
    assert (boundary.current_used, boundary.remaining) == (77, 0)


def test_full_store_keeps_live_entries_until_expired_sweep() -> None:
    store = HardenedMemoryStore(max_keys=2)
    start = 10_000_000_000
    store.set("tenant-a", 1, 5.0, active_throttled=True, now_ns=start)
    store.set("tenant-b", 2, 10.0, active_throttled=True, now_ns=start)

    with pytest.raises(StoreCapacityError):
        store.set("tenant-c", 3, 10.0, active_throttled=True, now_ns=start)
    assert store.get("tenant-a", now_ns=start) == 1
    assert store.get("tenant-b", now_ns=start) == 2
    assert store.get("tenant-c", now_ns=start) is None

    after_expiry = start + 5_000_000_000
    store.set("tenant-c", 3, 10.0, active_throttled=True, now_ns=after_expiry)
    snapshot = store.snapshot(now_ns=after_expiry)
    assert snapshot.current_keys == 2
    assert snapshot.expired_swept_total == 1
    assert snapshot.active_evicted_total == 0
    assert store.get("tenant-b", now_ns=after_expiry) == 2
    assert store.get("tenant-c", now_ns=after_expiry) == 3
