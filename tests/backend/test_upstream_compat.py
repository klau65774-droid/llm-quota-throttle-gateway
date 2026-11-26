"""Upstream throttled-py compatibility & concurrency correctness verification."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from llm_gateway.compat import (
    FixedSlidingWindowLimiter,
    HardenedMemoryStore,
    MonotonicGCRALimiter,
    MonotonicTokenBucketLimiter,
    UpstreamMemoryStore,
    UpstreamRateLimiterType,
    UpstreamThrottled,
    upstream_rate_limiter,
)


def test_upstream_baseline_throttled_smoke() -> None:
    """Ensure unmodified upstream_baseline library works cleanly on MemoryStore."""
    store = UpstreamMemoryStore()
    limiter = UpstreamThrottled(
        using=UpstreamRateLimiterType.TOKEN_BUCKET.value,
        quota=upstream_rate_limiter.per_min(5),
        store=store,
    )
    first = limiter.limit("smoke-key", cost=2)
    assert not first.limited
    assert first.state.remaining == 3

    blocked = limiter.limit("smoke-key", cost=4)
    assert blocked.limited


def test_enhanced_limiters_basic_and_concurrent_gcra() -> None:
    """Verify monotonic limiters handle 100 concurrent threads without GCRA race drops."""
    store = HardenedMemoryStore(max_keys=512)
    gcra = MonotonicGCRALimiter(store)
    sw = FixedSlidingWindowLimiter(store)
    tb = MonotonicTokenBucketLimiter(store)

    def _hit_gcra(i: int) -> bool:
        _ = i
        return gcra.check_and_consume(
            "concurrent-gcra", cost=1, capacity=100, emission_interval_sec=0.6
        ).allowed

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(_hit_gcra, range(100)))

    assert sum(1 for ok in results if ok) == 100
    assert not gcra.check_and_consume(
        "concurrent-gcra", cost=1, capacity=100, emission_interval_sec=0.6
    ).allowed

    sw_ok = sw.check_and_consume("sw-key", cost=2, limit=5, period_seconds=60)
    assert sw_ok.allowed and sw_ok.remaining == 3
    tb_ok = tb.check_and_consume("tb-key", cost=100, capacity=500, refill_per_sec=10.0)
    assert tb_ok.allowed and tb_ok.remaining == 400
