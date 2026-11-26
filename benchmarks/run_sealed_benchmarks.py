"""Replay pinned public inference traces and verify local gateway invariants."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import sys
import time
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from heapq import merge
from itertools import islice
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[1]
_BACKEND_DIR = _REPO_ROOT / "backend"
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

from llm_gateway.compat import (  # noqa: E402
    FixedSlidingWindowLimiter,
    HardenedMemoryStore,
    MonotonicTokenBucketLimiter,
    OversizedRequestError,
    StoreCapacityError,
)
from llm_gateway.config import GatewaySettings  # noqa: E402
from llm_gateway.fairness import (  # noqa: E402
    TenantDemandSpec,
    run_fairness_and_failover_simulation,
)
from llm_gateway.redaction import evaluate_redaction  # noqa: E402

_NS_PER_SECOND = 1_000_000_000
_PERIOD_SECONDS = 60
_COMPLETION_BUDGET = 2_048
_REDACTION_BUDGET_MS = 50.0
_AZURE_EPOCH = datetime(1970, 1, 1)


@dataclass(frozen=True, slots=True)
class TraceSource:
    filename: str
    git_blob: str
    sha256: str
    size_bytes: int
    columns: tuple[str, ...]
    rows_to_replay: int
    require_full_file: bool = True

    def provenance(self) -> dict[str, str | int]:
        return {"git_blob": self.git_blob, "sha256": self.sha256, "size_bytes": self.size_bytes}


_AZURE_COLUMNS = ("TIMESTAMP", "ContextTokens", "GeneratedTokens")
_AZURE_CODE = TraceSource(
    "AzureLLMInferenceTrace_code.csv",
    "62bd8109d0f2b1bc48206d397ecca48d8de03aab",
    "54e9a6d2a4bd06ba1e060304b900abbc74cbea53de96506e60fe5bb4f2277fb6",
    320_117,
    _AZURE_COLUMNS,
    8_819,
)
_AZURE_CONV = TraceSource(
    "AzureLLMInferenceTrace_conv.csv",
    "5b9219f2caf93bbbf95ec29894e206115658e235",
    "2f1e5b666d4e3055fdbba98598ce2ec307767b9064e03e2fa46676dbcc7d0bf8",
    719_188,
    _AZURE_COLUMNS,
    19_366,
)
_BURST = TraceSource(
    "BurstGPT_1.csv",
    "f95dd2c84454b067fdd0973858c55ecfe31bdad7",
    "46fc9480ef0b748ecb2b51d512ff08c196b031782cbe6f78e28044d768e86d5a",
    50_853_373,
    ("Timestamp", "Model", "Request tokens", "Response tokens", "Total tokens", "Log Type"),
    50_000,
    require_full_file=False,
)


class BenchmarkInputError(ValueError):
    """A required external trace is absent or fails provenance or format checks."""


@dataclass(frozen=True, slots=True)
class TraceEvent:
    timestamp_ns: int
    tenant: str
    prompt_tokens: int
    completion_tokens: int


def _load_rows(sealed_dir: Path, source: TraceSource) -> list[list[str]]:
    try:
        with (sealed_dir / source.filename).open("rb") as binary:
            payload = binary.read(source.size_bytes + 1)
    except OSError:
        raise BenchmarkInputError(f"{source.filename}: unavailable") from None

    if len(payload) != source.size_bytes or hashlib.sha256(payload).hexdigest() != source.sha256:
        raise BenchmarkInputError(f"{source.filename}: differs from pinned publisher bytes")

    try:
        with io.TextIOWrapper(io.BytesIO(payload), encoding="utf-8", newline="") as stream:
            reader = csv.reader(stream, strict=True)
            if tuple(next(reader, [])) != source.columns:
                raise BenchmarkInputError(f"{source.filename}: unexpected CSV header")
            limit = source.rows_to_replay + int(source.require_full_file)
            rows = list(islice(reader, limit))
    except (UnicodeError, csv.Error):
        raise BenchmarkInputError(f"{source.filename}: invalid UTF-8 or CSV") from None

    if len(rows) != source.rows_to_replay:
        raise BenchmarkInputError(f"{source.filename}: unexpected number of rows")
    return rows


def _nonnegative_int(value: str) -> int:
    if not value.isascii() or not value.isdecimal():
        raise ValueError("expected a nonnegative integer")
    return int(value)


def _azure_timestamp_ns(value: str) -> int:
    whole_seconds, separator, fraction = value.partition(".")
    if not separator or len(fraction) != 7 or len(whole_seconds) != 19:
        raise ValueError("expected an Azure timestamp with 100 ns precision")
    elapsed = datetime.fromisoformat(whole_seconds) - _AZURE_EPOCH
    return (elapsed.days * 86_400 + elapsed.seconds) * _NS_PER_SECOND + (
        _nonnegative_int(fraction) * 100
    )


def _load_azure(sealed_dir: Path, source: TraceSource, service: str) -> list[TraceEvent]:
    events = []
    previous_ns = -1
    for idx, row in enumerate(_load_rows(sealed_dir, source)):
        try:
            timestamp, prompt, completion = row
            timestamp_ns = _azure_timestamp_ns(timestamp)
            if timestamp_ns < previous_ns:
                raise ValueError("out-of-order timestamp")
            events.append(
                TraceEvent(
                    timestamp_ns,
                    f"{service}:{idx % 8}",
                    _nonnegative_int(prompt),
                    _nonnegative_int(completion),
                )
            )
            previous_ns = timestamp_ns
        except ValueError:
            raise BenchmarkInputError(f"{source.filename}: invalid row {idx + 2}") from None
    return events


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return round(ordered[math.ceil(q * len(ordered)) - 1], 4)


def _replay(events: Iterable[TraceEvent], *, rpm_limit: int, tpm_limit: int) -> dict[str, Any]:
    store = HardenedMemoryStore(max_keys=4_096)
    rpm = FixedSlidingWindowLimiter(store)
    tpm = MonotonicTokenBucketLimiter(store)
    refill_per_sec = tpm_limit / _PERIOD_SECONDS
    first_timestamp_ns: int | None = None
    now_ns = 0
    admitted = rejected = oversized = 0
    reserved_total = settled_total = refunded_total = overrun_total = 0
    rollback_failures = settle_failures = 0
    latencies_ms: list[float] = []
    t_start = time.perf_counter()

    for event in events:
        if first_timestamp_ns is None:
            first_timestamp_ns = event.timestamp_ns
        now_ns = event.timestamp_ns - first_timestamp_ns
        reserved = event.prompt_tokens + _COMPLETION_BUDGET
        actual = event.prompt_tokens + event.completion_tokens
        rpm_key = f"rpm:{event.tenant}"
        tpm_key = f"tpm:{event.tenant}"
        remaining_tpm: int | None = None
        t0 = time.perf_counter_ns()
        rpm_decision = rpm.check_and_consume(
            rpm_key, cost=1, limit=rpm_limit, period_seconds=_PERIOD_SECONDS, now_ns=now_ns
        )
        if rpm_decision.allowed:
            try:
                tpm_decision = tpm.check_and_consume(
                    tpm_key,
                    cost=reserved,
                    capacity=tpm_limit,
                    refill_per_sec=refill_per_sec,
                    now_ns=now_ns,
                )
                if tpm_decision.allowed:
                    remaining_tpm = tpm_decision.remaining
            except OversizedRequestError:
                oversized += 1
            if remaining_tpm is None:
                reclaimed = rpm.refund(
                    rpm_key,
                    tokens=1,
                    limit=rpm_limit,
                    period_seconds=_PERIOD_SECONDS,
                    now_ns=now_ns,
                )
                rollback_failures += int(reclaimed != 1)

        if remaining_tpm is not None:
            admitted += 1
            reserved_total += reserved
            settled_total += actual
            adjustment = reserved - actual
            if adjustment:
                settled_balance = tpm.adjust(
                    tpm_key,
                    tokens_delta=adjustment,
                    capacity=tpm_limit,
                    refill_per_sec=refill_per_sec,
                    now_ns=now_ns,
                )
                # The admission API reports integer remaining tokens, so the fractional
                # part of the exact balance can only place it within the next token.
                expected_floor = remaining_tpm + adjustment
                if expected_floor - 1e-8 <= settled_balance < expected_floor + 1 + 1e-8:
                    refunded_total += max(0, adjustment)
                    overrun_total += max(0, -adjustment)
                else:
                    settle_failures += 1
        else:
            rejected += 1
        latencies_ms.append((time.perf_counter_ns() - t0) / 1_000_000)

    elapsed = max(0.001, time.perf_counter() - t_start)
    ratio = round(100 * refunded_total / reserved_total, 2) if reserved_total else 0.0
    return {
        "admitted": admitted,
        "rejected": rejected,
        "oversized_rejected": oversized,
        "tpm_reserved_total": reserved_total,
        "tpm_actual_settled_total": settled_total,
        "tpm_refunded_reclaimed": refunded_total,
        "tpm_overrun_charged": overrun_total,
        "reclamation_ratio_pct": ratio,
        "throughput_req_per_sec": round(len(latencies_ms) / elapsed, 1),
        "p50_decision_ms": _percentile(latencies_ms, 0.50),
        "p99_decision_ms": _percentile(latencies_ms, 0.99),
        "duration_trace_seconds": round(now_ns / _NS_PER_SECOND, 7),
        "accounting_consistent": (
            rollback_failures == settle_failures == 0
            and reserved_total - refunded_total + overrun_total == settled_total
        ),
        "rpm_rollback_failures": rollback_failures,
        "tpm_settlement_adjustment_failures": settle_failures,
    }


def _run_tier1a_azure(sealed_dir: Path) -> dict[str, Any]:
    code = _load_azure(sealed_dir, _AZURE_CODE, "azure-code")
    conv = _load_azure(sealed_dir, _AZURE_CONV, "azure-conversation")
    replay = _replay(
        merge(code, conv, key=lambda event: event.timestamp_ns),
        rpm_limit=600,
        tpm_limit=1_500_000,
    )
    return {
        "datasets": [_AZURE_CODE.filename, _AZURE_CONV.filename],
        "input_provenance": {
            _AZURE_CODE.filename: _AZURE_CODE.provenance(),
            _AZURE_CONV.filename: _AZURE_CONV.provenance(),
        },
        "dataset_rows": {_AZURE_CODE.filename: len(code), _AZURE_CONV.filename: len(conv)},
        "total_requests": len(code) + len(conv),
        **replay,
    }


def _run_tier1b_burstgpt(sealed_dir: Path) -> dict[str, Any]:
    events = []
    model_counts: Counter[str] = Counter()
    zero_responses = 0
    previous_ns = -1
    for idx, row in enumerate(_load_rows(sealed_dir, _BURST)):
        try:
            timestamp, model, prompt, completion, total, log_type = row
            timestamp_ns = _nonnegative_int(timestamp) * _NS_PER_SECOND
            prompt_tokens = _nonnegative_int(prompt)
            completion_tokens = _nonnegative_int(completion)
            if (
                timestamp_ns < previous_ns
                or model not in {"ChatGPT", "GPT-4"}
                or not log_type
                or _nonnegative_int(total) != prompt_tokens + completion_tokens
            ):
                raise ValueError("invalid order or event fields")
        except ValueError:
            raise BenchmarkInputError(f"{_BURST.filename}: invalid row {idx + 2}") from None
        events.append(
            TraceEvent(timestamp_ns, f"{model}:{idx % 12}", prompt_tokens, completion_tokens)
        )
        previous_ns = timestamp_ns
        model_counts[model] += 1
        zero_responses += int(completion_tokens == 0)

    replay = _replay(events, rpm_limit=500, tpm_limit=1_200_000)
    return {
        "dataset": _BURST.filename,
        "input_provenance": {_BURST.filename: _BURST.provenance()},
        "slice_events": len(events),
        "model_distribution": dict(model_counts),
        "zero_response_failed_events": zero_responses,
        "tokens_reclaimed_total": replay["tpm_refunded_reclaimed"],
        **replay,
    }


def _check_sliding_window(now_ns: int) -> dict[str, Any]:
    limiter = FixedSlidingWindowLimiter(HardenedMemoryStore(max_keys=8))
    first = limiter.check_and_consume("window", cost=3, limit=5, period_seconds=60, now_ns=now_ns)
    unexpected = int(not first.allowed or first.current_used != 3)
    rejected = 0
    for _ in range(500):
        decision = limiter.check_and_consume(
            "window", cost=3, limit=5, period_seconds=60, now_ns=now_ns
        )
        rejected += int(not decision.allowed)
        unexpected += int(decision.allowed or decision.current_used != 3)
    final = limiter.check_and_consume("window", cost=2, limit=5, period_seconds=60, now_ns=now_ns)
    unexpected += int(not final.allowed or final.current_used != 5)
    refund = limiter.refund("window", tokens=20, limit=5, period_seconds=60, now_ns=now_ns)
    repeated = limiter.refund("window", tokens=20, limit=5, period_seconds=60, now_ns=now_ns)
    after = limiter.check_and_consume("window", cost=0, limit=5, period_seconds=60, now_ns=now_ns)
    return {
        "enhanced_phantom_burn_count": unexpected,
        "sliding_window_rejections_checked": rejected,
        "sliding_window_refund_bounded": refund == 5 and repeated == 0 and after.current_used == 0,
    }


def _check_capacity(now_ns: int) -> dict[str, Any]:
    max_keys, active_keys = 256, 64
    expired_keys = max_keys - active_keys
    store = HardenedMemoryStore(max_keys=max_keys)
    limiter = FixedSlidingWindowLimiter(store)
    seeded = sum(
        limiter.check_and_consume(
            f"active:{idx}", cost=3, limit=3, period_seconds=60, now_ns=now_ns
        ).allowed
        for idx in range(active_keys)
    )
    for idx in range(expired_keys):
        store.set(f"expiring:{idx}", 1, ttl_seconds=0.5, now_ns=now_ns)

    later_ns = now_ns + _NS_PER_SECOND
    store.set("replacement", 1, ttl_seconds=30, now_ns=later_ns)
    sweep = store.snapshot(now_ns=later_ns)
    for idx in range(3_000):
        store.set(f"idle:{idx}", 1, ttl_seconds=30, now_ns=later_ns)
    retained = sum(
        not limiter.check_and_consume(
            f"active:{idx}", cost=1, limit=3, period_seconds=60, now_ns=later_ns
        ).allowed
        for idx in range(active_keys)
    )
    pressure = store.snapshot(now_ns=later_ns)

    saturated = HardenedMemoryStore(max_keys=1)
    saturated.set("active", 1, ttl_seconds=30, active_throttled=True, now_ns=now_ns)
    capacity_rejected = False
    try:
        saturated.set("new", 1, ttl_seconds=30, now_ns=now_ns)
    except StoreCapacityError:
        capacity_rejected = True
    return {
        "cache_thrash_expired_swept": sweep.expired_swept_total,
        "cache_thrash_sweep_before_evict": (
            seeded == active_keys
            and sweep.expired_swept_total == expired_keys
            and sweep.idle_evicted_total == sweep.active_evicted_total == 0
            and sweep.current_keys == active_keys + 1
        ),
        "cache_thrash_idle_evicted": pressure.idle_evicted_total,
        "cache_thrash_active_evicted": pressure.active_evicted_total,
        "cache_thrash_active_expected": active_keys,
        "cache_thrash_active_retained": retained,
        "cache_thrash_capacity_reached": pressure.current_keys == pressure.max_keys,
        "saturated_active_store_fails_closed": (
            capacity_rejected and saturated.get("active", now_ns=now_ns) == 1
        ),
    }


def _check_token_bucket(now_ns: int) -> dict[str, Any]:
    limiter = MonotonicTokenBucketLimiter(HardenedMemoryStore(max_keys=8))
    t0 = time.perf_counter_ns()
    oversized_caught = 0
    for _ in range(100):
        try:
            limiter.check_and_consume(
                "refill", cost=11, capacity=10, refill_per_sec=4.0, now_ns=now_ns
            )
        except OversizedRequestError:
            oversized_caught += 1
    oversized_ms = round((time.perf_counter_ns() - t0) / 1_000_000, 4)
    drained = limiter.check_and_consume(
        "refill", cost=10, capacity=10, refill_per_sec=4.0, now_ns=now_ns
    ).allowed
    too_soon = limiter.check_and_consume(
        "refill", cost=2, capacity=10, refill_per_sec=4.0, now_ns=now_ns + 250_000_000
    ).allowed
    on_time = limiter.check_and_consume(
        "refill", cost=2, capacity=10, refill_per_sec=4.0, now_ns=now_ns + 500_000_000
    ).allowed

    initial = limiter.check_and_consume(
        "settle", cost=6, capacity=8, refill_per_sec=2.0, now_ns=now_ns
    ).allowed
    charged = limiter.adjust(
        "settle", tokens_delta=-6, capacity=8, refill_per_sec=2.0, now_ns=now_ns
    )
    before_refill = limiter.check_and_consume(
        "settle", cost=1, capacity=8, refill_per_sec=2.0, now_ns=now_ns + 2_000_000_000
    ).allowed
    after_refill = limiter.check_and_consume(
        "settle", cost=1, capacity=8, refill_per_sec=2.0, now_ns=now_ns + 2_500_000_000
    ).allowed
    return {
        "subsecond_monotonic_refill_pass": drained and not too_soon and on_time,
        "oversized_fail_fast_caught": oversized_caught,
        "oversized_100x_elapsed_ms": oversized_ms,
        "settlement_overrun_debt_observed": (
            initial and charged == -4.0 and not before_refill and after_refill
        ),
    }


def _run_synthetic_fairness() -> tuple[dict[str, Any], dict[str, Any]]:
    tenants = [
        TenantDemandSpec(tenant_id="batch", weight=1, demand_rpm=60, avg_tokens=600),
        TenantDemandSpec(tenant_id="interactive", weight=4, demand_rpm=10, avg_tokens=450),
        TenantDemandSpec(tenant_id="stream", weight=3, demand_rpm=12, avg_tokens=400),
    ]
    no_faults, faulted = (
        run_fairness_and_failover_simulation(
            tenants=tenants,
            total_tpm_capacity=18_000,
            quantum_tokens=800,
            rounds=12,
            inject_provider_429=inject_faults,
        )
        for inject_faults in (False, True)
    )
    return (
        {
            "jains_fairness_fifo": no_faults.jains_fairness_baseline_fifo,
            "jains_fairness_drr": no_faults.jains_fairness_drr,
            "starvation_events_fifo": no_faults.starvation_events_fifo,
            "starvation_events_drr": no_faults.starvation_events_drr,
            "drr_served_all_tenants": (
                len(no_faults.tenant_outcomes) == len(tenants)
                and all(tenant.drr_requests_served > 0 for tenant in no_faults.tenant_outcomes)
            ),
        },
        {
            "failover_events_triggered": len(faulted.failover_events),
            "drr_jains_under_faults": faulted.jains_fairness_drr,
            "fifo_jains_under_faults": faulted.jains_fairness_baseline_fifo,
            "primary_faults_observed": sum(
                p.total_429_5xx for p in faulted.provider_health if p.role == "primary"
            ),
            "backup_requests_routed": sum(
                p.routed_requests for p in faulted.provider_health if p.role == "backup"
            ),
        },
    )


def _run_tier3_redaction() -> dict[str, Any]:
    bearer = "synthetic.benchmark.token"
    samples = (
        ("api_token_prefix", "sk-" + "SyntheticCheckFixtureA"),
        ("api_token_prefix", "ghp_" + "SyntheticCheckFixtureB"),
        ("bearer_authorization", bearer),
        ("pem_private_key", "-----BEGIN " + "RSA PRIVATE KEY-----"),
        ("email_address", "benchmark" + "@" + "example.org"),
        ("structured_gov_id", "-".join(("000", "00", "0000"))),
    )
    benign = (
        "Summarize quarterly utilization for the sample workload.",
        "The documented order identifier was " + "0" * 11 + ".",
        "Count email domains in counts.csv at the next review.",
    )
    detected = 0
    latencies = []
    for rule, secret in samples:
        text = f"Authorization: Bearer {secret}" if rule == "bearer_authorization" else secret
        result = evaluate_redaction(text, mode="mask")
        latencies.append(result.latency_ms)
        detected += int(
            result.allowed
            and [(item.rule, item.count) for item in result.findings] == [(rule, 1)]
            and secret not in result.redacted_text
        )

    false_positives = 0
    for text in benign:
        result = evaluate_redaction(text, mode="mask")
        latencies.append(result.latency_ms)
        false_positives += int(
            not result.allowed or bool(result.findings) or result.redacted_text != text
        )

    blocked = evaluate_redaction(samples[0][1], mode="block")
    chunk = f"Authorization: Bearer {bearer} with benchmark request metadata.\n"
    small_n = 15_360 // len(chunk)
    large_n = 61_440 // len(chunk)
    small = evaluate_redaction(chunk * small_n, mode="mask")
    large = evaluate_redaction(chunk * large_n, mode="mask")
    return {
        "secret_detection_hits": detected,
        "secret_detection_targets": len(samples),
        "benign_false_positives": false_positives,
        "benign_targets": len(benign),
        "standard_max_latency_ms": max(latencies),
        "block_policy_check": not blocked.allowed and samples[0][1] not in blocked.redacted_text,
        "small_high_density_chars": len(chunk) * small_n,
        "small_high_density_latency_ms": small.latency_ms,
        "large_high_density_chars": len(chunk) * large_n,
        "large_high_density_latency_ms": large.latency_ms,
        "latency_budget_ms": _REDACTION_BUDGET_MS,
        "high_density_detection_pass": (
            small.total_hits == small_n
            and large.total_hits == large_n
            and bearer not in small.redacted_text
            and bearer not in large.redacted_text
        ),
    }


def _write_report(output: Path, report: dict[str, Any]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="\n", dir=output.parent, delete=False
        ) as stream:
            temporary_path = Path(stream.name)
            stream.write(json.dumps(report, indent=2, allow_nan=False) + "\n")
        temporary_path.replace(output)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=_REPO_ROOT / "docs" / "test_metrics.json",
        help="write the result atomically to this path",
    )
    output: Path = parser.parse_args().output
    sealed_dir = _REPO_ROOT / GatewaySettings().sealed_benchmark_dir
    input_errors = {}
    azure = burst = None
    try:
        azure = _run_tier1a_azure(sealed_dir)
    except BenchmarkInputError as exc:
        input_errors["tier_1a_azure_traces"] = str(exc)
    try:
        burst = _run_tier1b_burstgpt(sealed_dir)
    except BenchmarkInputError as exc:
        input_errors["tier_1b_burstgpt_50k"] = str(exc)

    now_ns = 960 * _NS_PER_SECOND
    fairness, failover = _run_synthetic_fairness()
    tier2 = {
        **_check_sliding_window(now_ns),
        **_check_capacity(now_ns),
        **_check_token_bucket(now_ns),
        **failover,
    }
    tier3 = _run_tier3_redaction()
    ci_gate = {
        "azure_trace_coverage_and_accounting": (
            azure is not None
            and azure["total_requests"] == _AZURE_CODE.rows_to_replay + _AZURE_CONV.rows_to_replay
            and azure["total_requests"] == azure["admitted"] + azure["rejected"]
            and azure["accounting_consistent"]
        ),
        "burstgpt_trace_coverage_and_accounting": (
            burst is not None
            and burst["slice_events"] == _BURST.rows_to_replay
            and burst["slice_events"] == burst["admitted"] + burst["rejected"]
            and burst["accounting_consistent"]
        ),
        "reserve_refund_tpm_reclaimed": (
            azure is not None
            and burst is not None
            and azure["tpm_refunded_reclaimed"] > 0
            and burst["tokens_reclaimed_total"] > 0
        ),
        "zero_phantom_quota_burn": (
            tier2["enhanced_phantom_burn_count"] == 0
            and tier2["sliding_window_rejections_checked"] == 500
        ),
        "sliding_window_refund_bounded": tier2["sliding_window_refund_bounded"],
        "expired_sweep_precedes_eviction": tier2["cache_thrash_sweep_before_evict"],
        "eviction_shield_zero_active_drop": (
            tier2["cache_thrash_capacity_reached"]
            and tier2["cache_thrash_active_retained"] == tier2["cache_thrash_active_expected"]
            and tier2["cache_thrash_idle_evicted"] > 0
            and tier2["cache_thrash_active_evicted"] == 0
            and tier2["saturated_active_store_fails_closed"]
        ),
        "subsecond_refill_and_oversized_guard": (
            tier2["subsecond_monotonic_refill_pass"] and tier2["oversized_fail_fast_caught"] == 100
        ),
        "settlement_overrun_debt_observed": tier2["settlement_overrun_debt_observed"],
        "drr_beats_fifo_fairness": (
            fairness["jains_fairness_drr"] > fairness["jains_fairness_fifo"]
            and fairness["starvation_events_drr"] <= fairness["starvation_events_fifo"]
            and fairness["drr_served_all_tenants"]
        ),
        "provider_failover_reaches_backup": (
            tier2["failover_events_triggered"] > 0
            and tier2["primary_faults_observed"] > 0
            and tier2["backup_requests_routed"] > 0
        ),
        "redaction_accuracy": (
            tier3["secret_detection_hits"] == tier3["secret_detection_targets"]
            and tier3["benign_false_positives"] == 0
            and tier3["block_policy_check"]
        ),
        "redaction_bounded_latency": (
            tier3["high_density_detection_pass"]
            and max(tier3["small_high_density_latency_ms"], tier3["large_high_density_latency_ms"])
            < _REDACTION_BUDGET_MS
        ),
    }
    overall_pass = bool(not input_errors and all(ci_gate.values()))
    status = "incomplete" if input_errors else ("passed" if overall_pass else "failed")
    report = {
        "schema_version": "2.0.0",
        "status": status,
        "overall_pass": overall_pass,
        "input_errors": input_errors,
        "trace_replay": {
            "clock": "source timestamps at original speed",
            "window_seconds": _PERIOD_SECONDS,
            "completion_reserve_cap_tokens": _COMPLETION_BUDGET,
        },
        "tier_1a_azure_traces": azure,
        "tier_1b_burstgpt_50k": burst,
        "tier_2_chaos_attacks": tier2,
        "synthetic_fairness": fairness,
        "tier_3_redaction_dos": tier3,
        "ci_regression_gate": ci_gate,
    }
    try:
        _write_report(output, report)
    except OSError:
        print("[INCOMPLETE] Unable to write the benchmark report", file=sys.stderr)
        return 2
    label = "OK" if overall_pass else status.upper()
    passed = sum(ci_gate.values())
    print(f"[{label}] Benchmark report written (status={status}, gates={passed}/{len(ci_gate)})")
    for tier, issue in input_errors.items():
        print(f"[INCOMPLETE] {tier}: {issue}", file=sys.stderr)
    for gate, passed_gate in ci_gate.items():
        if not passed_gate:
            print(f"[FAIL] {gate}", file=sys.stderr)
    if overall_pass:
        return 0
    return 2 if input_errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
