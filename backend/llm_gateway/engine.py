"""Core orchestration engine uniting compat limiters, ledger, DRR, redaction, and OTLP."""

from __future__ import annotations

import json
import math
import time
import uuid
from pathlib import Path
from typing import Any

from llm_gateway.compat import (
    FixedSlidingWindowLimiter,
    HardenedMemoryStore,
    MonotonicTokenBucketLimiter,
    StoreCapacityError,
    run_upstream_baseline_workload,
)
from llm_gateway.config import GatewaySettings
from llm_gateway.fairness import (
    TenantDemandSpec,
    jains_fairness_index,
    run_fairness_and_failover_simulation,
)
from llm_gateway.quota import (
    DualQuotaCoordinator,
    QuotaLimits,
    compute_token_cost_usd,
)
from llm_gateway.redaction import RedactionMode, RedactionResult, evaluate_redaction
from llm_gateway.scenarios import SCENARIOS
from llm_gateway.storage import USD_SCALE, GatewayStorage, StoredReservation
from llm_gateway.tracer import GatewayTraceCollector, TraceSpanRecord


class UnknownScenarioError(ValueError):
    """The selected built-in workload does not exist."""


class ReplayRejected(RuntimeError):
    """An event was denied admission and its quota changes must roll back."""


def _percentile_99(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, math.ceil(0.99 * len(ordered)) - 1))
    return round(ordered[idx], 4)


class GatewayControlEngine:
    """Thread-safe façade powering all HTTP endpoints and deterministic evaluations."""

    def __init__(self, settings: GatewaySettings) -> None:
        self.settings = settings
        self.store = HardenedMemoryStore(max_keys=settings.store_max_keys)
        self.storage = GatewayStorage(settings.resolved_db_path())
        self.tracer = GatewayTraceCollector(service_name=settings.otel_service_name)
        self.quota = DualQuotaCoordinator(
            store=self.store,
            storage=self.storage,
            limits=QuotaLimits(),
            redaction_mode=settings.redaction_mode,
        )

    def close(self) -> None:
        """Close backing SQLite storage."""
        self.storage.close()

    def compare(
        self,
        *,
        scenario_id: str | None,
        requests: list[dict[str, Any]] | None,
        rpm_limit: int,
        tpm_limit: int,
        window_seconds: int,
        usd_budget_limit: float,
        fail_fast_oversized: bool,
    ) -> dict[str, Any]:
        """Compare upstream RPM with an isolated enhanced quota replay."""
        if requests is not None:
            selected_id = scenario_id or "custom-workload"
            events = requests
        else:
            scenario = SCENARIOS.get(scenario_id or "phantom-quota-burn")
            if scenario is None:
                raise UnknownScenarioError("Unknown scenario ID")
            selected_id = scenario.scenario_id
            events = scenario.events

        baseline = run_upstream_baseline_workload(
            events=events,
            rpm_limit=rpm_limit,
            tpm_limit=tpm_limit,
            window_seconds=window_seconds,
        )
        store = HardenedMemoryStore(max_keys=self.settings.store_max_keys)
        rpm = FixedSlidingWindowLimiter(store)
        tpm = MonotonicTokenBucketLimiter(store)
        refill = tpm_limit / window_seconds

        run_id = f"cmp_{uuid.uuid4().hex}"
        trace_id = self.tracer.new_trace_id()
        spans: list[TraceSpanRecord] = []
        preview: list[dict[str, Any]] = []
        latencies_ms: list[float] = []
        demand: dict[str, int] = {}
        admitted: dict[str, int] = {}
        tenant_cost: dict[tuple[str, int], int] = {}
        budget_units = round(usd_budget_limit * USD_SCALE)
        tpm_reclaimed = 0
        actual_tpm_settled = 0
        total_cost_units = 0
        phantom_units = 0

        for idx, ev in enumerate(events):
            event_id = str(ev.get("event_id") or f"ev-{idx + 1:03d}")
            tenant = str(ev.get("tenant_id", "default"))
            model = str(ev.get("model") or self.settings.default_model)
            prompt = int(ev.get("prompt_tokens", 0))
            max_comp = int(ev.get("max_completion_tokens", 0))
            actual_comp = int(ev.get("actual_completion_tokens", max_comp))
            failed = int(ev.get("provider_status", 200)) >= 400
            reserved = prompt + max_comp
            actual_prompt = 0 if failed and not actual_comp else prompt
            usage = actual_prompt + actual_comp
            actual_cost = compute_token_cost_usd(model, actual_prompt, actual_comp)
            cost_units = round(actual_cost * USD_SCALE)
            est_units = round(compute_token_cost_usd(model, prompt, max_comp) * USD_SCALE)
            demand[tenant] = demand.get(tenant, 0) + 1
            rpm_key = f"rpm:{tenant}"
            tpm_key = f"tpm:{tenant}"
            now = idx * 1_000_000
            tenant_window = (tenant, idx // (window_seconds * 1_000))
            spent = tenant_cost.get(tenant_window, 0)
            before = rpm.check_and_consume(
                rpm_key,
                cost=0,
                limit=rpm_limit,
                period_seconds=window_seconds,
                now_ns=now,
            ).current_used
            t_start = time.perf_counter_ns()
            reason = "admitted"
            try:
                with store.rollback_on_error((rpm_key, tpm_key)):
                    if reserved > tpm_limit:
                        raise ReplayRejected(
                            "oversized_fail_fast" if fail_fast_oversized else "tpm_limit_exceeded"
                        )
                    if spent + est_units > budget_units:
                        raise ReplayRejected("usd_budget_exceeded")
                    if not rpm.check_and_consume(
                        rpm_key,
                        cost=1,
                        limit=rpm_limit,
                        period_seconds=window_seconds,
                        now_ns=now,
                    ).allowed:
                        raise ReplayRejected("rpm_limit_exceeded")
                    if not tpm.check_and_consume(
                        tpm_key,
                        cost=reserved,
                        capacity=tpm_limit,
                        refill_per_sec=refill,
                        now_ns=now,
                    ).allowed:
                        raise ReplayRejected("tpm_limit_exceeded")
                    tpm.adjust(
                        tpm_key,
                        tokens_delta=reserved - usage,
                        capacity=tpm_limit,
                        refill_per_sec=refill,
                        now_ns=now,
                    )
            except ReplayRejected as exc:
                reason = str(exc)
            except StoreCapacityError:
                reason = "quota_store_at_capacity"
            elapsed_ns = time.perf_counter_ns() - t_start
            latencies_ms.append(elapsed_ns / 1_000_000)
            allowed = reason == "admitted"

            refunded = max(0, reserved - usage) if allowed else 0
            if allowed:
                admitted[tenant] = admitted.get(tenant, 0) + 1
                tpm_reclaimed += refunded
                actual_tpm_settled += usage
                tenant_cost[tenant_window] = spent + cost_units
                total_cost_units += cost_units
                if failed:
                    reason = "settled_provider_error"
            else:
                after = rpm.check_and_consume(
                    rpm_key,
                    cost=0,
                    limit=rpm_limit,
                    period_seconds=window_seconds,
                    now_ns=now,
                ).current_used
                phantom_units += max(0, after - before)

            if len(preview) < 50:
                shown_event = evaluate_redaction(event_id).redacted_text
                shown_tenant = evaluate_redaction(tenant).redacted_text
                preview.append(
                    {
                        "event_id": shown_event,
                        "tenant_id": shown_tenant,
                        "model": model,
                        "reserved_tokens": reserved,
                        "actual_tokens": usage if allowed else 0,
                        "refunded_tokens": refunded,
                        "enhanced_allowed": allowed,
                        "enhanced_reason": reason,
                    }
                )
                spans.append(
                    self.tracer.build_span(
                        trace_id=trace_id,
                        name=f"gateway.compare.{reason}",
                        duration_ns=max(1_000, elapsed_ns),
                        attributes={
                            "event.id": shown_event,
                            "tenant.id": shown_tenant,
                            "llm.model": model,
                            "quota.reserved_tokens": reserved,
                            "quota.refunded_tokens": refunded,
                            "decision.allowed": allowed,
                            "decision.reason": reason,
                        },
                        status_code="OK" if allowed and not failed else "ERROR",
                    )
                )

        fifo_jain = jains_fairness_index(
            [baseline.admitted_by_tenant.get(key, 0) / count for key, count in demand.items()]
        )
        enhanced_jain = jains_fairness_index(
            [admitted.get(key, 0) / count for key, count in demand.items()]
        )
        enhanced_admitted = sum(admitted.values())
        store_stats = store.snapshot(now_ns=len(events) * 1_000_000)
        self.storage.save_trace_run(
            run_id=run_id,
            trace_id=trace_id,
            payload=self.tracer.to_otlp_payload(run_id, trace_id, spans),
            created_at_ms=time.time_ns() // 1_000_000,
        )
        return {
            "run_id": run_id,
            "scenario_id": selected_id,
            "total_events": len(events),
            "trace_id": trace_id,
            "baseline": {
                "admitted": baseline.admitted,
                "rejected": baseline.rejected,
                "errors": baseline.errors,
                "upstream_supported": baseline.upstream_supported,
                "upstream_error": baseline.upstream_error,
                "phantom_burn_units": baseline.phantom_burn_units,
                "oversized_waits": None,
                "evicted_active_keys": baseline.evicted_active_keys,
                "wasted_tpm_reserved": None,
                "actual_tpm_settled": None,
                "total_cost_usd": None,
                "jains_fairness": fifo_jain,
                "p99_decision_ms": _percentile_99(baseline.decision_latencies_ms),
            },
            "enhanced": {
                "admitted": enhanced_admitted,
                "rejected": len(events) - enhanced_admitted,
                "errors": 0,
                "upstream_supported": True,
                "upstream_error": None,
                "phantom_burn_units": phantom_units,
                "oversized_waits": 0,
                "evicted_active_keys": store_stats.active_evicted_total,
                "wasted_tpm_reserved": 0,
                "actual_tpm_settled": actual_tpm_settled,
                "total_cost_usd": total_cost_units / USD_SCALE,
                "jains_fairness": enhanced_jain,
                "p99_decision_ms": _percentile_99(latencies_ms),
            },
            "delta": {
                "phantom_burn_prevented": max(0, baseline.phantom_burn_units - phantom_units),
                "tpm_reclaimed": tpm_reclaimed,
                "fairness_gain": round(enhanced_jain - fifo_jain, 6),
                "oversized_spins_avoided": None,
                "evictions_prevented": max(
                    0, baseline.evicted_active_keys - store_stats.active_evicted_total
                ),
            },
            "events_preview": preview,
        }

    def create_reservation(
        self,
        *,
        tenant_id: str,
        model: str,
        estimated_prompt_tokens: int,
        max_completion_tokens: int,
        payload_text: str | None,
        idempotency_key: str | None,
    ) -> StoredReservation:
        """Reserve dual RPM+TPM+USD quota atomically and record OTLP span."""
        trace_id = self.tracer.new_trace_id()
        t0 = time.perf_counter_ns()
        return self.quota.reserve(
            tenant_id=tenant_id,
            model=model,
            estimated_prompt_tokens=estimated_prompt_tokens,
            max_completion_tokens=max_completion_tokens,
            payload_text=payload_text,
            idempotency_key=idempotency_key,
            trace_id=trace_id,
            build_trace=lambda record: self._reservation_trace(record, t0),
        )

    def _reservation_trace(self, record: StoredReservation, start_ns: int) -> dict[str, Any]:
        span = self.tracer.build_span(
            trace_id=record.trace_id,
            name="gateway.quota.reserve",
            duration_ns=max(1_000, time.perf_counter_ns() - start_ns),
            attributes={
                "reservation.id": record.reservation_id,
                "tenant.id": evaluate_redaction(record.tenant_id).redacted_text,
                "llm.model": record.model,
                "quota.reserved_tokens": record.reserved_tokens,
                "quota.remaining_rpm": record.remaining_rpm,
                "quota.remaining_tpm": record.remaining_tpm,
                "redaction.hits": record.redaction_hits,
            },
        )
        return self.tracer.to_otlp_payload(record.reservation_id, record.trace_id, [span])

    def settle_reservation(
        self,
        *,
        reservation_id: str,
        actual_prompt_tokens: int,
        actual_completion_tokens: int,
        provider_status: int,
    ) -> StoredReservation:
        """Settle reservation and refund unused tokens cleanly."""
        return self.quota.settle(
            reservation_id=reservation_id,
            actual_prompt_tokens=actual_prompt_tokens,
            actual_completion_tokens=actual_completion_tokens,
            provider_status=provider_status,
        )

    def simulate_fairness(
        self,
        *,
        tenants: list[TenantDemandSpec],
        total_tpm_capacity: int,
        quantum_tokens: int,
        rounds: int,
        inject_provider_429: bool,
    ) -> dict[str, Any]:
        """Run Weighted DRR vs FIFO + provider failover simulation and record OTLP spans."""
        t0 = time.perf_counter_ns()
        sim = run_fairness_and_failover_simulation(
            tenants=tenants,
            total_tpm_capacity=total_tpm_capacity,
            quantum_tokens=quantum_tokens,
            rounds=rounds,
            inject_provider_429=inject_provider_429,
        )
        elapsed_ns = max(1_000, time.perf_counter_ns() - t0)
        run_id = f"fair_{uuid.uuid4().hex}"
        trace_id = self.tracer.new_trace_id()
        spans = [
            self.tracer.build_span(
                trace_id=trace_id,
                name="gateway.fairness.drr_simulation",
                duration_ns=elapsed_ns,
                attributes={
                    "fairness.jains_fifo": sim.jains_fairness_baseline_fifo,
                    "fairness.jains_drr": sim.jains_fairness_drr,
                    "fairness.starvation_fifo": sim.starvation_events_fifo,
                    "fairness.starvation_drr": sim.starvation_events_drr,
                    "failover.events": len(sim.failover_events),
                },
            )
        ]
        otlp = self.tracer.to_otlp_payload(run_id, trace_id, spans)
        self.storage.save_trace_run(
            run_id=run_id,
            trace_id=trace_id,
            payload=otlp,
            created_at_ms=int(time.time() * 1000),
        )
        return {
            "run_id": run_id,
            "trace_id": trace_id,
            "jains_fairness_baseline_fifo": sim.jains_fairness_baseline_fifo,
            "jains_fairness_drr": sim.jains_fairness_drr,
            "starvation_events_fifo": sim.starvation_events_fifo,
            "starvation_events_drr": sim.starvation_events_drr,
            "tenant_outcomes": [
                {
                    "tenant_id": item.tenant_id,
                    "weight": item.weight,
                    "requested_tokens": item.requested_tokens,
                    "fifo_served_tokens": item.fifo_served_tokens,
                    "drr_served_tokens": item.drr_served_tokens,
                    "fifo_requests_served": item.fifo_requests_served,
                    "drr_requests_served": item.drr_requests_served,
                    "fulfillment_ratio_fifo": item.fulfillment_ratio_fifo,
                    "fulfillment_ratio_drr": item.fulfillment_ratio_drr,
                }
                for item in sim.tenant_outcomes
            ],
            "provider_health": [
                {
                    "provider": p.provider,
                    "role": p.role,
                    "state": p.state,
                    "consecutive_failures": p.consecutive_failures,
                    "cooldown_rounds": p.cooldown_rounds,
                    "total_429_5xx": p.total_429_5xx,
                    "routed_requests": p.routed_requests,
                }
                for p in sim.provider_health
            ],
            "failover_events": [
                {
                    "round_index": fe.round_index,
                    "from_provider": fe.from_provider,
                    "to_provider": fe.to_provider,
                    "reason": fe.reason,
                    "backoff_seconds": fe.backoff_seconds,
                }
                for fe in sim.failover_events
            ],
        }

    def eval_redaction_payload(self, *, text: str, mode: RedactionMode) -> RedactionResult:
        """Evaluate redaction rules with zero-ReDoS engine."""
        return evaluate_redaction(text, mode=mode)

    def read_benchmarks(self) -> dict[str, Any]:
        """Read external benchmark report if generated; never fabricate metrics."""
        path: Path = self.settings.resolved_metrics_path()
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError):
            raw = None
        if isinstance(raw, dict) and raw.get("schema_version") == "2.0.0":
            gates = raw.get("ci_regression_gate")
            status = raw.get("status")
            tier_keys = (
                "tier_1a_azure_traces",
                "tier_1b_burstgpt_50k",
                "tier_2_chaos_attacks",
                "tier_3_redaction_dos",
                "synthetic_fairness",
            )
            if (
                isinstance(gates, dict)
                and gates
                and all(isinstance(value, bool) for value in gates.values())
                and status in ("passed", "failed", "incomplete")
                and raw.get("overall_pass") is (status == "passed")
                and all(gates.values()) == (status == "passed")
                and all(
                    isinstance(raw.get(key), dict)
                    or (status == "incomplete" and key in raw and raw[key] is None)
                    for key in tier_keys
                )
            ):
                return {"available": True, "metrics": raw}
        return {"available": False, "metrics": None}
