"""Tests for Weighted DRR fairness scheduling, 429 failover, and redaction engine."""

from __future__ import annotations

import math

import pytest
from llm_gateway.fairness import (
    ProviderFailoverRouter,
    TenantDemandSpec,
    jains_fairness_index,
    run_fairness_and_failover_simulation,
)
from llm_gateway.redaction import evaluate_redaction


def test_weighted_drr_prevents_vip_starvation_and_routes_429_failover() -> None:
    """DRR outperforms FIFO fairness and opens circuit on primary 429 streak."""
    tenants = [
        TenantDemandSpec(tenant_id="noisy-batch", weight=1, demand_rpm=45, avg_tokens=500),
        TenantDemandSpec(tenant_id="vip-chat-1", weight=4, demand_rpm=6, avg_tokens=400),
        TenantDemandSpec(tenant_id="vip-chat-2", weight=4, demand_rpm=6, avg_tokens=400),
    ]
    res = run_fairness_and_failover_simulation(
        tenants=tenants,
        total_tpm_capacity=12_000,
        quantum_tokens=600,
        rounds=12,
        inject_provider_429=True,
    )
    assert res.jains_fairness_drr > res.jains_fairness_baseline_fifo
    assert res.starvation_events_drr <= res.starvation_events_fifo
    assert len(res.failover_events) >= 2
    backup = next(p for p in res.provider_health if p.provider == "azure-backup")
    assert backup.routed_requests >= 2
    assert jains_fairness_index([1.0, 1.0, 1.0]) == 1.0


def test_drr_carries_capacity_and_resumes_weighted_visits() -> None:
    """Request costs can span releases, and equal packet sizes get weighted shares."""
    result = run_fairness_and_failover_simulation(
        tenants=[
            TenantDemandSpec(tenant_id="weighted", weight=3, demand_rpm=10, avg_tokens=30),
            TenantDemandSpec(tenant_id="standard", weight=1, demand_rpm=9, avg_tokens=30),
        ],
        total_tpm_capacity=241,
        quantum_tokens=30,
        rounds=12,
    )
    weighted, standard = result.tenant_outcomes
    assert (weighted.drr_requests_served, standard.drr_requests_served) == (6, 2)
    assert (weighted.fifo_requests_served, standard.fifo_requests_served) == (8, 0)
    assert weighted.drr_served_tokens + standard.drr_served_tokens == 240
    assert result.starvation_events_drr == 16
    assert result.starvation_events_fifo == 16


def test_drr_rejects_duplicate_tenant_queues() -> None:
    tenant = TenantDemandSpec(tenant_id="repeated", weight=1, demand_rpm=2, avg_tokens=1_000)
    with pytest.raises(ValueError, match="unique tenant IDs"):
        run_fairness_and_failover_simulation(tenants=[tenant, tenant])


def test_no_fulfilled_requests_do_not_produce_a_perfect_jain_index() -> None:
    result = run_fairness_and_failover_simulation(
        tenants=[TenantDemandSpec(tenant_id="batch", weight=1, demand_rpm=2, avg_tokens=1_100)],
        total_tpm_capacity=1_000,
        rounds=3,
    )
    assert result.jains_fairness_baseline_fifo == result.jains_fairness_drr == 0.0
    assert result.starvation_events_fifo == result.starvation_events_drr == 3


def test_provider_reopens_after_failed_probe_and_recovers_on_success() -> None:
    router = ProviderFailoverRouter([("primary", "primary"), ("backup", "backup")])
    primary, backup = router.provider_health

    assert router.route(1, {"primary": 429}) == "backup"
    assert primary.state == "degraded"
    assert router.route(2, {"primary": 503}) == "backup"
    assert primary.state == "open"
    assert primary.cooldown_rounds == math.ceil(router.failover_events[-1].backoff_seconds)

    probe_round = 2 + primary.cooldown_rounds
    for round_index in range(3, probe_round):
        assert router.route(round_index, {"primary": 429}) == "backup"
    assert primary.routed_requests == 2
    assert primary.total_429_5xx == 2

    assert router.route(probe_round, {"primary": 429}) == "backup"
    assert primary.state == "open"
    assert primary.total_429_5xx == 3
    assert router.failover_events[-1].reason == "HTTP 429: half-open probe failed"
    assert router.failover_events[-1].backoff_seconds > router.failover_events[-2].backoff_seconds

    assert router.route(probe_round + primary.cooldown_rounds, {}) == "primary"
    assert primary.state == "closed"
    assert primary.consecutive_failures == primary.cooldown_rounds == 0
    assert primary.routed_requests == 4
    assert backup.routed_requests == probe_round


def test_redaction_mask_block_audit_and_linear_latency() -> None:
    """Verify redaction accuracy and strict zero-ReDoS latency on large payload."""
    synthetic_api_key = "sk-" + "AlphaBetaGamma1234567890"
    synthetic_gh = "ghp_" + "9876543210ZyXwVuTsRqPo"
    synthetic_pem = "-----BEGIN " + "RSA PRIVATE KEY-----"
    synthetic_gov_id = f"{0:03d}-{0:02d}-{0:04d}"
    sample = (
        f"Bearer {synthetic_api_key} contact ops-alert@example.org "
        f"token={synthetic_gh} ssn={synthetic_gov_id} header={synthetic_pem}"
    )

    masked = evaluate_redaction(sample, mode="mask")
    assert masked.allowed
    assert synthetic_api_key not in masked.redacted_text
    assert synthetic_gh not in masked.redacted_text
    assert "ops-alert@example.org" not in masked.redacted_text
    assert "[REDACTED:PRIVATE_KEY]" in masked.redacted_text
    assert masked.total_hits >= 4

    blocked = evaluate_redaction(sample, mode="block")
    assert not blocked.allowed
    assert blocked.redacted_text == "[BLOCKED_BY_REDACTION_POLICY]"
    audit = evaluate_redaction(sample, mode="audit")
    assert audit.allowed and audit.redacted_text == sample
    assert audit.findings == masked.findings

    plain_key_label = "PRIVATE KEY"
    start = f"-----BEGIN {plain_key_label}-----"
    end = f"-----END {plain_key_label}-----"
    assert evaluate_redaction(
        f"before {start}\nfixture-only-content\n{end} after"
    ).redacted_text == ("before [REDACTED:PRIVATE_KEY] after")

    # Adversarial 60KB repetitive input must finish far below 50ms (linear scan)
    adversarial = ("Bearer " + "x" * 200 + " ") * 280
    perf = evaluate_redaction(adversarial, mode="mask")
    assert perf.latency_ms < 50.0
