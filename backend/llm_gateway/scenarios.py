"""Reproducible LLM gateway stress scenarios for side-by-side baseline comparison."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ScenarioDefinition:
    """Built-in reproducible stress scenario metadata and workload generator."""

    scenario_id: str
    title: str
    description: str
    default_rpm_limit: int
    default_tpm_limit: int
    default_window_seconds: int
    events: list[dict[str, Any]]


def _build_phantom_burn_events() -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    # 18 requests across an immediate burst against rpm_limit=10
    for i in range(18):
        events.append(
            {
                "event_id": f"ev-{i + 1:03d}",
                "tenant_id": "tenant-alpha",
                "model": "gpt-4o-mini",
                "prompt_tokens": 350,
                "max_completion_tokens": 250,
                "actual_completion_tokens": 120,
                "priority": 1,
                "store_key": "tenant:tenant-alpha",
            }
        )
    return events


def _build_streaming_refund_events() -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for i in range(14):
        events.append(
            {
                "event_id": f"ev-{i + 1:03d}",
                "tenant_id": "tenant-stream",
                "model": "gpt-4o",
                "prompt_tokens": 800,
                "max_completion_tokens": 3200,
                "actual_completion_tokens": 240 if i % 2 == 0 else 410,
                "priority": 1,
                "store_key": "tenant:tenant-stream",
            }
        )
    return events


def _build_noisy_neighbor_events() -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for i in range(16):
        is_noisy = i < 12
        events.append(
            {
                "event_id": f"ev-{i + 1:03d}",
                "tenant_id": "tenant-abuser" if is_noisy else "tenant-vip",
                "model": "gpt-4o-mini" if is_noisy else "claude-3-5-sonnet",
                "prompt_tokens": 2200 if is_noisy else 450,
                "max_completion_tokens": 1800 if is_noisy else 350,
                "actual_completion_tokens": 1500 if is_noisy else 180,
                "priority": 3 if is_noisy else 1,
                "store_key": f"tenant:{'abuser' if is_noisy else 'vip'}",
            }
        )
    return events


def _build_failover_events() -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for i in range(12):
        events.append(
            {
                "event_id": f"ev-{i + 1:03d}",
                "tenant_id": f"tenant-svc-{(i % 3) + 1}",
                "model": "gpt-4o-mini",
                "prompt_tokens": 600,
                "max_completion_tokens": 900,
                "actual_completion_tokens": 0 if i in (3, 4, 5) else 280,
                "priority": 1,
                "provider_status": 429 if i in (3, 4, 5) else 200,
                "store_key": f"tenant:svc-{(i % 3) + 1}",
            }
        )
    return events


SCENARIOS: dict[str, ScenarioDefinition] = {
    "phantom-quota-burn": ScenarioDefinition(
        scenario_id="phantom-quota-burn",
        title="Phantom Quota Burn Under 429 Overload",
        description=(
            "Replays an 18-request burst against rpm_limit=10 and measures whether either "
            "counter changes on denial."
        ),
        default_rpm_limit=10,
        default_tpm_limit=24_000,
        default_window_seconds=60,
        events=_build_phantom_burn_events(),
    ),
    "streaming-reserve-refund": ScenarioDefinition(
        scenario_id="streaming-reserve-refund",
        title="Streaming Reserve -> Settle/Refund Reclamation",
        description=(
            "High max_completion_tokens reservations settled cleanly when SSE finishes early, "
            "reclaiming unused TPM capacity back into the bucket."
        ),
        default_rpm_limit=20,
        default_tpm_limit=22_000,
        default_window_seconds=60,
        events=_build_streaming_refund_events(),
    ),
    "noisy-neighbor-drr": ScenarioDefinition(
        scenario_id="noisy-neighbor-drr",
        title="Multi-Tenant Noisy Neighbor vs Weighted DRR",
        description=(
            "Replays heavy batch and interactive quota demand. Use the dedicated fairness "
            "simulator for DRR scheduling."
        ),
        default_rpm_limit=24,
        default_tpm_limit=20_000,
        default_window_seconds=60,
        events=_build_noisy_neighbor_events(),
    ),
    "provider-429-failover": ScenarioDefinition(
        scenario_id="provider-429-failover",
        title="Upstream Provider 429 Circuit Failover",
        description=(
            "Replays upstream HTTP 429 outcomes with zero observed usage. Use the dedicated "
            "fairness simulator to evaluate routing and backoff."
        ),
        default_rpm_limit=20,
        default_tpm_limit=25_000,
        default_window_seconds=60,
        events=_build_failover_events(),
    ),
}
