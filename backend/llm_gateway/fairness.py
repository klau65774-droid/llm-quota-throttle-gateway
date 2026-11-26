"""Weighted Deficit Round Robin (DRR) fair scheduling & circuit-breaker failover."""

from __future__ import annotations

import hashlib
import math
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field


def jains_fairness_index(values: list[float]) -> float:
    """Compute Jain's Fairness Index in [0, 1] over per-tenant fulfillment ratios."""
    if not values:
        return 0.0
    total = sum(values)
    sq_total = sum(v * v for v in values)
    if sq_total <= 0.0:
        return 0.0
    return round((total * total) / (len(values) * sq_total), 6)


@dataclass(frozen=True, slots=True)
class TenantDemandSpec:
    """Tenant scheduling configuration for fairness simulations."""

    tenant_id: str
    weight: int
    demand_rpm: int
    avg_tokens: int


@dataclass(frozen=True, slots=True)
class TenantScheduleOutcome:
    """Per-tenant served vs starved summary across FIFO and DRR."""

    tenant_id: str
    weight: int
    requested_tokens: int
    fifo_served_tokens: int
    drr_served_tokens: int
    fifo_requests_served: int
    drr_requests_served: int
    fulfillment_ratio_fifo: float
    fulfillment_ratio_drr: float


@dataclass(slots=True)
class ProviderCircuitState:
    """Tracks circuit-breaker state and exponential backoff for a provider."""

    provider: str
    role: str
    state: str = "closed"
    consecutive_failures: int = 0
    cooldown_rounds: int = 0
    total_429_5xx: int = 0
    routed_requests: int = 0


@dataclass(frozen=True, slots=True)
class FailoverEvent:
    """Recorded provider failover transition."""

    round_index: int
    from_provider: str
    to_provider: str
    reason: str
    backoff_seconds: float


@dataclass(frozen=True, slots=True)
class FairnessSimulationResult:
    """Complete FIFO vs Weighted DRR + failover simulation outcome."""

    jains_fairness_baseline_fifo: float
    jains_fairness_drr: float
    starvation_events_fifo: int
    starvation_events_drr: int
    tenant_outcomes: list[TenantScheduleOutcome]
    provider_health: list[ProviderCircuitState]
    failover_events: list[FailoverEvent]


def _deterministic_jitter(provider: str, round_idx: int) -> float:
    digest = hashlib.sha256(f"{provider}:{round_idx}".encode()).digest()
    return round((digest[0] / 255.0) * 0.45, 3)


@dataclass(slots=True)
class _TenantQueue:
    spec: TenantDemandSpec
    pending: int = field(init=False)
    deficit: int = 0

    def __post_init__(self) -> None:
        self.pending = self.spec.demand_rpm

    @property
    def served(self) -> int:
        return self.spec.demand_rpm - self.pending


class WeightedFairScheduler:
    """Keep the DRR cursor and any unspent deficit across capacity releases."""

    def __init__(self, tenants: Sequence[TenantDemandSpec], quantum_tokens: int) -> None:
        if not tenants or len({spec.tenant_id for spec in tenants}) != len(tenants):
            raise ValueError("tenants must be non-empty with unique tenant IDs")
        if quantum_tokens < 1 or any(
            min(spec.weight, spec.demand_rpm, spec.avg_tokens) < 1 for spec in tenants
        ):
            raise ValueError("quantum, weights, demand, and token costs must be positive")
        self._quantum = quantum_tokens
        self._queues = {spec.tenant_id: _TenantQueue(spec) for spec in tenants}
        self._active = deque(self._queues.values())
        self._visit_open = False

    def served_requests(self, tenant_id: str) -> int:
        return self._queues[tenant_id].served

    def starvation_events(self, served_this_round: set[str]) -> int:
        return sum(
            queue.pending > 0 and tenant_id not in served_this_round
            for tenant_id, queue in self._queues.items()
        )

    def schedule(self, available: int, capacity_left: int) -> tuple[int, set[str]]:
        """Schedule integral requests using current and remaining simulation capacity."""
        tokens_used = 0
        served_tenants: set[str] = set()
        empty_visits = 0
        while available > 0 and self._active:
            queue = self._active[0]
            spec = queue.spec
            if spec.avg_tokens > capacity_left - tokens_used:
                # The request cannot fit in any remaining round. Keep its backlog
                # for outcome reporting and let smaller requests use the capacity.
                self._active.popleft()
                self._visit_open = False
                empty_visits = 0
                continue
            if not self._visit_open:
                queue.deficit += self._quantum * spec.weight
                self._visit_open = True
            count = min(
                queue.pending, queue.deficit // spec.avg_tokens, available // spec.avg_tokens
            )
            if count:
                cost = count * spec.avg_tokens
                queue.pending -= count
                queue.deficit -= cost
                available -= cost
                tokens_used += cost
                served_tenants.add(spec.tenant_id)
                empty_visits = 0

            if not queue.pending:
                self._active.popleft()
                self._visit_open = False
            elif queue.deficit >= spec.avg_tokens:
                # Resume this visit when its next packet fits the released capacity.
                break
            else:
                self._active.rotate(-1)
                self._visit_open = False
                if count == 0:
                    empty_visits += 1
                    if empty_visits == len(self._active):
                        self._advance_idle_cycles()
                        empty_visits = 0
        return tokens_used, served_tenants

    def _advance_idle_cycles(self) -> None:
        """Skip complete DRR cycles in which no queue can afford its head packet."""
        cycles = min(
            (queue.spec.avg_tokens - queue.deficit - 1) // (self._quantum * queue.spec.weight)
            for queue in self._active
        )
        for queue in self._active:
            queue.deficit += cycles * self._quantum * queue.spec.weight


def _schedule_fifo(queues: deque[_TenantQueue], available: int) -> tuple[int, set[str]]:
    tokens_used = 0
    served_tenants: set[str] = set()
    while queues and available >= queues[0].spec.avg_tokens:
        queue = queues[0]
        count = min(queue.pending, available // queue.spec.avg_tokens)
        cost = count * queue.spec.avg_tokens
        queue.pending -= count
        available -= cost
        tokens_used += cost
        served_tenants.add(queue.spec.tenant_id)
        if not queue.pending:
            queues.popleft()
    return tokens_used, served_tenants


class ProviderFailoverRouter:
    """Simulate ordered provider attempts, retries, and circuit-breaker probes."""

    def __init__(self, providers: Sequence[tuple[str, str]]) -> None:
        if not providers or len({name for name, _ in providers}) != len(providers):
            raise ValueError("providers must be non-empty with unique names")
        self.provider_health = [
            ProviderCircuitState(provider=name, role=role) for name, role in providers
        ]
        self.failover_events: list[FailoverEvent] = []
        self._next_probe_round: dict[str, int] = {}

    def route(self, round_index: int, statuses: Mapping[str, int]) -> str | None:
        """Route one simulated request. Count each provider actually attempted."""
        for provider in self.provider_health:
            if provider.state == "open":
                provider.cooldown_rounds = max(
                    0, self._next_probe_round[provider.provider] - round_index
                )
                if provider.cooldown_rounds == 0:
                    provider.state = "half_open"

        previous_failure: tuple[str, str, float] | None = None
        for provider in self.provider_health:
            if provider.state == "open":
                continue
            if previous_failure is not None:
                source, reason, backoff = previous_failure
                self.failover_events.append(
                    FailoverEvent(round_index, source, provider.provider, reason, backoff)
                )
            provider.routed_requests += 1
            status = statuses.get(provider.provider, 200)
            if status != 429 and not 500 <= status <= 599:
                provider.state = "closed"
                provider.consecutive_failures = 0
                self._next_probe_round.pop(provider.provider, None)
                return provider.provider

            provider.consecutive_failures += 1
            provider.total_429_5xx += 1
            backoff = round(
                (2 ** min(4, provider.consecutive_failures - 1)) * 0.5
                + _deterministic_jitter(provider.provider, round_index),
                3,
            )
            if provider.consecutive_failures >= 2:
                probe = provider.state == "half_open"
                provider.state = "open"
                provider.cooldown_rounds = math.ceil(backoff)
                self._next_probe_round[provider.provider] = round_index + provider.cooldown_rounds
                reason = f"HTTP {status}: {'half-open probe failed' if probe else 'circuit opened'}"
            else:
                provider.state = "degraded"
                reason = f"Transient HTTP {status}"
            previous_failure = (provider.provider, reason, backoff)
        return None


def run_fairness_and_failover_simulation(
    *,
    tenants: list[TenantDemandSpec],
    total_tpm_capacity: int = 20_000,
    quantum_tokens: int = 1_000,
    rounds: int = 12,
    inject_provider_429: bool = False,
) -> FairnessSimulationResult:
    """Compare naive FIFO burst admission against Weighted DRR + provider failover."""
    if total_tpm_capacity < 1 or rounds < 1:
        raise ValueError("total capacity and rounds must be positive")

    # Build round-by-round demand queues (noisy neighbor listed first in arrival order)
    ordered = sorted(tenants, key=lambda t: (-t.demand_rpm, t.tenant_id))
    drr = WeightedFairScheduler(ordered, quantum_tokens)
    fifo = {spec.tenant_id: _TenantQueue(spec) for spec in ordered}
    fifo_active = deque(fifo.values())
    fifo_tokens_used = 0
    drr_tokens_used = 0

    starvation_fifo = 0
    starvation_drr = 0
    router = ProviderFailoverRouter(
        [
            ("openai-primary", "primary"),
            ("azure-backup", "backup"),
            ("anthropic-primary", "secondary-primary"),
            ("bedrock-backup", "secondary-backup"),
        ]
    )

    for r_idx in range(1, rounds + 1):
        statuses = {"openai-primary": 429} if inject_provider_429 and r_idx in (3, 4, 5) else {}
        router.route(r_idx, statuses)

        # Each algorithm receives the same cumulative capacity. Unused tokens and
        # the remainder of integer division become available in later rounds.
        released = total_tpm_capacity * r_idx // rounds
        fifo_used, fifo_served = _schedule_fifo(fifo_active, released - fifo_tokens_used)
        fifo_tokens_used += fifo_used
        starvation_fifo += sum(
            queue.pending > 0 and tenant_id not in fifo_served for tenant_id, queue in fifo.items()
        )
        drr_used, drr_served = drr.schedule(
            released - drr_tokens_used, total_tpm_capacity - drr_tokens_used
        )
        drr_tokens_used += drr_used
        starvation_drr += drr.starvation_events(drr_served)

    outcomes: list[TenantScheduleOutcome] = []
    fifo_ratios: list[float] = []
    drr_ratios: list[float] = []
    for spec in tenants:
        req_tokens = spec.demand_rpm * spec.avg_tokens
        fifo_reqs = fifo[spec.tenant_id].served
        drr_reqs = drr.served_requests(spec.tenant_id)
        f_ratio = fifo_reqs / spec.demand_rpm
        d_ratio = drr_reqs / spec.demand_rpm
        fifo_ratios.append(f_ratio)
        drr_ratios.append(d_ratio)
        outcomes.append(
            TenantScheduleOutcome(
                tenant_id=spec.tenant_id,
                weight=spec.weight,
                requested_tokens=req_tokens,
                fifo_served_tokens=fifo_reqs * spec.avg_tokens,
                drr_served_tokens=drr_reqs * spec.avg_tokens,
                fifo_requests_served=fifo_reqs,
                drr_requests_served=drr_reqs,
                fulfillment_ratio_fifo=round(f_ratio, 4),
                fulfillment_ratio_drr=round(d_ratio, 4),
            )
        )

    return FairnessSimulationResult(
        jains_fairness_baseline_fifo=jains_fairness_index(fifo_ratios),
        jains_fairness_drr=jains_fairness_index(drr_ratios),
        starvation_events_fifo=starvation_fifo,
        starvation_events_drr=starvation_drr,
        tenant_outcomes=outcomes,
        provider_health=router.provider_health,
        failover_events=router.failover_events,
    )
