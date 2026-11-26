"""Pydantic v2 API schemas aligned verbatim with docs/API_CONTRACT.md."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, field_validator

Identifier = Annotated[str, Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")]
SupportedModel = Literal["gpt-4o-mini", "gpt-4o", "claude-3-5-sonnet", "llama-3-70b"]


class ErrorResponse(BaseModel):
    """Standardized API error payload."""

    detail: str
    error_code: str
    retry_after_seconds: float | None = None


class ValidationIssue(BaseModel):
    loc: list[str | int]
    msg: str
    type: str


class ValidationErrorResponse(BaseModel):
    detail: list[ValidationIssue]


class HealthResponse(BaseModel):
    """Health check response."""

    status: Literal["ok"]
    upstream_baseline_version: str
    upstream_commit_sha: str


class ModelPricingInfo(BaseModel):
    """Supported LLM model pricing and routing entry."""

    model: str
    prompt_usd_per_1k: float
    completion_usd_per_1k: float
    primary_provider: str
    fallback_provider: str


class MetaResponse(BaseModel):
    """Gateway metadata and active configuration response."""

    service: str
    version: str
    upstream_repo: str
    upstream_version: str
    upstream_commit_sha: str
    storage_backend: str
    store_max_keys: int
    redaction_mode: str
    models: list[ModelPricingInfo]


class ScenarioSummary(BaseModel):
    """Summary metadata for a built-in reproducible stress scenario."""

    scenario_id: str
    title: str
    description: str
    default_rpm_limit: int
    default_tpm_limit: int
    default_window_seconds: int
    event_count: int


class ScenariosListResponse(BaseModel):
    """Scenario list wrapper."""

    scenarios: list[ScenarioSummary]


class WorkloadEventInput(BaseModel):
    """Single workload event for ad-hoc comparison."""

    event_id: Identifier | None = None
    tenant_id: Identifier
    model: SupportedModel | None = None
    prompt_tokens: int = Field(ge=0, le=2_000_000)
    max_completion_tokens: int = Field(ge=0, le=2_000_000)
    actual_completion_tokens: int = Field(ge=0, le=2_000_000)
    priority: int = Field(default=1, ge=1, le=5)
    provider_status: int = Field(default=200, ge=100, le=599)


class CompareRequest(BaseModel):
    """Dual baseline-vs-enhanced workload comparison request."""

    scenario_id: Identifier | None = "phantom-quota-burn"
    requests: list[WorkloadEventInput] | None = Field(default=None, max_length=2000)
    rpm_limit: int = Field(default=10, ge=1, le=100_000)
    tpm_limit: int = Field(default=24_000, ge=1, le=100_000_000)
    window_seconds: int = Field(default=60, ge=1, le=3600)
    usd_budget_limit: float = Field(default=25.0, gt=0.0, le=100_000.0)
    fail_fast_oversized: bool = True


class EngineMetrics(BaseModel):
    """Per-engine evaluation metrics (No-False-Greens compatible)."""

    admitted: int
    rejected: int
    errors: int
    upstream_supported: bool
    upstream_error: str | None
    phantom_burn_units: int
    oversized_waits: int | None
    evicted_active_keys: int
    wasted_tpm_reserved: int | None
    actual_tpm_settled: int | None
    total_cost_usd: float | None
    jains_fairness: float
    p99_decision_ms: float


class ComparisonDelta(BaseModel):
    """Quantified improvements of enhanced engine over upstream baseline."""

    phantom_burn_prevented: int
    tpm_reclaimed: int
    fairness_gain: float
    oversized_spins_avoided: int | None
    evictions_prevented: int


class DecisionEventRecord(BaseModel):
    """Per-event comparison decision preview row."""

    event_id: str
    tenant_id: str
    model: str
    reserved_tokens: int
    actual_tokens: int
    refunded_tokens: int
    enhanced_allowed: bool
    enhanced_reason: str


class CompareResponse(BaseModel):
    """Dual comparison output."""

    run_id: str
    scenario_id: str
    total_events: int
    trace_id: str
    baseline: EngineMetrics
    enhanced: EngineMetrics
    delta: ComparisonDelta
    events_preview: list[DecisionEventRecord]


class ReserveRequest(BaseModel):
    """Create a quota reservation prior to upstream LLM streaming/completion."""

    tenant_id: Identifier
    model: SupportedModel | None = None
    estimated_prompt_tokens: int = Field(ge=0, le=1_000_000)
    max_completion_tokens: int = Field(ge=0, le=1_000_000)
    priority: int = Field(default=1, ge=1, le=5)
    payload_text: str | None = Field(default=None, max_length=65_536)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=128)


class SettleRequest(BaseModel):
    """Settle an active reservation with observed usage tokens."""

    actual_prompt_tokens: int = Field(ge=0, le=1_000_000)
    actual_completion_tokens: int = Field(ge=0, le=1_000_000)
    provider_status: int = Field(default=200, ge=100, le=599)


class ReservationRecord(BaseModel):
    """Persisted reservation ledger item."""

    reservation_id: str
    trace_id: str
    tenant_id: str
    model: str
    status: str
    reserved_tokens: int
    actual_tokens: int | None
    refunded_tokens: int
    estimated_cost_usd: float
    actual_cost_usd: float | None
    remaining_rpm: int
    remaining_tpm: int
    provider_used: str
    redaction_hits: int
    created_at_ms: int
    settled_at_ms: int | None


class ReservationsListResponse(BaseModel):
    """Paginated reservation list wrapper."""

    items: list[ReservationRecord]
    total: int


class TenantWeightSpec(BaseModel):
    """Tenant scheduling specification for DRR vs FIFO simulator."""

    tenant_id: Identifier
    weight: int = Field(ge=1, le=20)
    demand_rpm: int = Field(ge=1, le=500)
    avg_tokens: int = Field(ge=10, le=100_000)


class FairnessSimRequest(BaseModel):
    """Weighted DRR & failover simulation payload."""

    tenants: list[TenantWeightSpec] = Field(min_length=1, max_length=32)
    total_tpm_capacity: int = Field(default=20_000, ge=1_000, le=10_000_000)
    quantum_tokens: int = Field(default=1_000, ge=100, le=100_000)
    rounds: int = Field(default=12, ge=1, le=100)
    inject_provider_429: bool = False

    @field_validator("tenants")
    @classmethod
    def _unique_tenants(cls, value: list[TenantWeightSpec]) -> list[TenantWeightSpec]:
        if len({tenant.tenant_id for tenant in value}) != len(value):
            raise ValueError("Tenant IDs must be unique")
        return value


class TenantFairnessOutcome(BaseModel):
    """Per-tenant FIFO vs Weighted DRR outcome."""

    tenant_id: str
    weight: int
    requested_tokens: int
    fifo_served_tokens: int
    drr_served_tokens: int
    fifo_requests_served: int
    drr_requests_served: int
    fulfillment_ratio_fifo: float
    fulfillment_ratio_drr: float


class ProviderHealthSnapshot(BaseModel):
    """Provider circuit-breaker health snapshot."""

    provider: str
    role: str
    state: str
    consecutive_failures: int
    cooldown_rounds: int
    total_429_5xx: int
    routed_requests: int


class FailoverEventRecord(BaseModel):
    """Failover transition record."""

    round_index: int
    from_provider: str
    to_provider: str
    reason: str
    backoff_seconds: float


class FairnessSimResponse(BaseModel):
    """Weighted DRR fairness & failover simulation output."""

    run_id: str
    trace_id: str
    jains_fairness_baseline_fifo: float
    jains_fairness_drr: float
    starvation_events_fifo: int
    starvation_events_drr: int
    tenant_outcomes: list[TenantFairnessOutcome]
    provider_health: list[ProviderHealthSnapshot]
    failover_events: list[FailoverEventRecord]


class RedactionEvalRequest(BaseModel):
    """Prompt redaction evaluation request."""

    text: str = Field(min_length=1, max_length=65_536)
    mode: Literal["mask", "block", "audit"] = "mask"


class RedactionFinding(BaseModel):
    """Aggregated match count per rule."""

    rule: str
    count: int
    sample_masked: str


class RedactionEvalResponse(BaseModel):
    """Prompt redaction evaluation output."""

    allowed: bool
    mode: str
    redacted_text: str
    findings: list[RedactionFinding]
    latency_ms: float


class BenchmarksResponse(BaseModel):
    """Sealed benchmark results payload."""

    available: bool
    metrics: dict[str, Any] | None = None


class TraceResponse(BaseModel):
    run_id: str
    trace_id: str
    resourceSpans: list[dict[str, Any]]
