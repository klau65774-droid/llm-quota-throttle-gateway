export interface HealthPayload {
  status: "ok";
  upstream_baseline_version: string;
  upstream_commit_sha: string;
}

export interface ModelPricingInfo {
  model: string;
  prompt_usd_per_1k: number;
  completion_usd_per_1k: number;
  primary_provider: string;
  fallback_provider: string;
}

export interface MetaPayload {
  service: string;
  version: string;
  upstream_repo: string;
  upstream_version: string;
  upstream_commit_sha: string;
  storage_backend: string;
  store_max_keys: number;
  redaction_mode: string;
  models: ModelPricingInfo[];
}

export interface ScenarioSummary {
  scenario_id: string;
  title: string;
  description: string;
  default_rpm_limit: number;
  default_tpm_limit: number;
  default_window_seconds: number;
  event_count: number;
}

export interface EngineMetrics {
  admitted: number;
  rejected: number;
  errors: number;
  upstream_supported: boolean;
  upstream_error: string | null;
  phantom_burn_units: number;
  oversized_waits: number | null;
  evicted_active_keys: number;
  wasted_tpm_reserved: number | null;
  actual_tpm_settled: number | null;
  total_cost_usd: number | null;
  jains_fairness: number;
  p99_decision_ms: number;
}

export interface ComparisonDelta {
  phantom_burn_prevented: number;
  tpm_reclaimed: number;
  fairness_gain: number;
  oversized_spins_avoided: number | null;
  evictions_prevented: number;
}

export interface DecisionEventRecord {
  event_id: string;
  tenant_id: string;
  model: string;
  reserved_tokens: number;
  actual_tokens: number;
  refunded_tokens: number;
  enhanced_allowed: boolean;
  enhanced_reason: string;
}

export interface CompareResponse {
  run_id: string;
  scenario_id: string;
  total_events: number;
  trace_id: string;
  baseline: EngineMetrics;
  enhanced: EngineMetrics;
  delta: ComparisonDelta;
  events_preview: DecisionEventRecord[];
}

export interface ReservationRecord {
  reservation_id: string;
  trace_id: string;
  tenant_id: string;
  model: string;
  status: string;
  reserved_tokens: number;
  actual_tokens: number | null;
  refunded_tokens: number;
  estimated_cost_usd: number;
  actual_cost_usd: number | null;
  remaining_rpm: number;
  remaining_tpm: number;
  provider_used: string;
  redaction_hits: number;
  created_at_ms: number;
  settled_at_ms: number | null;
}

export interface TenantFairnessOutcome {
  tenant_id: string;
  weight: number;
  requested_tokens: number;
  fifo_served_tokens: number;
  drr_served_tokens: number;
  fifo_requests_served: number;
  drr_requests_served: number;
  fulfillment_ratio_fifo: number;
  fulfillment_ratio_drr: number;
}

export interface ProviderHealthSnapshot {
  provider: string;
  role: string;
  state: string;
  consecutive_failures: number;
  cooldown_rounds: number;
  total_429_5xx: number;
  routed_requests: number;
}

export interface FailoverEventRecord {
  round_index: number;
  from_provider: string;
  to_provider: string;
  reason: string;
  backoff_seconds: number;
}

export interface FairnessSimResponse {
  run_id: string;
  trace_id: string;
  jains_fairness_baseline_fifo: number;
  jains_fairness_drr: number;
  starvation_events_fifo: number;
  starvation_events_drr: number;
  tenant_outcomes: TenantFairnessOutcome[];
  provider_health: ProviderHealthSnapshot[];
  failover_events: FailoverEventRecord[];
}

export interface RedactionFinding {
  rule: string;
  count: number;
  sample_masked: string;
}

export interface RedactionEvalResponse {
  allowed: boolean;
  mode: string;
  redacted_text: string;
  findings: RedactionFinding[];
  latency_ms: number;
}

export interface BenchmarksResponse {
  available: boolean;
  metrics: Record<string, unknown> | null;
}

export class ApiClientError extends Error {
  status: number;
  errorCode: string;
  retryAfterSeconds: number | null;

  constructor(
    message: string,
    status: number,
    errorCode = "HTTP_ERROR",
    retryAfterSeconds: number | null = null
  ) {
    super(message);
    this.name = "ApiClientError";
    this.status = status;
    this.errorCode = errorCode;
    this.retryAfterSeconds = retryAfterSeconds;
  }
}

async function requestJson<T>(url: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  headers.set("Accept", "application/json");
  if (init?.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  const res = await fetch(url, { ...init, headers });
  const body = await res.text();
  let parsed: Record<string, unknown> | null = null;
  try {
    const value: unknown = JSON.parse(body);
    if (value && typeof value === "object" && !Array.isArray(value)) {
      parsed = value as Record<string, unknown>;
    }
  } catch {
    // Proxies can return non-JSON responses. Preserve their HTTP status below.
  }
  if (!res.ok) {
    const fallback =
      res.status === 422
        ? "The gateway rejected one or more request fields."
        : `Gateway request failed (HTTP ${res.status}).`;
    const detail =
      typeof parsed?.detail === "string" ? parsed.detail : fallback;
    const code =
      typeof parsed?.error_code === "string" ? parsed.error_code : `HTTP_${res.status}`;
    const retryHeader = res.headers.get("Retry-After") ?? "";
    const headerSeconds = /^\d+$/.test(retryHeader) ? Number(retryHeader) : NaN;
    const bodySeconds = parsed?.retry_after_seconds;
    const retry = typeof bodySeconds === "number" && Number.isFinite(bodySeconds) && bodySeconds >= 0
      ? bodySeconds
      : Number.isSafeInteger(headerSeconds) ? headerSeconds : null;
    throw new ApiClientError(detail, res.status, code, retry);
  }
  if (parsed === null) {
    throw new ApiClientError(
      "The gateway returned an invalid JSON response.",
      res.status,
      "INVALID_RESPONSE",
    );
  }
  return parsed as T;
}

export function traceStreamUrl(runId: string): string {
  return `/api/v1/traces/${encodeURIComponent(runId)}/stream`;
}

export const gatewayApi = {
  getHealth: (signal?: AbortSignal): Promise<HealthPayload> =>
    requestJson<HealthPayload>("/health", { signal }),
  getMeta: (signal?: AbortSignal): Promise<MetaPayload> =>
    requestJson<MetaPayload>("/api/v1/meta", { signal }),
  getScenarios: (signal?: AbortSignal): Promise<{ scenarios: ScenarioSummary[] }> =>
    requestJson<{ scenarios: ScenarioSummary[] }>("/api/v1/scenarios", { signal }),
  runCompare: (body: {
    scenario_id: string;
    rpm_limit: number;
    tpm_limit: number;
    window_seconds: number;
    usd_budget_limit: number;
    fail_fast_oversized: boolean;
  }): Promise<CompareResponse> =>
    requestJson<CompareResponse>("/api/v1/compare", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  createReservation: (body: {
    tenant_id: string;
    model: string;
    estimated_prompt_tokens: number;
    max_completion_tokens: number;
    priority: number;
    payload_text: string | null;
    idempotency_key: string | null;
  }): Promise<ReservationRecord> =>
    requestJson<ReservationRecord>("/api/v1/reservations", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  settleReservation: (
    reservationId: string,
    body: {
      actual_prompt_tokens: number;
      actual_completion_tokens: number;
      provider_status: number;
    }
  ): Promise<ReservationRecord> =>
    requestJson<ReservationRecord>(`/api/v1/reservations/${encodeURIComponent(reservationId)}/settle`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  listReservations: (limit = 50, signal?: AbortSignal): Promise<{ items: ReservationRecord[]; total: number }> =>
    requestJson<{ items: ReservationRecord[]; total: number }>(
      `/api/v1/reservations?limit=${limit}`,
      { signal }
    ),
  simulateFairness: (body: {
    tenants: Array<{ tenant_id: string; weight: number; demand_rpm: number; avg_tokens: number }>;
    total_tpm_capacity: number;
    quantum_tokens: number;
    rounds: number;
    inject_provider_429: boolean;
  }): Promise<FairnessSimResponse> =>
    requestJson<FairnessSimResponse>("/api/v1/fairness/simulate", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  getTrace: (runId: string, signal?: AbortSignal): Promise<Record<string, unknown>> =>
    requestJson<Record<string, unknown>>(`/api/v1/traces/${encodeURIComponent(runId)}`, { signal }),
  evalRedaction: (body: {
    text: string;
    mode: "mask" | "block" | "audit";
  }, signal?: AbortSignal): Promise<RedactionEvalResponse> =>
    requestJson<RedactionEvalResponse>("/api/v1/eval/redaction", {
      method: "POST",
      body: JSON.stringify(body),
      signal,
    }),
  getBenchmarks: (signal?: AbortSignal): Promise<BenchmarksResponse> =>
    requestJson<BenchmarksResponse>("/api/v1/benchmarks", { signal }),
};
