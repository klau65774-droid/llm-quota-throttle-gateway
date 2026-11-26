# API Contract — llm-quota-throttle-gateway

## 1. Operating scope and HTTP conventions

The console and API run on loopback for a trusted local operator using one backend process. The ledger and tenant USD budget are durable in SQLite WAL. RPM and TPM state are process-local and reset on restart. The workbench simulates provider behavior. All endpoints share the local operator's access to the ledger and traces. Use synthetic identifiers and payloads in the console.

- **Backend Base URL**: `http://127.0.0.1:8000`
- **Frontend Dev URL**: `http://127.0.0.1:5173` (Vite proxies `/api` and `/health` to `http://127.0.0.1:8000`)
- **OpenAPI Schema Export**: `backend/docs/openapi.json`
- **Health Check**: `GET /health` -> `200 OK`
- **Errors** return `{ "detail": string, "error_code": string, "retry_after_seconds": number | null }`. Validation errors use HTTP `422` with `{ "detail": [{ "loc": [], "msg": string, "type": string }] }`. Invalid input values and stack traces stay out of these responses.
- **Request checks** enforce a loopback or explicitly configured Host and an exact Origin allowlist for cross-origin API requests. Unapproved Hosts or Origins receive `403`. JSON request bodies over 8 MiB receive `413`. The configured browser origins can read the quota and retry headers.

## 2. Endpoints

### `GET /health`
- **200 OK**:
```json
{
  "status": "ok",
  "upstream_baseline_version": "3.5.0",
  "upstream_commit_sha": "d20114067281038e5c5fb8de71d30a609d3cb7f9"
}
```

### `GET /api/v1/meta`
- **200 OK**: Returns service identity, pinned upstream commit metadata, active store limits (`store_max_keys`), redaction policy, and configured model pricing table.

### `GET /api/v1/scenarios`
- **200 OK**: Returns `{ "scenarios": ScenarioSummary[] }` for 4 reproducible stress scenarios:
  - `phantom-quota-burn` measures counter changes during an over-limit burst, including ordinary denials that leave the counter unchanged.
  - `streaming-reserve-refund`: Streaming completion over-estimation where `Reserve -> Settle/Refund` reclaims unused TPM.
  - `noisy-neighbor-drr` replays different tenant demand sizes. Use the fairness route to compare FIFO and DRR scheduling.
  - `provider-429-failover` replays provider HTTP 429 outcomes and token settlement. Use the fairness route for circuit behavior and backup routing.

### `POST /api/v1/compare`
Replays the same events and per-tenant RPM limits in the pinned upstream sliding-window memory action and the enhanced in-memory quota replay. The replay uses one virtual millisecond per event. The enhanced side also applies per-tenant TPM and USD limits and immediately settles the modeled usage. A provider error with zero observed completion usage models zero total use. `requests: []` evaluates zero events. `requests: null` selects the named built-in scenario. Both enhanced oversized modes deny admission immediately and the flag selects the rejection label.

The baseline reports `upstream_supported: false` and identifies the unsupported combined capability. Unavailable baseline token, cost and wait measurements are `null`. Both Jain scores use admitted/requested events for every tenant with demand. An empty run returns zero. `fairness_gain` is the enhanced score minus the baseline score. The replay leaves `oversized_spins_avoided` null because it measures no waiting. Phantom units count changes in the limiter counter on denial.
- **Request (`CompareRequest`)**:
```json
{
  "scenario_id": "phantom-quota-burn",
  "requests": null,
  "rpm_limit": 20,
  "tpm_limit": 40000,
  "window_seconds": 60,
  "usd_budget_limit": 10.0,
  "fail_fast_oversized": true
}
```
- **200 OK (`CompareResponse`)**:
  - `run_id`, `scenario_id`, `total_events`, `trace_id`
  - `baseline`: `EngineMetrics` (`admitted`, `rejected`, `errors`, `upstream_supported`, `upstream_error`, `phantom_burn_units`, `oversized_waits`, `evicted_active_keys`, `wasted_tpm_reserved`, `actual_tpm_settled`, `total_cost_usd`, `jains_fairness`, `p99_decision_ms`)
  - `enhanced`: `EngineMetrics`
  - `delta`: `{ "phantom_burn_prevented": int, "tpm_reclaimed": int, "fairness_gain": float, "oversized_spins_avoided": int, "evictions_prevented": int }`
  - `events_preview`: up to 50 per-event side-by-side decision records.
- **400 Bad Request** for an unknown built-in scenario, with `INVALID_SCENARIO`.

### `POST /api/v1/reservations`
Checks RPM and TPM at global, tenant and tenant/model scopes and a per-tenant USD budget in fixed 60-second UTC windows. Persists the reservation and its trace in one WAL transaction. The prompt is evaluated using the configured redaction policy and is discarded after evaluation.
- **Request (`ReserveRequest`)**:
  - `tenant_id` is an ASCII identifier. `model` is one of `gpt-4o-mini`, `gpt-4o`, `claude-3-5-sonnet`, or `llama-3-70b` and defaults to the configured model. The estimated prompt and completion limits are nonnegative. The optional `payload_text` is bounded at 65,536 characters.
  - The optional `idempotency_key` has 1–128 characters and is scoped to the tenant. Replaying the same key, model and estimates returns the original reservation and trace. The API accepts `priority` in `1..5` for workload compatibility.
- **201 Created (`ReservationRecord`)** (plus `X-RateLimit-Remaining-RPM`, `X-RateLimit-Remaining-TPM` headers)
- **400 Bad Request**: Oversized single request exceeding bucket capacity (`cost > limit`) when fail-fast is active (`error_code: "OVERSIZED_REQUEST"`), or redaction `block` policy hit (`error_code: "REDACTION_BLOCKED"`).
- **409 Conflict** for a key already used by the same tenant with a different model or token estimate, with `IDEMPOTENCY_CONFLICT`.
- **429 Too Many Requests**: Dual quota or USD budget exceeded (`error_code: "QUOTA_EXCEEDED"`), includes `Retry-After` header.

### `POST /api/v1/reservations/{reservation_id}/settle`
Finalizes the reservation exactly once using actual prompt plus completion tokens. The ledger records the unused amount. Live bucket generations receive eligible refunds up to capacity, and already replenished generations receive no extra credit. Overruns are charged at all three TPM scopes and can create debt that refills over time. The USD settlement updates the original reservation window.
- **Request (`SettleRequest`)**: `{ "actual_prompt_tokens": int, "actual_completion_tokens": int, "provider_status": int = 200 }`
- **200 OK**: Updated `ReservationRecord`
- **404 Not Found**: Unknown `reservation_id`
- **409 Conflict**: Reservation already settled/closed (`error_code: "ALREADY_SETTLED"`)
- **429 Too Many Requests** if an overrun needs new bucket state while the store is full of live entries. The reservation remains open and `Retry-After` is provided.

### `GET /api/v1/reservations`
- **Query params**: `tenant_id` (optional), `status` (optional, `reserved`, `settled` or `refunded`), `limit` (`1..200`, default `50`)
- **200 OK**: `{ "items": ReservationRecord[], "total": number }`

### `POST /api/v1/fairness/simulate`
Simulates weighted Deficit Round Robin scheduling and FIFO queuing over the same released token capacity. Each tenant ID is unique. Starvation counts backlogged tenant-rounds without service. The optional injection attempts HTTP 429 on primary rounds 3–5, skipping requests during cooldown. Provider routing uses a separate simulated request each round. `routed_requests` counts attempts, including backup retries.
- **Request (`FairnessSimRequest`)**:
  `{ "tenants": [{ "tenant_id": str, "weight": int, "demand_rpm": int, "avg_tokens": int }], "total_tpm_capacity": int, "quantum_tokens": int, "rounds": int, "inject_provider_429": bool }`
- **200 OK (`FairnessSimResponse`)**: `run_id`, `trace_id`, `jains_fairness_baseline_fifo`, `jains_fairness_drr`, `starvation_events_fifo`, `starvation_events_drr`, `tenant_outcomes`, `provider_health`, `failover_events`.

### `GET /api/v1/traces/{run_id}`
- **200 OK**: OTLP-compatible JSON (`{ "run_id": str, "trace_id": str, "resourceSpans": [...] }`)
- **404 Not Found**: Unknown `run_id`

### `GET /api/v1/traces/{run_id}/stream`
- **200 OK (`text/event-stream`)**: Server-Sent Events stream emitting `event: span` JSON chunks and terminating with `event: done`.
- **404 Not Found** for an unknown run ID, with `TRACE_NOT_FOUND`.

### `POST /api/v1/eval/redaction`
Evaluates bounded-input secret and PII rules. `mask` substitutes detected values, `block` rejects matching payloads, and `audit` returns the caller's submitted text with fixed masked finding summaries.
- **200 OK (`RedactionEvalResponse`)**: `{ "allowed": bool, "mode": str, "redacted_text": str, "findings": [{ "rule": str, "count": int, "sample_masked": str }], "latency_ms": float }`

### `GET /api/v1/benchmarks`
- **200 OK** returns `{ "available": bool, "metrics": object | null }` from a recognized `docs/test_metrics.json` report. A valid failed or incomplete report remains available with its status. Missing, malformed and unsupported reports return `available: false` and `metrics: null`.

## 3. Environment Variables (`.env.example`)
- `GATEWAY_HOST` (default `127.0.0.1`)
- `GATEWAY_PORT` (default `8000`)
- `GATEWAY_DB_PATH` (default `data/gateway_state.sqlite3`)
- `GATEWAY_CORS_ORIGINS` (default `http://127.0.0.1:5173,http://localhost:5173`)
- `GATEWAY_SEALED_BENCHMARK_DIR` (default `../_sealed_benchmarks/llm-quota-throttle-gateway`)
- `GATEWAY_STORE_MAX_KEYS` (default `2048`)
- `GATEWAY_DEFAULT_MODEL` (default `gpt-4o-mini`)
- `GATEWAY_REDACTION_MODE` (default `mask`)
- `GATEWAY_OTEL_SERVICE_NAME` (default `llm-quota-throttle-gateway`)
