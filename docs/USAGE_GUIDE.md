# Usage & Architecture Guide

## 1. Dual-Mode Comparison Architecture
Every call to `POST /api/v1/compare` runs the identical event list through:
1. **Pinned `upstream_baseline`** runs the vendored sliding-window memory action from commit `d20114067281038e5c5fb8de71d30a609d3cb7f9`, whose package declares version `3.5.0`. The response reports `upstream_supported: false` for the combined quota and leaves unavailable measurements as `null`.
2. **Enhanced `GatewayControlEngine`** combines `FixedSlidingWindowLimiter`, `MonotonicTokenBucketLimiter`, per-tenant USD budget tracking, and immediate oversized request rejection.

## 2. Reserve → Settle/Refund Lifecycle
1. Before streaming starts, call `POST /api/v1/reservations` with `estimated_prompt_tokens`, `max_completion_tokens`, optional `payload_text`, and optional `idempotency_key`.
2. `DualQuotaCoordinator` evaluates redaction and acquires hierarchical **Global → Tenant → Model** quotas atomically. Failure restores touched rate-limit state. The SQLite WAL commit includes the reservation and trace.
3. Call `POST /api/v1/reservations/{reservation_id}/settle` with all observed prompt and completion tokens. Unused reservation tokens are recorded and eligible live buckets are credited. Overruns debit the same scopes. Report zero usage if the provider did no work.

Start one local backend process. Use the ledger with synthetic traffic. In-memory limiter state resets on restart, and the persisted USD budget continues to account for the current window.

## 3. Re-Exporting OpenAPI & Frontend Contract Verification
Whenever API schemas change under `backend/llm_gateway/api_models.py` or `api.py`:
```powershell
.\.venv\Scripts\python.exe backend\scripts\export_openapi.py
cd frontend; npm test; npm run build
```
