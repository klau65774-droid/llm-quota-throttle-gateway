"""FastAPI control-plane application for llm-quota-throttle-gateway."""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit

from fastapi import FastAPI, Path, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.datastructures import Headers
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from llm_gateway.api_models import (
    BenchmarksResponse,
    CompareRequest,
    CompareResponse,
    ErrorResponse,
    FairnessSimRequest,
    FairnessSimResponse,
    HealthResponse,
    MetaResponse,
    ModelPricingInfo,
    RedactionEvalRequest,
    RedactionEvalResponse,
    RedactionFinding,
    ReservationRecord,
    ReservationsListResponse,
    ReserveRequest,
    ScenariosListResponse,
    ScenarioSummary,
    SettleRequest,
    TraceResponse,
    ValidationErrorResponse,
)
from llm_gateway.compat import OversizedRequestError
from llm_gateway.config import MODEL_SPECS, GatewaySettings, get_settings
from llm_gateway.engine import GatewayControlEngine, UnknownScenarioError
from llm_gateway.fairness import TenantDemandSpec
from llm_gateway.quota import (
    IdempotencyConflictError,
    QuotaExceededError,
    RedactionBlockedError,
    ReservationConflictError,
    ReservationNotFoundError,
)
from llm_gateway.scenarios import SCENARIOS
from llm_gateway.storage import StoredReservation

_UPSTREAM_VERSION = "3.5.0"
_UPSTREAM_COMMIT_SHA = "d20114067281038e5c5fb8de71d30a609d3cb7f9"
_MAX_REQUEST_BYTES = 8 * 1024 * 1024
_RPM_HEADER = "X-RateLimit-Remaining-RPM"
_TPM_HEADER = "X-RateLimit-Remaining-TPM"
ResourceID = Annotated[str, Path(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")]
_QUOTA_HEADERS = {
    _RPM_HEADER: {"schema": {"type": "integer", "minimum": 0}},
    _TPM_HEADER: {"schema": {"type": "integer", "minimum": 0}},
}


def _error(
    status: int,
    code: str,
    detail: str,
    retry_after: float | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    body = ErrorResponse(detail=detail, error_code=code, retry_after_seconds=retry_after)
    return JSONResponse(body.model_dump(), status_code=status, headers=headers)


def _errors(*codes: int) -> dict[int | str, dict[str, Any]]:
    result: dict[int | str, dict[str, Any]] = {
        code: {"model": ValidationErrorResponse if code == 422 else ErrorResponse} for code in codes
    }
    if 429 in result:
        result[429]["headers"] = {"Retry-After": {"schema": {"type": "integer", "minimum": 1}}}
    return result


def _quota_error(exc: QuotaExceededError) -> JSONResponse:
    return _error(
        429,
        exc.error_code,
        str(exc),
        retry_after=exc.retry_after_seconds,
        headers={"Retry-After": str(max(1, math.ceil(exc.retry_after_seconds)))},
    )


class GatewayRequestLimits:
    """Enforce the origin and byte limits before JSON validation or SQL work."""

    def __init__(self, app: ASGIApp, allowed_origins: list[str], host: str) -> None:
        self.app = app
        self.allowed_origins = frozenset(allowed_origins)
        self.allowed_hosts = {"localhost", "127.0.0.1", "::1", host}

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = Headers(scope=scope)
        hosts = headers.getlist("host")
        host = hosts[0] if len(hosts) == 1 else ""
        try:
            parsed_host = urlsplit(f"//{host}")
            valid_host = (
                parsed_host.hostname in self.allowed_hosts
                and parsed_host.netloc == host
                and parsed_host.username is None
                and parsed_host.port != 0
            )
        except ValueError:
            valid_host = False
        if not valid_host:
            await _error(403, "HOST_NOT_ALLOWED", "Request host is not allowed")(
                scope, receive, send
            )
            return
        if not scope["path"].startswith("/api/"):
            await self.app(scope, receive, send)
            return
        origins = headers.getlist("origin")
        same_origin = f"{scope.get('scheme', 'http')}://{host}"
        if origins and (
            len(origins) != 1 or origins[0] not in self.allowed_origins | {same_origin}
        ):
            await _error(403, "ORIGIN_NOT_ALLOWED", "Request origin is not allowed")(
                scope, receive, send
            )
            return
        if scope["method"] != "POST":
            await self.app(scope, receive, send)
            return

        too_large = _error(413, "REQUEST_TOO_LARGE", "Request body exceeds 8 MiB")
        length = headers.get("content-length")
        if length is not None:
            if not length.isascii() or not length.isdecimal():
                await _error(400, "INVALID_CONTENT_LENGTH", "Invalid Content-Length")(
                    scope, receive, send
                )
                return
            length = length.lstrip("0") or "0"
            if len(length) > len(str(_MAX_REQUEST_BYTES)) or int(length) > _MAX_REQUEST_BYTES:
                await too_large(scope, receive, send)
                return
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            if len(body) + len(chunk) > _MAX_REQUEST_BYTES:
                await too_large(scope, receive, send)
                return
            body.extend(chunk)
            if not message.get("more_body", False):
                break
        payload: bytes | None = bytes(body)
        body.clear()

        async def _replay() -> Message:
            nonlocal payload
            if payload is None:
                return await receive()
            message: Message = {"type": "http.request", "body": payload, "more_body": False}
            payload = None
            return message

        await self.app(scope, _replay, send)


def _to_reservation_schema(row: StoredReservation) -> ReservationRecord:
    return ReservationRecord(
        reservation_id=row.reservation_id,
        trace_id=row.trace_id,
        tenant_id=row.tenant_id,
        model=row.model,
        status=row.status,
        reserved_tokens=row.reserved_tokens,
        actual_tokens=row.actual_tokens,
        refunded_tokens=row.refunded_tokens,
        estimated_cost_usd=row.estimated_cost_usd,
        actual_cost_usd=row.actual_cost_usd,
        remaining_rpm=row.remaining_rpm,
        remaining_tpm=row.remaining_tpm,
        provider_used=row.provider_used,
        redaction_hits=row.redaction_hits,
        created_at_ms=row.created_at_ms,
        settled_at_ms=row.settled_at_ms,
    )


def create_app(settings: GatewaySettings | None = None) -> FastAPI:
    """Construct configured FastAPI application instance."""
    resolved_settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app_inst: FastAPI) -> AsyncIterator[None]:
        engine = GatewayControlEngine(resolved_settings)
        app_inst.state.engine = engine
        try:
            yield
        finally:
            engine.close()

    app = FastAPI(
        title="LLM Quota Throttle Gateway",
        version="0.1.0",
        description=(
            "Deterministic dual RPM+TPM quota control plane, Reserve -> Settle/Refund ledger, "
            "Weighted DRR fairness scheduler, and circuit-breaker failover built on throttled-py."
        ),
        lifespan=lifespan,
        responses=_errors(500),
    )

    origins = resolved_settings.cors_origin_list
    app.add_middleware(GatewayRequestLimits, allowed_origins=origins, host=resolved_settings.host)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=False,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Content-Type"],
        expose_headers=[_RPM_HEADER, _TPM_HEADER, "Retry-After"],
    )

    def _engine(request: Request) -> GatewayControlEngine:
        return request.app.state.engine  # type: ignore[no-any-return]

    @app.exception_handler(Exception)
    async def _unhandled_exception_handler(request: Request, _exc: Exception) -> JSONResponse:
        origin = request.headers.get("origin")
        headers = (
            {"Access-Control-Allow-Origin": origin, "Vary": "Origin"}
            if origin is not None and origin in origins
            else None
        )
        return _error(500, "INTERNAL_ERROR", "Internal gateway error", headers=headers)

    @app.exception_handler(RequestValidationError)
    async def _invalid_request(_request: Request, exc: RequestValidationError) -> JSONResponse:
        issues = [
            {"loc": list(item["loc"]), "msg": item["msg"], "type": item["type"]}
            for item in exc.errors()
        ]
        return JSONResponse({"detail": issues}, status_code=422)

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        if exc.status_code == 404:
            return _error(404, "NOT_FOUND", "Resource not found")
        if exc.status_code == 405:
            return _error(405, "METHOD_NOT_ALLOWED", "Method not allowed", headers=exc.headers)
        return _error(exc.status_code, "HTTP_ERROR", "Request rejected", headers=exc.headers)

    @app.get("/health", response_model=HealthResponse, responses=_errors(403))
    def health() -> HealthResponse:
        return HealthResponse(
            status="ok",
            upstream_baseline_version=_UPSTREAM_VERSION,
            upstream_commit_sha=_UPSTREAM_COMMIT_SHA,
        )

    @app.get("/api/v1/meta", response_model=MetaResponse, responses=_errors(403))
    def meta(request: Request) -> MetaResponse:
        eng = _engine(request)
        return MetaResponse(
            service="llm-quota-throttle-gateway",
            version="0.1.0",
            upstream_repo="ZhuoZhuoCrayon/throttled-py",
            upstream_version=_UPSTREAM_VERSION,
            upstream_commit_sha=_UPSTREAM_COMMIT_SHA,
            storage_backend="sqlite_wal+memory",
            store_max_keys=eng.settings.store_max_keys,
            redaction_mode=eng.settings.redaction_mode,
            models=[
                ModelPricingInfo(
                    model=s.model,
                    prompt_usd_per_1k=s.prompt_usd_per_1k,
                    completion_usd_per_1k=s.completion_usd_per_1k,
                    primary_provider=s.primary_provider,
                    fallback_provider=s.fallback_provider,
                )
                for s in MODEL_SPECS.values()
            ],
        )

    @app.get("/api/v1/scenarios", response_model=ScenariosListResponse, responses=_errors(403))
    def scenarios() -> ScenariosListResponse:
        return ScenariosListResponse(
            scenarios=[
                ScenarioSummary(
                    scenario_id=s.scenario_id,
                    title=s.title,
                    description=s.description,
                    default_rpm_limit=s.default_rpm_limit,
                    default_tpm_limit=s.default_tpm_limit,
                    default_window_seconds=s.default_window_seconds,
                    event_count=len(s.events),
                )
                for s in SCENARIOS.values()
            ]
        )

    @app.post(
        "/api/v1/compare", response_model=CompareResponse, responses=_errors(400, 403, 413, 422)
    )
    def compare_workload(body: CompareRequest, request: Request) -> CompareResponse | JSONResponse:
        eng = _engine(request)
        try:
            raw_reqs = (
                [item.model_dump() for item in body.requests] if body.requests is not None else None
            )
            result = eng.compare(
                scenario_id=body.scenario_id,
                requests=raw_reqs,
                rpm_limit=body.rpm_limit,
                tpm_limit=body.tpm_limit,
                window_seconds=body.window_seconds,
                usd_budget_limit=body.usd_budget_limit,
                fail_fast_oversized=body.fail_fast_oversized,
            )
        except UnknownScenarioError as exc:
            return _error(400, "INVALID_SCENARIO", str(exc))
        return CompareResponse.model_validate(result)

    @app.post(
        "/api/v1/reservations",
        response_model=ReservationRecord,
        status_code=201,
        responses={**_errors(400, 403, 409, 413, 422, 429), 201: {"headers": _QUOTA_HEADERS}},
    )
    def create_reservation(
        body: ReserveRequest, request: Request, response: Response
    ) -> ReservationRecord | JSONResponse:
        eng = _engine(request)
        try:
            rec = eng.create_reservation(
                tenant_id=body.tenant_id,
                model=body.model or eng.settings.default_model,
                estimated_prompt_tokens=body.estimated_prompt_tokens,
                max_completion_tokens=body.max_completion_tokens,
                payload_text=body.payload_text,
                idempotency_key=body.idempotency_key,
            )
        except OversizedRequestError as exc:
            return _error(400, "OVERSIZED_REQUEST", str(exc))
        except RedactionBlockedError as exc:
            return _error(400, "REDACTION_BLOCKED", str(exc))
        except IdempotencyConflictError as exc:
            return _error(409, "IDEMPOTENCY_CONFLICT", str(exc))
        except QuotaExceededError as exc:
            return _quota_error(exc)
        response.headers[_RPM_HEADER] = str(rec.remaining_rpm)
        response.headers[_TPM_HEADER] = str(rec.remaining_tpm)
        return _to_reservation_schema(rec)

    @app.post(
        "/api/v1/reservations/{reservation_id}/settle",
        response_model=ReservationRecord,
        responses=_errors(400, 403, 404, 409, 413, 422, 429),
    )
    def settle_reservation(
        reservation_id: ResourceID, body: SettleRequest, request: Request
    ) -> ReservationRecord | JSONResponse:
        eng = _engine(request)
        try:
            updated = eng.settle_reservation(
                reservation_id=reservation_id,
                actual_prompt_tokens=body.actual_prompt_tokens,
                actual_completion_tokens=body.actual_completion_tokens,
                provider_status=body.provider_status,
            )
        except ReservationNotFoundError as exc:
            return _error(404, "NOT_FOUND", str(exc))
        except ReservationConflictError as exc:
            return _error(409, "ALREADY_SETTLED", str(exc))
        except QuotaExceededError as exc:
            return _quota_error(exc)
        return _to_reservation_schema(updated)

    @app.get(
        "/api/v1/reservations", response_model=ReservationsListResponse, responses=_errors(403, 422)
    )
    def list_reservations(
        request: Request,
        tenant_id: str | None = Query(
            default=None, min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$"
        ),
        status: Literal["reserved", "settled", "refunded"] | None = None,
        limit: int = Query(default=50, ge=1, le=200),
    ) -> ReservationsListResponse:
        eng = _engine(request)
        rows, total = eng.storage.list_reservations(tenant_id=tenant_id, status=status, limit=limit)
        return ReservationsListResponse(
            items=[_to_reservation_schema(r) for r in rows],
            total=total,
        )

    @app.post(
        "/api/v1/fairness/simulate",
        response_model=FairnessSimResponse,
        responses=_errors(400, 403, 413, 422),
    )
    def simulate_fairness(body: FairnessSimRequest, request: Request) -> FairnessSimResponse:
        eng = _engine(request)
        result = eng.simulate_fairness(
            tenants=[
                TenantDemandSpec(
                    tenant_id=t.tenant_id,
                    weight=t.weight,
                    demand_rpm=t.demand_rpm,
                    avg_tokens=t.avg_tokens,
                )
                for t in body.tenants
            ],
            total_tpm_capacity=body.total_tpm_capacity,
            quantum_tokens=body.quantum_tokens,
            rounds=body.rounds,
            inject_provider_429=body.inject_provider_429,
        )
        return FairnessSimResponse.model_validate(result)

    @app.get(
        "/api/v1/traces/{run_id}",
        response_model=TraceResponse,
        responses=_errors(403, 404, 422),
    )
    def get_trace(run_id: ResourceID, request: Request) -> TraceResponse | JSONResponse:
        eng = _engine(request)
        payload = eng.storage.get_trace_run(run_id)
        if payload is None:
            return _error(404, "TRACE_NOT_FOUND", "Trace run not found")
        return TraceResponse.model_validate(payload)

    @app.get(
        "/api/v1/traces/{run_id}/stream",
        response_class=StreamingResponse,
        responses={
            **_errors(403, 404, 422),
            200: {"content": {"text/event-stream": {"schema": {"type": "string"}}}},
        },
    )
    def stream_trace(run_id: ResourceID, request: Request) -> Response:
        eng = _engine(request)
        payload = eng.storage.get_trace_run(run_id)
        if payload is None:
            return _error(404, "TRACE_NOT_FOUND", "Trace run not found")

        spans: list[dict[str, Any]] = []
        resource_spans = payload.get("resourceSpans", [])
        if resource_spans:
            scope_spans = resource_spans[0].get("scopeSpans", [])
            if scope_spans:
                spans = scope_spans[0].get("spans", [])

        async def _event_gen() -> AsyncIterator[str]:
            for span_obj in spans[:50]:
                yield f"event: span\ndata: {json.dumps(span_obj)}\n\n"
                await asyncio.sleep(0)
            yield (
                f"event: done\ndata: {json.dumps({'run_id': run_id, 'count': len(spans[:50])})}\n\n"
            )

        return StreamingResponse(_event_gen(), media_type="text/event-stream")

    @app.post(
        "/api/v1/eval/redaction",
        response_model=RedactionEvalResponse,
        responses=_errors(400, 403, 413, 422),
    )
    def eval_redaction(body: RedactionEvalRequest, request: Request) -> RedactionEvalResponse:
        eng = _engine(request)
        res = eng.eval_redaction_payload(text=body.text, mode=body.mode)
        return RedactionEvalResponse(
            allowed=res.allowed,
            mode=res.mode,
            redacted_text=res.redacted_text,
            findings=[
                RedactionFinding(rule=f.rule, count=f.count, sample_masked=f.sample_masked)
                for f in res.findings
            ],
            latency_ms=res.latency_ms,
        )

    @app.get("/api/v1/benchmarks", response_model=BenchmarksResponse, responses=_errors(403))
    def benchmarks(request: Request) -> BenchmarksResponse:
        eng = _engine(request)
        return BenchmarksResponse.model_validate(eng.read_benchmarks())

    return app


app = create_app()
