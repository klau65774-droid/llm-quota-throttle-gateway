"""End-to-end HTTP API & OpenAPI schema synchronization tests."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from llm_gateway import api
from llm_gateway.api import create_app
from llm_gateway.config import GatewaySettings
from llm_gateway.storage import GatewayStorage
from pydantic import ValidationError

_BACKEND_URL = "http://127.0.0.1:8000"


def test_full_api_contract_and_openapi_sync(tmp_path: Path) -> None:
    """Exercise all 12 contract routes, status codes, SSE stream, and OpenAPI sync."""
    db_path = tmp_path / "api_test.sqlite3"
    app = create_app(
        GatewaySettings(
            db_path=str(db_path),
            store_max_keys=512,
            redaction_mode="mask",
        )
    )

    with TestClient(app, base_url=_BACKEND_URL) as client:
        # 1. Health & Meta & Scenarios & Benchmarks
        h = client.get("/health")
        assert h.status_code == 200
        assert h.json()["status"] == "ok"

        m = client.get("/api/v1/meta")
        assert m.status_code == 200
        assert m.json()["upstream_commit_sha"] == "d20114067281038e5c5fb8de71d30a609d3cb7f9"

        sc = client.get("/api/v1/scenarios")
        assert sc.status_code == 200
        scenario_ids = [s["scenario_id"] for s in sc.json()["scenarios"]]
        assert "phantom-quota-burn" in scenario_ids

        bm = client.get("/api/v1/benchmarks")
        assert bm.status_code == 200
        assert "available" in bm.json()

        # 2. Dual Compare + OTLP Trace + SSE stream
        cmp_res = client.post(
            "/api/v1/compare",
            json={
                "scenario_id": "streaming-reserve-refund",
                "rpm_limit": 20,
                "tpm_limit": 22000,
                "window_seconds": 60,
                "usd_budget_limit": 20.0,
                "fail_fast_oversized": True,
            },
        )
        assert cmp_res.status_code == 200
        cmp_json = cmp_res.json()
        assert cmp_json["baseline"]["upstream_supported"] is False
        assert cmp_json["baseline"]["actual_tpm_settled"] is None
        assert cmp_json["baseline"]["oversized_waits"] is None
        assert cmp_json["delta"]["oversized_spins_avoided"] is None
        assert cmp_json["delta"]["tpm_reclaimed"] > 0
        run_id = cmp_json["run_id"]

        tr = client.get(f"/api/v1/traces/{run_id}")
        assert tr.status_code == 200
        assert len(tr.json()["resourceSpans"]) == 1

        sse = client.get(f"/api/v1/traces/{run_id}/stream")
        assert sse.status_code == 200
        assert sse.headers["content-type"].startswith("text/event-stream")
        assert "event: span" in sse.text
        assert "event: done" in sse.text

        # 3. Reservations + Settle + Conflict + Oversized + 429
        synthetic_tok = "sk-" + "TestSecret987654321"
        r_ok = client.post(
            "/api/v1/reservations",
            json={
                "tenant_id": "tenant-api",
                "model": "gpt-4o-mini",
                "estimated_prompt_tokens": 300,
                "max_completion_tokens": 1200,
                "payload_text": f"hello {synthetic_tok}",
                "idempotency_key": "api-idem-1",
            },
        )
        assert r_ok.status_code == 201
        assert int(r_ok.headers["X-RateLimit-Remaining-RPM"]) == r_ok.json()["remaining_rpm"]
        assert int(r_ok.headers["X-RateLimit-Remaining-TPM"]) == r_ok.json()["remaining_tpm"]
        resv_id = r_ok.json()["reservation_id"]
        assert r_ok.json()["redaction_hits"] == 1

        s_ok = client.post(
            f"/api/v1/reservations/{resv_id}/settle",
            json={
                "actual_prompt_tokens": 300,
                "actual_completion_tokens": 200,
                "provider_status": 200,
            },
        )
        assert s_ok.status_code == 200
        assert s_ok.json()["refunded_tokens"] == 1000

        s_dup = client.post(
            f"/api/v1/reservations/{resv_id}/settle",
            json={"actual_prompt_tokens": 300, "actual_completion_tokens": 200},
        )
        assert s_dup.status_code == 409

        r_oversized = client.post(
            "/api/v1/reservations",
            json={
                "tenant_id": "tenant-api",
                "model": "gpt-4o-mini",
                "estimated_prompt_tokens": 200_000,
                "max_completion_tokens": 100_000,
            },
        )
        assert r_oversized.status_code == 400
        assert r_oversized.json()["error_code"] == "OVERSIZED_REQUEST"

        lst = client.get("/api/v1/reservations?tenant_id=tenant-api")
        assert lst.status_code == 200
        assert lst.json()["total"] == 1

        # 4. Fairness simulation & Redaction evaluator
        fair = client.post(
            "/api/v1/fairness/simulate",
            json={
                "tenants": [
                    {"tenant_id": "batch", "weight": 1, "demand_rpm": 30, "avg_tokens": 500},
                    {"tenant_id": "vip", "weight": 4, "demand_rpm": 6, "avg_tokens": 400},
                ],
                "total_tpm_capacity": 10000,
                "quantum_tokens": 500,
                "rounds": 10,
                "inject_provider_429": True,
            },
        )
        assert fair.status_code == 200
        assert fair.json()["jains_fairness_drr"] >= fair.json()["jains_fairness_baseline_fifo"]

        red = client.post(
            "/api/v1/eval/redaction",
            json={"text": f"leak {synthetic_tok}", "mode": "block"},
        )
        assert red.status_code == 200
        assert red.json()["allowed"] is False

        r_quota = client.post(
            "/api/v1/reservations",
            json={
                "tenant_id": "tenant-quota",
                "estimated_prompt_tokens": 120_000,
                "max_completion_tokens": 0,
            },
        )
        assert r_quota.status_code == 201
        denied = client.post(
            "/api/v1/reservations",
            json={
                "tenant_id": "tenant-quota",
                "estimated_prompt_tokens": 120_000,
                "max_completion_tokens": 0,
            },
        )
        assert denied.status_code == 429
        assert denied.json()["error_code"] == "QUOTA_EXCEEDED"
        assert int(denied.headers["Retry-After"]) >= denied.json()["retry_after_seconds"]

    # Match the whole specification, including status, response headers, and SSE media type.
    openapi_file = Path(__file__).resolve().parents[2] / "backend" / "docs" / "openapi.json"
    committed = json.loads(openapi_file.read_text(encoding="utf-8"))
    live = app.openapi()
    assert committed == live
    assert set(live["paths"]["/api/v1/reservations"]["post"]["responses"]["429"]["headers"]) == {
        "Retry-After",
    }
    stream = live["paths"]["/api/v1/traces/{run_id}/stream"]["get"]["responses"]["200"]
    assert set(stream["content"]) == {"text/event-stream"}


def test_compare_replay_counts_only_observed_outcomes(tmp_path: Path) -> None:
    app = create_app(GatewaySettings(db_path=str(tmp_path / "compare.sqlite3")))
    event = {
        "tenant_id": "tenant-demo",
        "prompt_tokens": 1,
        "max_completion_tokens": 1,
        "actual_completion_tokens": 1,
    }
    with TestClient(app, base_url=_BACKEND_URL) as client:
        normal = client.post("/api/v1/compare", json={"rpm_limit": 1, "requests": [event, event]})
        assert normal.status_code == 200
        baseline = normal.json()["baseline"]
        assert (baseline["admitted"], baseline["rejected"], baseline["phantom_burn_units"]) == (
            1,
            1,
            0,
        )
        overrun = client.post(
            "/api/v1/compare",
            json={
                "rpm_limit": 1,
                "tpm_limit": 10,
                "fail_fast_oversized": False,
                "requests": [
                    {**event, "max_completion_tokens": 20},
                    {**event, "actual_completion_tokens": 4},
                ],
            },
        ).json()
        assert overrun["enhanced"]["admitted"] == 1
        assert overrun["enhanced"]["actual_tpm_settled"] == 5
        assert overrun["events_preview"][1]["actual_tokens"] == 5
        empty = client.post("/api/v1/compare", json={"requests": []})
        assert empty.status_code == 200 and empty.json()["total_events"] == 0
        assert empty.json()["enhanced"]["jains_fairness"] == 0
        missing = client.post("/api/v1/compare", json={"scenario_id": "example-scenario"})
        assert missing.status_code == 400
        assert missing.json() == {
            "detail": "Unknown scenario ID",
            "error_code": "INVALID_SCENARIO",
            "retry_after_seconds": None,
        }


def test_http_boundaries_and_safe_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    app = create_app(GatewaySettings(db_path=str(tmp_path / "boundaries.sqlite3")))
    allowed = "http://localhost:5173"
    with TestClient(app, base_url=_BACKEND_URL, raise_server_exceptions=False) as client:
        route = "/api/v1/eval/redaction"
        denied = client.post(
            route, json={"text": "example"}, headers={"Origin": "https://example.org"}
        )
        assert denied.status_code == 403
        assert "access-control-allow-origin" not in denied.headers
        assert (
            client.post(
                route, json={"text": "example"}, headers={"Origin": _BACKEND_URL}
            ).status_code
            == 200
        )
        ok = client.post(route, json={"text": "example"}, headers={"Origin": allowed})
        assert ok.status_code == 200 and ok.headers["access-control-allow-origin"] == allowed
        assert client.get("/health", headers={"Host": "example.org"}).status_code == 403

        invalid = client.post(route, json={"text": {"example": "private fixture"}})
        assert invalid.status_code == 422
        assert "private fixture" not in invalid.text
        assert set(invalid.json()["detail"][0]) == {"loc", "msg", "type"}

        monkeypatch.setattr(api, "_MAX_REQUEST_BYTES", 64)
        too_long = client.post(route, json={"text": "x" * 64}, headers={"Origin": allowed})
        assert too_long.status_code == 413
        assert too_long.json()["error_code"] == "REQUEST_TOO_LARGE"

        def chunks() -> Iterator[bytes]:
            yield b"x" * 32
            yield b"x" * 33

        assert client.post(route, content=chunks()).status_code == 413

        def broken_read(_storage: GatewayStorage, _run_id: str) -> None:
            raise RuntimeError("fixture-only internal detail")

        monkeypatch.setattr(GatewayStorage, "get_trace_run", broken_read)
        failed = client.get("/api/v1/traces/missing", headers={"Origin": allowed})
        assert failed.status_code == 500
        assert failed.headers["access-control-allow-origin"] == allowed
        assert failed.json() == {
            "detail": "Internal gateway error",
            "error_code": "INTERNAL_ERROR",
            "retry_after_seconds": None,
        }


def test_gateway_paths_and_origins_are_validated() -> None:
    with pytest.raises(ValidationError):
        GatewaySettings(cors_origins="*")
    with pytest.raises(ValueError, match="stay inside"):
        GatewaySettings(db_path="../outside.sqlite3").resolved_db_path()


def test_benchmark_endpoint_reports_exact_data_and_rejects_unreadable_reports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    metrics_path = tmp_path / "metrics.json"
    expected = json.loads(
        (Path(__file__).resolve().parents[2] / "docs" / "test_metrics.json").read_text("utf-8")
    )
    monkeypatch.setattr(GatewaySettings, "resolved_metrics_path", lambda self: metrics_path)
    app = create_app(GatewaySettings(db_path=str(tmp_path / "benchmarks.sqlite3")))
    with TestClient(app, base_url=_BACKEND_URL) as client:
        route = "/api/v1/benchmarks"
        unavailable = {"available": False, "metrics": None}
        assert client.get(route).json() == unavailable
        metrics_path.write_text("{", encoding="utf-8")
        assert client.get(route).json() == unavailable
        metrics_path.write_text(json.dumps(expected), encoding="utf-8")
        assert client.get(route).json() == {"available": True, "metrics": expected}
