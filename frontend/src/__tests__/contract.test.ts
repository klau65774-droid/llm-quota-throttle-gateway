import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiClientError, gatewayApi, traceStreamUrl } from "../api";

function mockGateway() {
  const fetchMock = vi.fn<typeof fetch>();
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function jsonResponse(body: unknown, init?: ResponseInit): Response {
  return new Response(JSON.stringify(body), init);
}

afterEach(() => vi.unstubAllGlobals());

describe("frontend API contract", () => {
  it("documents the request methods and success statuses the UI uses", () => {
    const openapiPath = resolve(process.cwd(), "../backend/docs/openapi.json");
    const spec = JSON.parse(readFileSync(openapiPath, "utf-8")) as {
      paths: Record<string, Record<string, { responses: Record<string, unknown> }>>;
    };
    const operations = [
      ["get", "/health", "200"],
      ["get", "/api/v1/meta", "200"],
      ["get", "/api/v1/scenarios", "200"],
      ["post", "/api/v1/compare", "200"],
      ["get", "/api/v1/reservations", "200"],
      ["post", "/api/v1/reservations", "201"],
      ["post", "/api/v1/reservations/{reservation_id}/settle", "200"],
      ["post", "/api/v1/fairness/simulate", "200"],
      ["get", "/api/v1/traces/{run_id}", "200"],
      ["get", "/api/v1/traces/{run_id}/stream", "200"],
      ["post", "/api/v1/eval/redaction", "200"],
      ["get", "/api/v1/benchmarks", "200"],
    ];
    for (const [method, path, status] of operations) {
      expect(spec.paths[path]?.[method]?.responses[status], `${method} ${path} returns ${status}`)
        .toBeDefined();
    }
  });

  it("issues a cancelable GET without a request content type", async () => {
    const fetchMock = mockGateway().mockResolvedValueOnce(jsonResponse({
      status: "ok",
      upstream_baseline_version: "3.5.0",
      upstream_commit_sha: "d20114067281038e5c5fb8de71d30a609d3cb7f9",
    }));
    const { signal } = new AbortController();
    await expect(gatewayApi.getHealth(signal)).resolves.toMatchObject({ status: "ok" });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith("/health", {
      signal,
      headers: expect.any(Headers),
    });
    const headers = new Headers(fetchMock.mock.calls[0][1]?.headers);
    expect(headers.get("Accept")).toBe("application/json");
    expect(headers.has("Content-Type")).toBe(false);
  });

  it("uses the selected reservation ID and preserves explicitly entered zero usage", async () => {
    const fetchMock = mockGateway().mockResolvedValueOnce(jsonResponse({
      reservation_id: "row /A",
      status: "refunded",
      actual_tokens: 0,
    }));
    const enteredUsage = {
      actual_prompt_tokens: 0,
      actual_completion_tokens: 0,
      provider_status: 200,
    };
    await expect(gatewayApi.settleReservation("row /A", enteredUsage))
      .resolves.toMatchObject({ reservation_id: "row /A", actual_tokens: 0 });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith("/api/v1/reservations/row%20%2FA/settle", {
      method: "POST",
      body: JSON.stringify(enteredUsage),
      headers: expect.any(Headers),
    });
    expect(new Headers(fetchMock.mock.calls[0][1]?.headers).get("Content-Type"))
      .toBe("application/json");
  });

  it("preserves unavailable baseline measurements and the selected replay inputs", async () => {
    const baseline = {
      oversized_waits: null,
      wasted_tpm_reserved: null,
      actual_tpm_settled: null,
      total_cost_usd: null,
    };
    const fetchMock = mockGateway().mockResolvedValueOnce(jsonResponse({
      run_id: "cmp_sample",
      baseline,
      delta: { oversized_spins_avoided: null },
    }));
    const configuration = {
      scenario_id: "phantom-quota-burn",
      rpm_limit: 1,
      tpm_limit: 1,
      window_seconds: 60,
      usd_budget_limit: 0.05,
      fail_fast_oversized: false,
    };
    await expect(gatewayApi.runCompare(configuration)).resolves.toMatchObject({
      baseline,
      delta: { oversized_spins_avoided: null },
    });
    expect(fetchMock).toHaveBeenCalledWith("/api/v1/compare", expect.objectContaining({
      method: "POST",
      body: JSON.stringify(configuration),
    }));
  });

  it("uses the same encoded trace ID in both transport URLs", async () => {
    const fetchMock = mockGateway().mockResolvedValueOnce(jsonResponse({ resourceSpans: [] }));
    const runId = "run / preview";
    await gatewayApi.getTrace(runId);
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/traces/run%20%2F%20preview",
      expect.objectContaining({ headers: expect.any(Headers) }),
    );
    expect(traceStreamUrl(runId)).toBe("/api/v1/traces/run%20%2F%20preview/stream");
  });

  it("preserves known structured errors and retry metadata", async () => {
    mockGateway().mockResolvedValueOnce(jsonResponse({
      detail: "TPM exhausted",
      error_code: "QUOTA_EXCEEDED",
      retry_after_seconds: 1.25,
    }, { status: 429, headers: { "Retry-After": "2" } }));

    await expect(gatewayApi.getHealth()).rejects.toMatchObject({
      status: 429,
      errorCode: "QUOTA_EXCEEDED",
      retryAfterSeconds: 1.25,
      message: "TPM exhausted",
    });
  });

  it("uses a safe HTTP fallback when a proxy supplies a different body format", async () => {
    mockGateway()
      .mockResolvedValueOnce(new Response("<html>Proxy error</html>", {
        status: 502,
        headers: { "Retry-After": "5" },
      }))
      .mockResolvedValueOnce(jsonResponse(null, { status: 503 }));

    const error = gatewayApi.getHealth();
    await expect(error).rejects.toBeInstanceOf(ApiClientError);
    await expect(error).rejects.toMatchObject({
      status: 502,
      retryAfterSeconds: 5,
      message: "Gateway request failed (HTTP 502).",
    });
    await expect(gatewayApi.getHealth()).rejects.toMatchObject({
      status: 503,
      errorCode: "HTTP_503",
      retryAfterSeconds: null,
    });
  });

  it("recognizes validation errors and rejects malformed success responses", async () => {
    mockGateway()
      .mockResolvedValueOnce(jsonResponse({
        detail: [{ loc: ["body", "text"], type: "missing", msg: "Field required" }],
      }, { status: 422 }))
      .mockResolvedValueOnce(new Response("invalid JSON"));

    await expect(gatewayApi.evalRedaction({ text: "", mode: "mask" })).rejects.toMatchObject({
      status: 422,
      errorCode: "HTTP_422",
      message: "The gateway rejected one or more request fields.",
    });
    await expect(gatewayApi.getMeta()).rejects.toMatchObject({
      status: 200,
      errorCode: "INVALID_RESPONSE",
    });
  });
});
