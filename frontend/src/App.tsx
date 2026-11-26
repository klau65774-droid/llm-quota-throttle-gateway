import { type FormEvent, useEffect, useRef, useState } from "react";
import {
  Activity,
  AlertTriangle,
  BarChart3,
  Cpu,
  GitCompare,
  Layers,
  ShieldCheck,
} from "lucide-react";
import {
  ApiClientError,
  BenchmarksResponse,
  CompareResponse,
  FairnessSimResponse,
  gatewayApi,
  MetaPayload,
  RedactionEvalResponse,
  ReservationRecord,
  ScenarioSummary,
  traceStreamUrl,
} from "./api";

type TabId = "compare" | "ledger" | "fairness" | "traces" | "benchmarks";
type ErrorScope = TabId | "health" | "meta" | "redaction";

type SettlementInput = {
  reservationId: string;
  promptTokens: string;
  completionTokens: string;
};

const HEALTH_DISPLAY = {
  connecting: { label: "API CONNECTING", style: "" },
  healthy: { label: "API HEALTHY", style: "badge-ok" },
  unavailable: { label: "API UNAVAILABLE", style: "badge-err" },
};

const LEDGER_LIMIT = 30;
const MAX_TOKEN_COUNT = 1_000_000;
const MAX_PROMPT_LENGTH = 65_536;

function errorMessage(error: unknown, fallback: string): string {
  if (error instanceof ApiClientError) {
    const retry = error.retryAfterSeconds === null
      ? ""
      : ` Retry after ${error.retryAfterSeconds}s.`;
    return `[${error.errorCode}] ${error.message}${retry}`;
  }
  return fallback;
}

const TABS: ReadonlyArray<{ id: TabId; label: string }> = [
  { id: "compare", label: "Baseline vs Enhanced" },
  { id: "ledger", label: "Quota Ledger" },
  { id: "fairness", label: "Fairness & Failover" },
  { id: "traces", label: "Traces & Redaction" },
  { id: "benchmarks", label: "Sealed Benchmarks" },
];

export default function App() {
  const [activeTab, setActiveTab] = useState<TabId>("compare");
  const [health, setHealth] = useState<keyof typeof HEALTH_DISPLAY>("connecting");
  const [meta, setMeta] = useState<MetaPayload | null>(null);
  const [scenarios, setScenarios] = useState<ScenarioSummary[]>([]);
  const [errors, setErrors] = useState<Partial<Record<ErrorScope, string | null>>>({});

  // Tab 1: Compare
  const [selectedScenario, setSelectedScenario] = useState("");
  const [rpmLimit, setRpmLimit] = useState("");
  const [tpmLimit, setTpmLimit] = useState("");
  const [windowSeconds, setWindowSeconds] = useState("");
  const [usdBudget, setUsdBudget] = useState("25");
  const [failFastOversized, setFailFastOversized] = useState(true);
  const [compareLoading, setCompareLoading] = useState(false);
  const [compareResult, setCompareResult] = useState<CompareResponse | null>(null);

  // Tab 2: Quota Ledger
  const [tenantId, setTenantId] = useState("tenant-alpha");
  const [modelName, setModelName] = useState("gpt-4o-mini");
  const [promptTokens, setPromptTokens] = useState("400");
  const [maxCompletionTokens, setMaxCompletionTokens] = useState("1600");
  const [payloadText, setPayloadText] = useState("Summarize gateway telemetry for tenant-alpha");
  const [reservations, setReservations] = useState<ReservationRecord[] | null>(null);
  const ledgerRevision = useRef(0);
  const reserveAttempt = useRef<string | null>(null);
  const [ledgerLoading, setLedgerLoading] = useState(false);
  const [settlement, setSettlement] = useState<SettlementInput | null>(null);

  // Tab 3: Fairness & Failover
  const [totalTpmCap, setTotalTpmCap] = useState("12000");
  const [quantumTokens, setQuantumTokens] = useState("600");
  const [simRounds, setSimRounds] = useState("12");
  const [inject429, setInject429] = useState(true);
  const [fairnessResult, setFairnessResult] = useState<FairnessSimResponse | null>(null);
  const [fairnessLoading, setFairnessLoading] = useState(false);

  // Tab 4: Traces & Redaction
  const [traceRunId, setTraceRunId] = useState("");
  const traceRevision = useRef(0);
  const traceRequest = useRef<AbortController | null>(null);
  const traceStream = useRef<EventSource | null>(null);
  const [traceLoading, setTraceLoading] = useState(false);
  const [traceJson, setTraceJson] = useState<Record<string, unknown> | null>(null);
  const [sseEvents, setSseEvents] = useState<string[]>([]);
  const [redactionInput, setRedactionInput] = useState("");
  const [redactionMode, setRedactionMode] = useState<"mask" | "block" | "audit">("mask");
  const [redactionResult, setRedactionResult] = useState<RedactionEvalResponse | null>(null);
  const redactionRequest = useRef<AbortController | null>(null);
  const [redactionLoading, setRedactionLoading] = useState(false);

  // Tab 5: Sealed Benchmarks
  const [benchmarks, setBenchmarks] = useState<BenchmarksResponse | null>(null);

  function showError(scope: ErrorScope, error: string | null) {
    setErrors((current) => ({ ...current, [scope]: error }));
  }

  useEffect(() => {
    const startup = new AbortController();
    const { signal } = startup;
    const initialLedgerRevision = ledgerRevision.current;

    function load<T>(request: Promise<T>, onSuccess: (value: T) => void, onError: () => void) {
      void request.then((value) => {
        if (!signal.aborted) onSuccess(value);
      }).catch(() => {
        if (!signal.aborted) onError();
      });
    }

    load(
      gatewayApi.getHealth(signal),
      (value) => {
        if (value.status !== "ok") throw new Error("Invalid health response");
        setHealth("healthy");
      },
      () => {
        setHealth("unavailable");
        showError("health", "The gateway health check failed.");
      },
    );
    load(gatewayApi.getMeta(signal), (value) => {
      if (typeof value.upstream_version !== "string" ||
          typeof value.upstream_commit_sha !== "string" ||
          !Number.isInteger(value.store_max_keys)) {
        throw new Error("Invalid metadata response");
      }
      setMeta(value);
    }, () => {
      showError("meta", "Gateway metadata could not be loaded.");
    });
    load(gatewayApi.getScenarios(signal), ({ scenarios: available }) => {
      if (!Array.isArray(available) || available.length === 0) throw new Error("No scenarios");
      const initial = available[0];
      setSelectedScenario(initial.scenario_id);
      setRpmLimit(String(initial.default_rpm_limit));
      setTpmLimit(String(initial.default_tpm_limit));
      setWindowSeconds(String(initial.default_window_seconds));
      setScenarios(available);
    }, () => {
      showError("compare", "Replay scenarios could not be loaded.");
    });
    load(gatewayApi.listReservations(LEDGER_LIMIT, signal), ({ items }) => {
      if (!Array.isArray(items)) throw new Error("Invalid ledger response");
      if (ledgerRevision.current === initialLedgerRevision) setReservations(items);
    }, () => {
      if (ledgerRevision.current === initialLedgerRevision) {
        showError("ledger", "The reservation ledger could not be loaded.");
      }
    });
    load(gatewayApi.getBenchmarks(signal), (report) => {
      if (typeof report.available !== "boolean" ||
          (report.available && (!report.metrics || typeof report.metrics !== "object"))) {
        throw new Error("Invalid benchmark response");
      }
      setBenchmarks(report);
    }, () => {
      showError("benchmarks", "The benchmark results could not be loaded.");
    });
    return () => {
      startup.abort();
      traceRequest.current?.abort();
      traceRequest.current = null;
      traceStream.current?.close();
      traceStream.current = null;
      redactionRequest.current?.abort();
      redactionRequest.current = null;
    };
  }, []);

  function handleScenarioChange(nextId: string) {
    setSelectedScenario(nextId);
    const match = scenarios.find((s) => s.scenario_id === nextId);
    if (match) {
      setRpmLimit(String(match.default_rpm_limit));
      setTpmLimit(String(match.default_tpm_limit));
      setWindowSeconds(String(match.default_window_seconds));
    }
  }

  async function handleRunCompare(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (compareLoading) return;
    showError("compare", null);
    setCompareResult(null);
    if (Number(usdBudget) <= 0) {
      showError("compare", "Enter a USD budget greater than zero.");
      return;
    }
    const revision = ++traceRevision.current;
    setCompareLoading(true);
    try {
      const res = await gatewayApi.runCompare({
        scenario_id: selectedScenario,
        rpm_limit: Number(rpmLimit),
        tpm_limit: Number(tpmLimit),
        window_seconds: Number(windowSeconds),
        usd_budget_limit: Number(usdBudget),
        fail_fast_oversized: failFastOversized,
      });
      setCompareResult(res);
      if (traceRevision.current === revision) selectTrace(res.run_id);
    } catch (err) {
      showError("compare", errorMessage(err, "The comparison could not be run."));
    } finally {
      setCompareLoading(false);
    }
  }

  function rememberReservation(record: ReservationRecord) {
    ledgerRevision.current += 1;
    setReservations((current) => {
      const rows = current ?? [];
      return rows.some((row) => row.reservation_id === record.reservation_id)
        ? rows.map((row) => row.reservation_id === record.reservation_id ? record : row)
        : [record, ...rows].slice(0, LEDGER_LIMIT);
    });
  }

  async function refreshLedgerAfter(action: string) {
    try {
      const { items } = await gatewayApi.listReservations(LEDGER_LIMIT);
      if (!Array.isArray(items)) throw new Error("Invalid ledger response");
      setReservations(items);
      showError("ledger", null);
    } catch {
      showError("ledger", `${action} The remaining ledger entries could not be refreshed.`);
    }
  }

  async function handleCreateReservation(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (ledgerLoading) return;
    showError("ledger", null);
    const revision = ++traceRevision.current;
    setLedgerLoading(true);
    try {
      const idempotencyKey = reserveAttempt.current ?? crypto.randomUUID();
      reserveAttempt.current = idempotencyKey;
      const created = await gatewayApi.createReservation({
        tenant_id: tenantId.trim(),
        model: modelName,
        estimated_prompt_tokens: Number(promptTokens),
        max_completion_tokens: Number(maxCompletionTokens),
        priority: 1,
        payload_text: payloadText || null,
        idempotency_key: idempotencyKey,
      });
      reserveAttempt.current = null;
      rememberReservation(created);
      if (traceRevision.current === revision) selectTrace(created.reservation_id);
      await refreshLedgerAfter("The reservation was created.");
    } catch (err) {
      showError("ledger", errorMessage(err, "The reservation could not be created."));
    } finally {
      setLedgerLoading(false);
    }
  }

  async function handleSettle(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (settlement === null || ledgerLoading) return;
    showError("ledger", null);
    setLedgerLoading(true);
    try {
      const updated = await gatewayApi.settleReservation(settlement.reservationId, {
        actual_prompt_tokens: Number(settlement.promptTokens),
        actual_completion_tokens: Number(settlement.completionTokens),
        provider_status: 200,
      });
      rememberReservation(updated);
      setSettlement(null);
      await refreshLedgerAfter("The reservation was settled.");
    } catch (err) {
      showError("ledger", errorMessage(err, "The reservation could not be settled."));
    } finally {
      setLedgerLoading(false);
    }
  }

  async function handleSimulateFairness(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (fairnessLoading) return;
    showError("fairness", null);
    setFairnessResult(null);
    const revision = ++traceRevision.current;
    setFairnessLoading(true);
    try {
      const res = await gatewayApi.simulateFairness({
        tenants: [
          { tenant_id: "noisy-batch", weight: 1, demand_rpm: 42, avg_tokens: 500 },
          { tenant_id: "vip-chat-1", weight: 4, demand_rpm: 6, avg_tokens: 400 },
          { tenant_id: "vip-chat-2", weight: 4, demand_rpm: 6, avg_tokens: 400 },
        ],
        total_tpm_capacity: Number(totalTpmCap),
        quantum_tokens: Number(quantumTokens),
        rounds: Number(simRounds),
        inject_provider_429: inject429,
      });
      setFairnessResult(res);
      if (traceRevision.current === revision) selectTrace(res.run_id);
    } catch (err) {
      showError("fairness", errorMessage(err, "The fairness simulation could not be run."));
    } finally {
      setFairnessLoading(false);
    }
  }

  function stopTrace() {
    traceRequest.current?.abort();
    traceRequest.current = null;
    traceStream.current?.close();
    traceStream.current = null;
    setTraceLoading(false);
  }

  function selectTrace(runId: string) {
    traceRevision.current += 1;
    stopTrace();
    setTraceRunId(runId);
    setTraceJson(null);
    setSseEvents([]);
    showError("traces", null);
  }

  async function handleLoadTrace() {
    const runId = traceRunId.trim();
    if (!runId) return;
    selectTrace(runId);
    const request = new AbortController();
    traceRequest.current = request;
    setTraceLoading(true);
    try {
      const payload = await gatewayApi.getTrace(runId, request.signal);
      if (!request.signal.aborted) setTraceJson(payload);
    } catch (err) {
      if (!request.signal.aborted) {
        showError("traces", errorMessage(err, "The trace could not be loaded."));
      }
    } finally {
      if (traceRequest.current === request) {
        traceRequest.current = null;
        setTraceLoading(false);
      }
    }
  }

  function handleStreamTrace() {
    const runId = traceRunId.trim();
    if (!runId) return;
    selectTrace(runId);
    const stream = new EventSource(traceStreamUrl(runId));
    traceStream.current = stream;
    setTraceLoading(true);
    stream.addEventListener("span", (event: MessageEvent<string>) => {
      if (traceStream.current === stream) {
        setSseEvents((previous) => [...previous, event.data]);
      }
    });
    stream.addEventListener("done", (event: MessageEvent<string>) => {
      if (traceStream.current !== stream) return;
      setSseEvents((previous) => [...previous, `DONE: ${event.data}`]);
      stopTrace();
    });
    stream.onerror = () => {
      if (traceStream.current !== stream) return;
      stopTrace();
      showError("traces", "The trace stream ended before completion.");
    };
  }

  function resetRedactionResult() {
    redactionRequest.current?.abort();
    redactionRequest.current = null;
    setRedactionLoading(false);
    setRedactionResult(null);
    showError("redaction", null);
  }

  async function handleEvalRedaction(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    resetRedactionResult();
    const request = new AbortController();
    redactionRequest.current = request;
    setRedactionLoading(true);
    try {
      const res = await gatewayApi.evalRedaction({
        text: redactionInput,
        mode: redactionMode,
      }, request.signal);
      if (!request.signal.aborted) setRedactionResult(res);
    } catch (err) {
      if (!request.signal.aborted) {
        showError("redaction", errorMessage(err, "The redaction evaluation could not be run."));
      }
    } finally {
      if (redactionRequest.current === request) {
        redactionRequest.current = null;
        setRedactionLoading(false);
      }
    }
  }

  function handleTabChange(tab: TabId) {
    if (activeTab === "traces" && tab !== "traces") {
      if (traceStream.current) setSseEvents([]);
      stopTrace();
      if (redactionRequest.current) resetRedactionResult();
    }
    setActiveTab(tab);
  }

  const visibleErrors = [
    errors.health,
    errors.meta,
    errors[activeTab],
    activeTab === "traces" && errors.redaction,
  ];

  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <h1 className="brand-title">LLM Quota Throttle Gateway</h1>
          <p className="brand-subtitle">
            Dual RPM+TPM Control Plane · Reserve/Settle Ledger · Weighted DRR · Failover
          </p>
        </div>
        <div className="pill-row">
          <span
            className={`badge ${HEALTH_DISPLAY[health].style}`}
            role="status"
          >
            <Activity size={13} />
            {HEALTH_DISPLAY[health].label}
          </span>
          {meta && (
            <>
              <span className="badge">
                <Cpu size={13} />
                upstream throttled-py v{meta.upstream_version} ({meta.upstream_commit_sha.slice(0, 7)})
              </span>
              <span className="badge">store max_keys={meta.store_max_keys}</span>
            </>
          )}
        </div>
      </header>

      <nav className="tablist" role="tablist" aria-label="Gateway control plane views">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            type="button"
            role="tab"
            className="tab-btn"
            aria-selected={activeTab === tab.id}
            onClick={() => handleTabChange(tab.id)}
          >
            {tab.id === "compare" && <GitCompare size={15} />}
            {tab.id === "ledger" && <Layers size={15} />}
            {tab.id === "fairness" && <Activity size={15} />}
            {tab.id === "traces" && <ShieldCheck size={15} />}
            {tab.id === "benchmarks" && <BarChart3 size={15} />}
            {tab.label}
          </button>
        ))}
      </nav>

      {visibleErrors.map((error, index) => error && (
        <div className="banner-error" role="alert" key={index}>
          <AlertTriangle size={15} style={{ verticalAlign: "middle", marginRight: 6 }} />
          {error}
        </div>
      ))}

      {activeTab === "compare" && (
        <section aria-label="Baseline versus enhanced comparison">
          <form
            className="panel config-panel"
            onSubmit={(event) => void handleRunCompare(event)}
            onChange={() => setCompareResult(null)}
          >
            <h2 className="panel-title">Dual-Engine Replay Configuration</h2>
            <fieldset className="config-fields" disabled={compareLoading || scenarios.length === 0}>
              <div className="form-grid">
                <div className="field">
                  <label htmlFor="scenario-select">Stress Scenario</label>
                  <select
                    id="scenario-select"
                    value={selectedScenario}
                    onChange={(e) => handleScenarioChange(e.target.value)}
                    required
                  >
                    {scenarios.length === 0 && (
                      <option value={selectedScenario}>
                        {errors.compare ? "No scenarios available" : "Loading scenarios…"}
                      </option>
                    )}
                    {scenarios.map((sc) => (
                      <option key={sc.scenario_id} value={sc.scenario_id}>
                        {sc.title} ({sc.event_count} reqs)
                      </option>
                    ))}
                  </select>
                </div>
                <div className="field">
                  <label htmlFor="rpm-limit-input">RPM Limit</label>
                  <input
                    id="rpm-limit-input"
                    type="number"
                    min={1}
                    max={100_000}
                    required
                    value={rpmLimit}
                    onChange={(e) => setRpmLimit(e.target.value)}
                  />
                </div>
                <div className="field">
                  <label htmlFor="tpm-limit-input">TPM Limit</label>
                  <input
                    id="tpm-limit-input"
                    type="number"
                    min={1}
                    max={100_000_000}
                    required
                    value={tpmLimit}
                    onChange={(e) => setTpmLimit(e.target.value)}
                  />
                </div>
                <div className="field">
                  <label htmlFor="window-sec-input">Window (sec)</label>
                  <input
                    id="window-sec-input"
                    type="number"
                    min={1}
                    max={3600}
                    required
                    value={windowSeconds}
                    onChange={(e) => setWindowSeconds(e.target.value)}
                  />
                </div>
                <div className="field">
                  <label htmlFor="usd-budget-input">USD Budget Cap</label>
                  <input
                    id="usd-budget-input"
                    type="number"
                    min={0}
                    max={100_000}
                    step="any"
                    required
                    value={usdBudget}
                    onChange={(e) => setUsdBudget(e.target.value)}
                  />
                </div>
              </div>
              <div className="btn-row">
                <label style={{ fontSize: "0.85rem", color: "var(--text-secondary)" }}>
                  <input
                    type="checkbox"
                    checked={failFastOversized}
                    onChange={(e) => setFailFastOversized(e.target.checked)}
                    style={{ marginRight: 6 }}
                  />
                  Classify oversized requests as immediate failures
                </label>
                <button
                  type="submit"
                  className="btn btn-primary"
                >
                  {compareLoading ? "Running Replay..." : "Run Side-by-Side Comparison"}
                </button>
              </div>
            </fieldset>
          </form>

          {compareResult && (
            <>
              {!compareResult.baseline.upstream_supported && (
                <div className="banner-warn">
                  <strong>Upstream Baseline Limitation Exposed:</strong>{" "}
                  {compareResult.baseline.upstream_error}
                </div>
              )}

              <div className="grid-cards">
                <div className="panel">
                  <div className="kpi-label">Phantom Burn Prevented</div>
                  <div className="kpi-value" style={{ color: "var(--status-ok)" }}>
                    {compareResult.delta.phantom_burn_prevented}
                  </div>
                  <div className="kpi-note">
                    Baseline leaked {compareResult.baseline.phantom_burn_units} units
                  </div>
                </div>
                <div className="panel">
                  <div className="kpi-label">TPM Reclaimed via Settle</div>
                  <div className="kpi-value" style={{ color: "var(--status-ok)" }}>
                    {compareResult.delta.tpm_reclaimed.toLocaleString()}
                  </div>
                  <div className="kpi-note">
                    {compareResult.baseline.wasted_tpm_reserved === null
                      ? "Baseline refund accounting is unavailable"
                      : `Baseline left ${compareResult.baseline.wasted_tpm_reserved.toLocaleString()} tokens unused`}
                  </div>
                </div>
                <div className="panel">
                  <div className="kpi-label">Enhanced Admitted / Rejected</div>
                  <div className="kpi-value">
                    {compareResult.enhanced.admitted} / {compareResult.enhanced.rejected}
                  </div>
                  <div className="kpi-note">
                    {compareResult.enhanced.total_cost_usd === null
                      ? "Actual cost unavailable"
                      : `Actual cost $${compareResult.enhanced.total_cost_usd.toFixed(5)}`}
                  </div>
                </div>
                <div className="panel">
                  <div className="kpi-label">Enhanced P99 Decision</div>
                  <div className="kpi-value">{compareResult.enhanced.p99_decision_ms} ms</div>
                  <div className="kpi-note">Run ID: {compareResult.run_id}</div>
                </div>
              </div>

              <div className="panel">
                <h3 className="panel-title">Per-Event Decision Preview</h3>
                <div
                  className="table-region"
                  role="region"
                  aria-label="Comparison decision events"
                  tabIndex={0}
                >
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Event</th>
                        <th>Tenant</th>
                        <th>Model</th>
                        <th>Reserved</th>
                        <th>Actual</th>
                        <th>Refunded</th>
                        <th>Decision</th>
                      </tr>
                    </thead>
                    <tbody>
                      {compareResult.events_preview.map((ev, index) => (
                        <tr key={`${ev.event_id}-${index}`}>
                          <td>{ev.event_id}</td>
                          <td>{ev.tenant_id}</td>
                          <td>{ev.model}</td>
                          <td>{ev.reserved_tokens}</td>
                          <td>{ev.actual_tokens}</td>
                          <td>{ev.refunded_tokens}</td>
                          <td>
                            <span
                              className={`badge ${ev.enhanced_allowed ? "badge-ok" : "badge-err"}`}
                            >
                              {ev.enhanced_reason}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </>
          )}
        </section>
      )}

      {activeTab === "ledger" && (
        <section aria-label="Dual quota reservation ledger">
          <form
            className="panel config-panel"
            onSubmit={(event) => void handleCreateReservation(event)}
            onChange={() => { reserveAttempt.current = null; }}
          >
            <h2 className="panel-title">Reserve -&gt; Settle/Refund Interactive Console</h2>
            <fieldset className="config-fields" disabled={ledgerLoading}>
              <div className="form-grid">
                <div className="field">
                  <label htmlFor="resv-tenant">Tenant ID</label>
                  <input
                    id="resv-tenant"
                    value={tenantId}
                    maxLength={64}
                    pattern={"[A-Za-z0-9_.\\-]+"}
                    required
                    onChange={(e) => setTenantId(e.target.value)}
                  />
                </div>
                <div className="field">
                  <label htmlFor="resv-model">Model</label>
                  <select
                    id="resv-model"
                    value={modelName}
                    onChange={(e) => setModelName(e.target.value)}
                  >
                    <option value="gpt-4o-mini">gpt-4o-mini</option>
                    <option value="gpt-4o">gpt-4o</option>
                    <option value="claude-3-5-sonnet">claude-3-5-sonnet</option>
                    <option value="llama-3-70b">llama-3-70b</option>
                  </select>
                </div>
                <div className="field">
                  <label htmlFor="resv-prompt">Prompt Tokens</label>
                  <input
                    id="resv-prompt"
                    type="number"
                    min={0}
                    max={MAX_TOKEN_COUNT}
                    required
                    value={promptTokens}
                    onChange={(e) => setPromptTokens(e.target.value)}
                  />
                </div>
                <div className="field">
                  <label htmlFor="resv-maxcomp">Max Completion Tokens</label>
                  <input
                    id="resv-maxcomp"
                    type="number"
                    min={0}
                    max={MAX_TOKEN_COUNT}
                    required
                    value={maxCompletionTokens}
                    onChange={(e) => setMaxCompletionTokens(e.target.value)}
                  />
                </div>
              </div>
              <div className="field" style={{ marginBottom: 12 }}>
                <label htmlFor="resv-payload">Optional Prompt Payload (Redaction Checked)</label>
                <input
                  id="resv-payload"
                  value={payloadText}
                  maxLength={MAX_PROMPT_LENGTH}
                  onChange={(e) => setPayloadText(e.target.value)}
                />
              </div>
              <button
                type="submit"
                className="btn btn-primary"
              >
                Reserve Quota (SQLite WAL)
              </button>
            </fieldset>
          </form>

          <div className="panel">
            <h3 className="panel-title">Recent Quota Reservations</h3>
            <div
              className="table-region"
              role="region"
              aria-label="Quota reservations table"
              tabIndex={0}
            >
              <table className="data-table">
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Tenant</th>
                    <th>Model</th>
                    <th>Status</th>
                    <th>Reserved</th>
                    <th>Refunded</th>
                    <th>Cost USD</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {reservations?.map((r) => (
                    <tr key={r.reservation_id}>
                      <td>{r.reservation_id}</td>
                      <td>{r.tenant_id}</td>
                      <td>{r.model}</td>
                      <td>
                        <span
                          className={`badge ${
                            r.status === "reserved" ? "badge-warn" : "badge-ok"
                          }`}
                        >
                          {r.status}
                        </span>
                      </td>
                      <td>{r.reserved_tokens}</td>
                      <td>{r.refunded_tokens}</td>
                      <td>${(r.actual_cost_usd ?? r.estimated_cost_usd).toFixed(5)}</td>
                      <td>
                        {r.status === "reserved" ? (
                          <button
                            type="button"
                            className="btn"
                            disabled={ledgerLoading}
                            aria-expanded={settlement?.reservationId === r.reservation_id}
                            aria-controls={
                              settlement?.reservationId === r.reservation_id
                                ? "settlement-form"
                                : undefined
                            }
                            onClick={() => {
                              showError("ledger", null);
                              setSettlement((current) => current?.reservationId === r.reservation_id
                                ? current
                                : { reservationId: r.reservation_id, promptTokens: "", completionTokens: "" }
                              );
                            }}
                          >
                            Settle…
                          </button>
                        ) : (
                          <span>Closed</span>
                        )}
                      </td>
                    </tr>
                  ))}
                  {(!reservations || reservations.length === 0) && (
                    <tr>
                      <td colSpan={8} style={{ color: "var(--text-muted)" }}>
                        {reservations
                          ? "No reservations recorded yet."
                          : errors.ledger ? "The ledger is unavailable." : "Loading reservations…"}
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
            {settlement && (
              <form
                id="settlement-form"
                key={settlement.reservationId}
                className="settlement-form"
                onSubmit={(event) => void handleSettle(event)}
              >
                <h4 className="panel-title" aria-live="polite">
                  Settle {settlement.reservationId}
                </h4>
                <fieldset className="config-fields" disabled={ledgerLoading}>
                  <div className="form-grid">
                    <div className="field">
                      <label htmlFor="actual-prompt-tokens">Actual Prompt Tokens</label>
                      <input
                        id="actual-prompt-tokens"
                        type="number"
                        min={0}
                        max={MAX_TOKEN_COUNT}
                        required
                        autoFocus
                        value={settlement.promptTokens}
                        onChange={(event) => setSettlement({
                          ...settlement,
                          promptTokens: event.target.value,
                        })}
                      />
                    </div>
                    <div className="field">
                      <label htmlFor="actual-completion-tokens">Actual Completion Tokens</label>
                      <input
                        id="actual-completion-tokens"
                        type="number"
                        min={0}
                        max={MAX_TOKEN_COUNT}
                        required
                        value={settlement.completionTokens}
                        onChange={(event) => setSettlement({
                          ...settlement,
                          completionTokens: event.target.value,
                        })}
                      />
                    </div>
                  </div>
                  <div className="btn-row">
                    <button type="submit" className="btn btn-primary">Confirm Settlement</button>
                    <button type="button" className="btn" onClick={() => setSettlement(null)}>
                      Cancel
                    </button>
                  </div>
                </fieldset>
              </form>
            )}
          </div>
        </section>
      )}

      {activeTab === "fairness" && (
        <section aria-label="Weighted DRR fairness and failover simulator">
          <form
            className="panel config-panel"
            onSubmit={(event) => void handleSimulateFairness(event)}
            onChange={() => setFairnessResult(null)}
          >
            <h2 className="panel-title">Weighted DRR Fairness &amp; Provider 429 Failover</h2>
            <fieldset className="config-fields" disabled={fairnessLoading}>
              <div className="form-grid">
                <div className="field">
                  <label htmlFor="fair-cap">Total TPM Capacity</label>
                  <input
                    id="fair-cap"
                    type="number"
                    min={1000}
                    max={10_000_000}
                    required
                    value={totalTpmCap}
                    onChange={(e) => setTotalTpmCap(e.target.value)}
                  />
                </div>
                <div className="field">
                  <label htmlFor="fair-quantum">Quantum Tokens</label>
                  <input
                    id="fair-quantum"
                    type="number"
                    min={100}
                    max={100_000}
                    required
                    value={quantumTokens}
                    onChange={(e) => setQuantumTokens(e.target.value)}
                  />
                </div>
                <div className="field">
                  <label htmlFor="fair-rounds">Rounds</label>
                  <input
                    id="fair-rounds"
                    type="number"
                    min={1}
                    max={100}
                    required
                    value={simRounds}
                    onChange={(e) => setSimRounds(e.target.value)}
                  />
                </div>
              </div>
              <div className="btn-row">
                <label style={{ fontSize: "0.85rem", color: "var(--text-secondary)" }}>
                  <input
                    type="checkbox"
                    checked={inject429}
                    onChange={(e) => setInject429(e.target.checked)}
                    style={{ marginRight: 6 }}
                  />
                  Inject primary provider 429/503 faults
                </label>
                <button
                  type="submit"
                  className="btn btn-primary"
                >
                  {fairnessLoading ? "Simulating..." : "Run DRR Fairness Simulation"}
                </button>
              </div>
            </fieldset>
          </form>

          {fairnessResult && (
            <>
              <div className="grid-cards">
                <div className="panel">
                  <div className="kpi-label">FIFO Jain Fairness</div>
                  <div className="kpi-value" style={{ color: "var(--status-warn)" }}>
                    {fairnessResult.jains_fairness_baseline_fifo.toFixed(4)}
                  </div>
                  <div className="kpi-note">
                    Starvation events: {fairnessResult.starvation_events_fifo}
                  </div>
                </div>
                <div className="panel">
                  <div className="kpi-label">Weighted DRR Jain Fairness</div>
                  <div className="kpi-value" style={{ color: "var(--status-ok)" }}>
                    {fairnessResult.jains_fairness_drr.toFixed(4)}
                  </div>
                  <div className="kpi-note">
                    Starvation events: {fairnessResult.starvation_events_drr}
                  </div>
                </div>
                <div className="panel">
                  <div className="kpi-label">Failover Transitions</div>
                  <div className="kpi-value">{fairnessResult.failover_events.length}</div>
                  <div className="kpi-note">Run ID: {fairnessResult.run_id}</div>
                </div>
              </div>

              <div className="grid-two">
                <div className="panel">
                  <h3 className="panel-title">Tenant Throughput &amp; Starvation</h3>
                  <div className="table-region" role="region" aria-label="Tenant fairness" tabIndex={0}>
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>Tenant</th>
                          <th>Weight</th>
                          <th>FIFO Served</th>
                          <th>DRR Served</th>
                          <th>DRR Ratio</th>
                        </tr>
                      </thead>
                      <tbody>
                        {fairnessResult.tenant_outcomes.map((t) => (
                          <tr key={t.tenant_id}>
                            <td>{t.tenant_id}</td>
                            <td>{t.weight}x</td>
                            <td>{t.fifo_served_tokens}</td>
                            <td>{t.drr_served_tokens}</td>
                            <td>{(t.fulfillment_ratio_drr * 100).toFixed(1)}%</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                <div className="panel">
                  <h3 className="panel-title">Provider Circuit Breaker Health</h3>
                  <div className="table-region" role="region" aria-label="Provider health" tabIndex={0}>
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>Provider</th>
                          <th>State</th>
                          <th>429/5xx</th>
                        <th>Requests Served</th>
                        </tr>
                      </thead>
                      <tbody>
                        {fairnessResult.provider_health.map((p) => (
                          <tr key={p.provider}>
                            <td>{p.provider}</td>
                            <td>
                              <span
                                className={`badge ${
                                  p.state === "closed"
                                    ? "badge-ok"
                                    : p.state === "open"
                                      ? "badge-err"
                                      : "badge-warn"
                                }`}
                              >
                                {p.state}
                              </span>
                            </td>
                            <td>{p.total_429_5xx}</td>
                            <td>{p.routed_requests}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </>
          )}
        </section>
      )}

      {activeTab === "traces" && (
        <section aria-label="OpenTelemetry traces and PII redaction">
          <div className="grid-two">
            <div className="panel">
              <h2 className="panel-title">OpenTelemetry Trace &amp; Live SSE Inspector</h2>
              <form onSubmit={(event) => {
                event.preventDefault();
                void handleLoadTrace();
              }}>
                <div className="field" style={{ marginBottom: 12 }}>
                  <label htmlFor="trace-run-input">Run / Reservation ID</label>
                  <input
                    id="trace-run-input"
                    value={traceRunId}
                    maxLength={64}
                    required
                    onChange={(e) => selectTrace(e.target.value)}
                    placeholder="cmp_... or fair_... or resv_..."
                  />
                </div>
                <div className="btn-row" style={{ marginBottom: 12 }}>
                  <button type="submit" className="btn btn-primary" disabled={!traceRunId.trim()}>
                    Load OTLP JSON
                  </button>
                  <button type="button" className="btn" disabled={!traceRunId.trim()} onClick={handleStreamTrace}>
                    Stream SSE Spans
                  </button>
                </div>
              </form>
              {traceLoading && <p className="kpi-note" role="status">Reading trace…</p>}
              {sseEvents.length > 0 && (
                <pre className="code-box">{sseEvents.join("\n")}</pre>
              )}
              {traceJson && (
                <pre className="code-box">{JSON.stringify(traceJson, null, 2)}</pre>
              )}
            </div>

            <form className="panel" onSubmit={(event) => void handleEvalRedaction(event)}>
              <h2 className="panel-title">Secret &amp; PII Redaction Evaluator</h2>
              <div className="field" style={{ marginBottom: 10 }}>
                <label htmlFor="redaction-mode">Policy Mode</label>
                <select
                  id="redaction-mode"
                  value={redactionMode}
                  onChange={(e) => {
                    setRedactionMode(e.target.value as "mask" | "block" | "audit");
                    resetRedactionResult();
                  }}
                >
                  <option value="mask">mask</option>
                  <option value="block">block</option>
                  <option value="audit">audit</option>
                </select>
              </div>
              <div className="field" style={{ marginBottom: 12 }}>
                <label htmlFor="redaction-text">Prompt Payload</label>
                <textarea
                  id="redaction-text"
                  rows={4}
                  value={redactionInput}
                  maxLength={MAX_PROMPT_LENGTH}
                  required
                  onChange={(e) => {
                    setRedactionInput(e.target.value);
                    resetRedactionResult();
                  }}
                />
              </div>
              <button
                type="submit"
                className="btn btn-primary"
                disabled={redactionLoading}
              >
                {redactionLoading ? "Evaluating…" : "Evaluate Redaction"}
              </button>
              {redactionResult && (
                <div style={{ marginTop: 12 }}>
                  <div className="pill-row" style={{ marginBottom: 8 }}>
                    <span className={`badge ${redactionResult.allowed ? "badge-ok" : "badge-err"}`}>
                      {redactionResult.allowed ? "ALLOWED" : "BLOCKED"}
                    </span>
                    <span className="badge">{redactionResult.latency_ms} ms</span>
                  </div>
                  {redactionResult.findings.length > 0 && (
                    <ul className="redaction-findings">
                      {redactionResult.findings.map(({ rule, count }) => (
                        <li key={rule}>{count} × {rule.replaceAll("_", " ")}</li>
                      ))}
                    </ul>
                  )}
                  <pre className="code-box">{redactionResult.redacted_text}</pre>
                </div>
              )}
            </form>
          </div>
        </section>
      )}

      {activeTab === "benchmarks" && (
        <section aria-label="External sealed benchmark results">
          <div className="panel">
            <h2 className="panel-title">External 3-Tier Sealed Benchmarks (docs/test_metrics.json)</h2>
            {benchmarks === null ? (
              !errors.benchmarks && <p className="kpi-note" role="status">Loading benchmark results…</p>
            ) : !benchmarks.available ? (
              <div className="banner-warn">
                No sealed benchmark artifact found yet. Run{" "}
                <code>python benchmarks/run_sealed_benchmarks.py</code> to evaluate Azure &amp;
                BurstGPT traces.
              </div>
            ) : (
              <pre className="code-box">{JSON.stringify(benchmarks.metrics, null, 2)}</pre>
            )}
          </div>
        </section>
      )}
    </div>
  );
}
