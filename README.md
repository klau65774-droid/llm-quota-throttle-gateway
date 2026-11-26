# LLM Quota Throttle Gateway

A CPU-only local workbench for dual RPM and TPM quotas, reservation settlement, SQLite WAL accounting, and deterministic weighted Deficit Round Robin (DRR) and provider failover simulation.

[中文文档](README.zh.md) · [API contract](docs/API_CONTRACT.md) · [Usage and architecture](docs/USAGE_GUIDE.md) · [Datasets](datasets/README.md) · [Independent audit](docs/AUDIT_AND_TEST_REPORT.md)

## Baseline and enhanced behavior

| Area | Pinned upstream replay | Enhanced workbench |
| --- | --- | --- |
| RPM | Executes the vendored sliding-window memory action and measures actual writes on denied calls. The upstream action can create a counter before it checks a fresh window. | Checks weighted usage with integer arithmetic before writing and rolls back composite reservations on failure. |
| RPM, TPM and USD | The pinned library rejects the combined RPM and TPM quota string. | The reservation endpoint checks global, tenant and model RPM/TPM plus a durable tenant USD budget. Settlement refunds unused tokens or charges an overrun. |
| Timing | The upstream token bucket uses integer-second time. | The enhanced token bucket refills using monotonic nanoseconds and immediately rejects an oversized reservation. |
| Bounded state | The upstream memory backend evicts the oldest entry at capacity. | The enhanced store sweeps expired entries, evicts idle keys and rejects new state when every key is live. |
| Fairness and provider status | Comparison reports observed RPM admission metrics. | The dedicated simulator measures FIFO and weighted DRR under the same token budget, then tracks simulated provider attempts and circuit changes. |

## Local CPU benchmark results

The audit run verified the publisher files and used source timestamps with a virtual clock. Both external trace replays admitted every request at the configured limits. The capacity, rejection, fairness, provider and redaction gates use synthetic inputs. Throughput and decision latency cover the in-memory replay and settlement on the audit host. See the [machine-readable results](docs/test_metrics.json) and [replay methodology](datasets/README.md).

<!-- benchmark-metrics:begin -->

| Measurement | Result |
| --- | --- |
| Report status | `passed` |
| Overall pass | `true` |
| Gates passed | `13/13` |
| Azure requests / admitted / rejected | `28185` / `28185` / `0` |
| Azure reserved / actual / refunded / overrun tokens | `98144724` / `44756405` / `53388319` / `0` |
| Azure refunds (%) | `54.4` |
| Azure throughput (req/s) / P50 (ms) / P99 (ms) | `72464.9` / `0.012` / `0.0261` |
| BurstGPT events / admitted / rejected | `50000` / `50000` / `0` |
| BurstGPT ChatGPT / GPT-4 / zero-response events | `40506` / `9494` / `1957` |
| BurstGPT reserved / actual / refunded / overrun tokens | `123337185` / `28183853` / `95168084` / `14752` |
| BurstGPT refunds (%) | `77.16` |
| BurstGPT throughput (req/s) / P50 (ms) / P99 (ms) | `52506.9` / `0.0131` / `0.0567` |
| Synthetic rejected calls checked / added RPM units | `500` / `0` |
| Expired keys swept / idle keys evicted / active keys evicted | `192` / `2809` / `0` |
| Active limited keys retained / seeded | `64` / `64` |
| Oversized requests rejected / cumulative time (ms) | `100` / `0.0639` |
| Synthetic Jain index (FIFO / DRR) | `0.333333` / `0.809266` |
| Synthetic starvation events (FIFO / DRR) | `24` / `10` |
| Failover events / primary failures / backup attempts | `2` / `2` / `3` |
| Redaction detections / positive inputs / benign false positives / benign inputs | `6` / `6` / `0` / `3` |
| Large redaction scan (characters / ms) | `61398` / `6.3946` |

<!-- benchmark-metrics:end -->

## Quickstart

Use Python 3.12 and Node 22 or newer supported by the frontend toolchain.

~~~powershell
uv venv .venv --python 3.12
uv pip install --python .venv\Scripts\python.exe -e .\backend[dev]
.\.venv\Scripts\ruff.exe check backend\llm_gateway backend\scripts tests\backend benchmarks
.\.venv\Scripts\mypy.exe --config-file backend\pyproject.toml backend\llm_gateway backend\scripts tests\backend benchmarks
.\.venv\Scripts\python.exe -m pytest tests\backend -v
.\.venv\Scripts\python.exe benchmarks\run_sealed_benchmarks.py
~~~

Run the backend and frontend in separate terminals.

~~~powershell
$env:PYTHONPATH = "backend"
.\.venv\Scripts\python.exe -m uvicorn llm_gateway.api:app --host 127.0.0.1 --port 8000
~~~

~~~powershell
cd frontend
npm ci
npm test
npm run build
npm run dev
~~~

The API and console are designed for a trusted local user with one backend process. The ledger and USD budget are durable. RPM and TPM state live in memory for that process. Provider behavior is simulated. Send demo identifiers and synthetic inputs to the workbench. See the [API operating scope](docs/API_CONTRACT.md).

[Desktop ledger interface](docs/screenshots/02_ledger_desktop.png) · [Desktop comparison view](docs/screenshots/01_compare_desktop.png) · [Desktop fairness view](docs/screenshots/03_fairness_desktop.png) · [Traces and redaction](docs/screenshots/04_traces_redaction_desktop.png) · [Sealed benchmarks view](docs/screenshots/05_benchmarks_desktop.png) · [Mobile comparison view](docs/screenshots/06_compare_mobile.png)

## License and provenance

The upstream source declares package version `3.5.0`. The baseline is pinned to commit `d20114067281038e5c5fb8de71d30a609d3cb7f9`. The upstream release tag resolves separately to `8626d3bd960e247e125bdc1e8c497a0a4eb96eb1`. [Pinned source](https://github.com/ZhuoZhuoCrayon/throttled-py/commit/d20114067281038e5c5fb8de71d30a609d3cb7f9) · [Release tag](https://github.com/ZhuoZhuoCrayon/throttled-py/tree/v3.5.0)

The project and upstream repository use MIT, with the original `Copyright (c) 2025 crayon` attribution retained. The vendored Redis pool file also carries its original BSD notice. The Azure and BurstGPT traces are distributed by their publishers under CC BY 4.0 and are stored outside the repository. [MIT license](LICENSE) · [Redis pool notice](backend/upstream_baseline/throttled/store/redis_pool.py) · [Dataset sources and terms](datasets/README.md)
