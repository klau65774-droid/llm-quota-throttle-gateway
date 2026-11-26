# LLM Quota Throttle Gateway

在本地 CPU 运行的 LLM 配额工作台，提供 RPM 与 TPM 双维配额、SQLite WAL 预留和结算账本，以及可复现的加权 Deficit Round Robin（DRR）与 Provider 熔断模拟。

[English README](README.md) · [API 契约](docs/API_CONTRACT.md) · [使用与架构](docs/USAGE_GUIDE.md) · [数据集](datasets/README.md) · [独立审计](docs/AUDIT_AND_TEST_REPORT.md)

## 基线与增强功能

| 审计项 | 固定上游回放 | 增强工作台 |
| --- | --- | --- |
| RPM | 运行上游滑动窗口的内存动作，实测拒绝时的写入。上游动作可能在核验新窗口前创建计数。 | 以整数计算加权用量，先检查再写入，任一预留步骤失败时回滚相关配额。 |
| RPM、TPM 和 USD | 固定版本会拒绝 RPM 与 TPM 的组合配额字符串。 | 预留检查全局、租户和模型的 RPM/TPM，并使用持久化的租户 USD 预算。结算返还剩余 token 或扣记透支。 |
| 时间精度 | 上游令牌桶采用整秒时间。 | 增强令牌桶采用单调纳秒时钟，超容量预留会立即拒绝。 |
| 状态容量 | 上游内存状态达容量后淘汰最旧键。 | 增强状态先清理过期项，再淘汰空闲键。全部键都活跃时拒绝接纳新状态。 |
| 公平性和 Provider | 对照接口报告实测的 RPM 准入指标。 | 专用模拟对比同一 token 容量下的 FIFO 和加权 DRR，记录实际模拟的路由尝试和熔断状态。 |

## 本地 CPU 基准结果

本次审计校验了发布方原始输入，通过虚拟时钟按源时间回放。两组外部轨迹在配置限额下全部准入。容量、拒绝、公平性、Provider 和脱敏门禁使用合成输入。吞吐量和决策延迟为本机内存回放与结算测量。见[机器可读结果](docs/test_metrics.json)和[回放方法](datasets/README.md)。

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

## 本地启动与验证

使用 Python 3.12 和前端工具链支持的 Node 22 或更新版本。

~~~powershell
uv venv .venv --python 3.12
uv pip install --python .venv\Scripts\python.exe -e .\backend[dev]
.\.venv\Scripts\ruff.exe check backend\llm_gateway backend\scripts tests\backend benchmarks
.\.venv\Scripts\mypy.exe --config-file backend\pyproject.toml backend\llm_gateway backend\scripts tests\backend benchmarks
.\.venv\Scripts\python.exe -m pytest tests\backend -v
.\.venv\Scripts\python.exe benchmarks\run_sealed_benchmarks.py
~~~

在两个终端启动后端和前端。

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

API 和控制台适用于可信本地用户的单进程环境。账本与 USD 预算持久化，RPM/TPM 配额状态位于当前进程内存。Provider 行为由模拟器实现。请使用演示标识和合成数据，详见 [API 运行范围](docs/API_CONTRACT.md)。

[桌面配额账本界面](docs/screenshots/02_ledger_desktop.png) · [桌面基线对照界面](docs/screenshots/01_compare_desktop.png) · [桌面公平与容灾界面](docs/screenshots/03_fairness_desktop.png) · [链路与脱敏界面](docs/screenshots/04_traces_redaction_desktop.png) · [三层基准面板](docs/screenshots/05_benchmarks_desktop.png) · [移动端响应式界面](docs/screenshots/06_compare_mobile.png)

## 许可和来源

上游源码声明的包版本为 `3.5.0`。项目将基线固定在提交 `d20114067281038e5c5fb8de71d30a609d3cb7f9`。上游发布标签则解析到提交 `8626d3bd960e247e125bdc1e8c497a0a4eb96eb1`。[固定源码](https://github.com/ZhuoZhuoCrayon/throttled-py/commit/d20114067281038e5c5fb8de71d30a609d3cb7f9) · [上游发布标签](https://github.com/ZhuoZhuoCrayon/throttled-py/tree/v3.5.0)

项目和上游仓库使用 MIT，保留原始署名 `Copyright (c) 2025 crayon`。上游 Redis 连接池文件另有原始 BSD 许可声明。Azure 和 BurstGPT 轨迹由发布方按 CC BY 4.0 分发，原始文件保存在仓库外。[MIT 许可](LICENSE) · [Redis 连接池许可](backend/upstream_baseline/throttled/store/redis_pool.py) · [数据来源和许可](datasets/README.md)
