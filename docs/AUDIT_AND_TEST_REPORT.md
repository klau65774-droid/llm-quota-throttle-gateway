# LLM Quota Throttle Gateway 独立审计与发布验收

审计日期为 **2026-11-26（Asia/Taipei）**。结论为 **No-Go（公开 GitHub 发布）**。代码回归、API 契约、依赖复核和三层基准均通过。拟发布的固定上游基线包含两处公开作者联系信息，其中一处是第三方版权声明的一部分。按此次全量零个人 PII 门禁，两处均需计入。保持基线逐文件一致的要求有效，发布门禁因此仍未通过。

被审计对象是当前工作树。Git 位于尚无提交的 `main`，索引为空。隐私扫描延伸到 Git 未忽略的拟发布文件。基准、旧 README 和现有 JSON 均通过本次运行和源码重新核对。

## 审计覆盖

| 范围 | 核查项和证据 |
| --- | --- |
| `backend/llm_gateway/` | 全部 12 个 Python 模块。核对窗口和桶状态、配额事务、SQLite WAL、调度、Provider 模拟、请求和指标契约、规则脱敏、OTLP/SSE、配置和场景来源。 |
| `backend/scripts/`、`backend/docs/openapi.json`、`docs/API_CONTRACT.md` | OpenAPI 导出脚本、完整文档、错误封装、状态码、安全响应、SSE 媒体类型和配额响应头。 |
| `tests/backend/` | 全部五个测试文件。实测 29 项，含并发、旧数据库迁移、退还上限、迟到结算、限额拒绝、输入边界、未知或损坏报告和基线兼容。 |
| `frontend/src/`、`frontend/package.json`、`frontend/package-lock.json` | React 生命周期与交互，客户端请求和 URL、响应格式、窄屏 CSS、Vitest、TypeScript、生产构建和依赖审计。 |
| `benchmarks/run_sealed_benchmarks.py`、`datasets/README.md`、`docs/test_metrics.json`、两份 README | 发布方来源、输入指纹、时间轴、配额守恒、合成门禁、缺失数据和输出一致性。 |
| 发布材料与基线 | 逐文件校验上游 Git blob 和原始工作树 SHA-256。扫描 Git 候选文件，检查六张 PNG 像素内容及元数据。 |

## 发现与修复

严重度对应修复前的影响。开发依赖一行采用公告的最高严重度。

| 严重度 | 已确认的发现 | 修复和复核 |
| --- | --- | --- |
| Critical | 旧 Vitest 2.1.9 锁文件含 Vitest 与 Tinypool 的严重公告，旧的传递 Vite 和 esbuild 也命中公告。 | 锁定 Vitest 4.1.12，整理官方注册表地址并验证干净安装。更新后的锁文件为 0 条 npm 漏洞。 [Vitest 公告](https://github.com/advisories/GHSA-5xrq-8626-4rwp) · [Tinypool 公告](https://github.com/advisories/GHSA-85c8-ppgw-ccpr) |
| High | 幂等键跨租户共用，预留写入失败可留下内存配额扣减，结算在存储提交前可重复改变配额。 | SQLite 事务内用租户加键指纹去重。旧记录迁移可重放，已有请求输入变化返回 409。一次预留提交包含账本和跟踪。结算比较并更新 `reserved` 状态，失败回滚涉及的桶。多线程、两条连接和写入失败回归通过。 [额度协调](../backend/llm_gateway/quota.py) · [SQLite 存储](../backend/llm_gateway/storage.py) |
| High | 满容量且全为活跃项时会淘汰有效限额。结算透支也未扣 TPM，迟到退还可给新一代桶额外加值。 | 先清扫到期项，再淘汰空闲项。全活跃时返回限额错误。透支按三层记录债务，退还限定于最初的桶代。容量、迟到结算和债务回归通过。 [限流与内存存储](../backend/llm_gateway/compat.py) |
| High | PEM 规则只处理部分首行，可能泄露正文。标准 422 会回显输入，旧配置允许宽泛的浏览器 Origin。 | 脱敏覆盖整个私钥块，处理常见令牌族并返回固定样例掩码。API 在解析对象前限制 Host、Origin 和 8 MiB 请求体。422 仅保留位置、类型和消息，500 保留通用封装。 [脱敏](../backend/llm_gateway/redaction.py) · [HTTP 边界](../backend/llm_gateway/api.py) |
| Medium | 滑动窗浮点临界误差和回填速度下限会改变配额。USD 预算在进程重启时丢失，小金额被过早截位。 | 加权使用整数和单次操作时钟，按实际配额计算补充和重试时间。USD 以 8 位精度按预留窗口读取 WAL，结算回写原窗口。超大请求、TPM 与 USD 拒绝均验证不消耗其他作用域。 [限流实现](../backend/llm_gateway/compat.py) · [账本回归](../tests/backend/test_quota_reserve_settle_wal.py) |
| Medium | FIFO/DRR 的每轮最小分配可超过总容量，DRR 重复从头扫描。Provider 统计把未发出的故障记到主路并自动关闭探测。 | 容量逐轮释放，保留 DRR 游标和权重额度并拒绝重复租户。故障仅计实际路由尝试，半开取决于探测结果。 [调度和 Provider 模拟](../backend/llm_gateway/fairness.py) |
| Medium | 对照把常规拒绝当成误扣，并用输入类别推测等待、淘汰和公平性。显式空工作负载回落到预置场景。 | 同一时钟回放真实上游内存动作，读取实际计数差异和淘汰，公平性纳入零准入租户。无观测值明确返回 `null`，空负载返回零事件与零公平性。 [对照引擎](../backend/llm_gateway/engine.py) |
| Medium | 基准门禁未覆盖所宣称的故障、脱敏和容量行为。旧指标使用事后生成量推算预算，缓存用例也没有真正打满容量。 | 校验三份原始输入指纹，回放真实时间，用事前可知的固定预算，检查退还与透支守恒。新增真实容量压力、结果门禁和输出退出码，外部轨迹与合成公平性分别报告。 [基准实现](../benchmarks/run_sealed_benchmarks.py) |
| Medium | OpenAPI 旧测试只匹配路径。错误状态、重试头、事件流媒体类型与运行时有缺口，旧 wheel 未携带固定基线。 | 运行时与完整 OpenAPI 字典相等。按实际状态和响应头测试。wheel 验证可导入内置上游源码，并能执行 RPM 与生成 schema。 [API 契约](API_CONTRACT.md) · [完整 schema](../backend/docs/openapi.json) |
| Low | 前端用新建表单中的值结算旧记录，旧请求或 SSE 可能覆盖当前结果。旧 README 和部分截图展示历史指标。 | 将实际用量输入绑定到账本选中行，加入取消和响应防护，独立加载健康状态。同步双语指标，界面示例标记为归档。 [前端实现](../frontend/src/App.tsx) |
| Low，发布阻断 | 两处公开联系信息位于固定基线。作者身份或许可来源与文件内容能够对应。 | 按原样保留。隐私核查位置为 `backend/upstream_baseline/pyproject.toml:10` 与 `backend/upstream_baseline/throttled/store/redis_pool.py:1`，报告仅记录位置。 |
| Confirmed Clean | 项目自有文件中的指定标记、基线完整性与运行后 SQLite 残留。 | 在已检查的候选中，项目自有文件无相应的路径、代理端口、原始私钥头、令牌字面量及固定 11 位电话规则命中。基线前后 SHA-256 一致。工作区 `data/` 下临时 SQLite、WAL、SHM 和 journal 文件均为 0。 |

## 上游完整性、许可与隐私证据

固定提交为 `d20114067281038e5c5fb8de71d30a609d3cb7f9`。它的包版本字段为 `3.5.0`。上游 `v3.5.0` 发布标签解析为 `8626d3bd960e247e125bdc1e8c497a0a4eb96eb1`。README 使用包版本与固定提交，并分别注明发布标签。 [固定提交](https://github.com/ZhuoZhuoCrayon/throttled-py/commit/d20114067281038e5c5fb8de71d30a609d3cb7f9) · [发布标签](https://github.com/ZhuoZhuoCrayon/throttled-py/tree/v3.5.0)

`LICENSE`、`pyproject.toml`、`throttled/` 和 `tests/` 的文件集合与上游 Git tree 相等。原始工作树为 15 个逐字节相等和 123 个仅 CRLF 差异。末次 SHA-256 与初始清单的 139 个文件全部一致，含项目的 `UPSTREAM_META.json`。使用仓库根目录的明确 LF 属性计算发布 Git blob，138/138 与固定提交相等，含上游 MIT 许可。主许可保留 `Copyright (c) 2025 crayon`，Redis 连接池包含原始 BSD 三条款声明。 [主许可](../LICENSE) · [上游许可](../backend/upstream_baseline/LICENSE) · [Redis 连接池声明](../backend/upstream_baseline/throttled/store/redis_pool.py)

报告写入前审查了 190 个 Git 候选，其中 169 个非空 UTF-8 文本及 6 张 PNG。其余 15 个均为空文件。没有图片含文本或 EXIF 区块，像素内容是历史合成界面示例。上游的 4 个路径样式命中均为测试的 `/users/1` 或 `/users/2` 路由。两个非示例域联系地址来自前述作者元数据和版权声明。

## 实测基准

| 指标 | Azure | BurstGPT |
| --- | ---: | ---: |
| 回放请求 | 28185 | 50000 |
| 准入 / 拒绝 | 28185 / 0 | 50000 / 0 |
| 预留 token | 98144724 | 123337185 |
| 结算 token | 44756405 | 28183853 |
| 退还 token | 53388319 | 95168084 |
| 透支 token | 0 | 14752 |
| 退还率 | 54.4% | 77.16% |
| 吞吐量 req/s | 72464.9 | 52506.9 |
| 决策 P50 / P99 ms | 0.012 / 0.0261 | 0.0131 / 0.0567 |

BurstGPT 包含 40506 条 ChatGPT、9494 条 GPT-4 和 1957 条零响应记录。按源时间回放，两组轨迹都没有触发配置限额。吞吐量和延迟是此 Windows 主机上的内存回放和结算测量，计时范围由数据说明定义。三份 CSV 的 SHA-256 和发布方 Git blob 均记录在数据说明。 [数据集与测量方法](../datasets/README.md) · [原始指标](test_metrics.json)

| 合成门禁 | 实测 |
| --- | --- |
| 滑动窗口 | 500 次拒绝，增加 0 个 RPM 单位。过期窗口退还不超原计数。 |
| 缓存 | 清扫 192 个过期键，淘汰 2809 个空闲键，保留 64/64 个受限键，活跃键淘汰为 0。 |
| 亚秒补充、超大拒绝、透支 | 亚秒补充和透支债务均通过。100/100 次超大请求被立即拒绝，合计 0.0639 ms。 |
| 公平性和熔断 | Jain 指数为 0.333333 / 0.809266（FIFO / DRR），饥饿事件 24 / 10。观察到 2 次切换、2 次主路错误和 3 次备份尝试。 |
| 脱敏和耗时 | 6/6 个正例命中，3 个良性样本中 0 个误报。61398 字符的高密度输入用时 6.3946 ms，阈值为 50.0 ms。 |
| 总判定及失败分支 | `overall_pass = true`，13/13 项通过。缺输入时 10/13、`status=incomplete`、退出码 2。 |

## 验证环境与范围

Windows，Python 3.12.15，pytest 9.2.0，Node 24.14.0，npm 11.9.0，Vitest 4.1.12，Vite 6.4.6。Windows 默认执行沙箱的 asyncio self-pipe 卡在回环 `socket.accept`，启用本机回环后以进程内 TestClient 完整执行。pytest 保留一条来自 Starlette 的 httpx 弃用提示。前端已运行测试、类型检查和构建，历史截图经过检查，当前版本的交互和窄屏显示未做实机浏览器验收。

服务范围为可信本地单进程。SQLite 账本与 USD 预算跨重启保留，RPM/TPM 使用进程内状态。Provider 路由为模拟。API 无独立租户认证，发布验收以源码和本机工具为范围。未建立 Git 提交或发布对象。

## 实际命令与输出

下方保留命令输出和精确计数。机器私有绝对路径用 `<workspace>` 占位。未替换路径的原始日志保存在本地忽略目录 `.benchmarks/audit/`。

### Ruff

~~~powershell
.\.venv\Scripts\ruff.exe check backend\llm_gateway backend\scripts tests\backend benchmarks
~~~

退出码 `0`。

~~~text
All checks passed!
~~~

### 严格 mypy

~~~powershell
.\.venv\Scripts\mypy.exe --config-file backend\pyproject.toml backend\llm_gateway backend\scripts tests\backend benchmarks
~~~

退出码 `0`。

~~~text
Success: no issues found in 19 source files
~~~

### 后端测试

~~~powershell
.\.venv\Scripts\python.exe -m pytest tests\backend -v
~~~

退出码 `0`。

~~~text
============================= test session starts =============================
platform win32 -- Python 3.12.15, pytest-9.2.0, pluggy-1.7.0 -- <workspace>\.venv\Scripts\python.exe
cachedir: .pytest_cache
rootdir: <workspace>
configfile: pytest.ini
plugins: anyio-4.16.0, asyncio-1.5.0
asyncio: mode=Mode.AUTO, debug=False, asyncio_default_fixture_loop_scope=function, asyncio_default_test_loop_scope=function
collecting ... collected 29 items

tests/backend/test_api_and_openapi.py::test_full_api_contract_and_openapi_sync PASSED [  3%]
tests/backend/test_api_and_openapi.py::test_compare_replay_counts_only_observed_outcomes PASSED [  6%]
tests/backend/test_api_and_openapi.py::test_http_boundaries_and_safe_errors PASSED [ 10%]
tests/backend/test_api_and_openapi.py::test_gateway_paths_and_origins_are_validated PASSED [ 13%]
tests/backend/test_api_and_openapi.py::test_benchmark_endpoint_reports_exact_data_and_rejects_unreadable_reports PASSED [ 17%]
tests/backend/test_bug_repros_and_fixes.py::test_bug1_phantom_quota_burn_fixed PASSED [ 20%]
tests/backend/test_bug_repros_and_fixes.py::test_bug2_upstream_rejects_multi_quota_string PASSED [ 24%]
tests/backend/test_bug_repros_and_fixes.py::test_bug3_subsecond_monotonic_refill_and_oversized_fail_fast PASSED [ 27%]
tests/backend/test_bug_repros_and_fixes.py::test_bug4_cache_thrashing_eviction_shield PASSED [ 31%]
tests/backend/test_bug_repros_and_fixes.py::test_refund_stays_bounded_after_window_rollover PASSED [ 34%]
tests/backend/test_bug_repros_and_fixes.py::test_sliding_window_keeps_exact_integer_usage_at_fractional_boundary PASSED [ 37%]
tests/backend/test_bug_repros_and_fixes.py::test_full_store_keeps_live_entries_until_expired_sweep PASSED [ 41%]
tests/backend/test_fairness_failover_redaction.py::test_weighted_drr_prevents_vip_starvation_and_routes_429_failover PASSED [ 44%]
tests/backend/test_fairness_failover_redaction.py::test_drr_carries_capacity_and_resumes_weighted_visits PASSED [ 48%]
tests/backend/test_fairness_failover_redaction.py::test_drr_rejects_duplicate_tenant_queues PASSED [ 51%]
tests/backend/test_fairness_failover_redaction.py::test_no_fulfilled_requests_do_not_produce_a_perfect_jain_index PASSED [ 55%]
tests/backend/test_fairness_failover_redaction.py::test_provider_reopens_after_failed_probe_and_recovers_on_success PASSED [ 58%]
tests/backend/test_fairness_failover_redaction.py::test_redaction_mask_block_audit_and_linear_latency PASSED [ 62%]
tests/backend/test_quota_reserve_settle_wal.py::test_reserve_settle_refund_and_wal_persistence PASSED [ 65%]
tests/backend/test_quota_reserve_settle_wal.py::test_atomic_rollback_when_tpm_fails PASSED [ 68%]
tests/backend/test_quota_reserve_settle_wal.py::test_failed_reservation_write_and_oversize_leave_all_quota_available PASSED [ 72%]
tests/backend/test_quota_reserve_settle_wal.py::test_budget_denial_preserves_quota_and_spend_survives_a_restart PASSED [ 75%]
tests/backend/test_quota_reserve_settle_wal.py::test_idempotent_replays_are_tenant_scoped_and_keep_original_inputs PASSED [ 79%]
tests/backend/test_quota_reserve_settle_wal.py::test_concurrent_settlement_refunds_once_after_a_failed_write PASSED [ 82%]
tests/backend/test_quota_reserve_settle_wal.py::test_large_settlement_overrun_remains_charged_until_refill PASSED [ 86%]
tests/backend/test_quota_reserve_settle_wal.py::test_late_refund_does_not_credit_a_refilled_bucket PASSED [ 89%]
tests/backend/test_quota_reserve_settle_wal.py::test_legacy_ledger_migrates_keys_and_keeps_existing_reservations PASSED [ 93%]
tests/backend/test_upstream_compat.py::test_upstream_baseline_throttled_smoke PASSED [ 96%]
tests/backend/test_upstream_compat.py::test_enhanced_limiters_basic_and_concurrent_gcra PASSED [100%]

============================== warnings summary ===============================
.venv\Lib\site-packages\fastapi\testclient.py:1
  <workspace>\.venv\Lib\site-packages\fastapi\testclient.py:1: StarletteDeprecationWarning: Using `httpx` with `starlette.testclient` is deprecated; install `httpx2` instead.
    from starlette.testclient import TestClient as TestClient  # noqa

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
======================== 29 passed, 1 warning in 1.53s ========================
~~~

### 完整三层基准

~~~powershell
.\.venv\Scripts\python.exe benchmarks\run_sealed_benchmarks.py
~~~

退出码 `0`。

~~~text
[OK] Benchmark report written (status=passed, gates=13/13)
~~~

### 缺失外部输入时的基准

~~~powershell
$env:GATEWAY_SEALED_BENCHMARK_DIR = '.benchmarks/audit/nonexistent-input'
.\.venv\Scripts\python.exe benchmarks\run_sealed_benchmarks.py --output .benchmarks/audit/incomplete.json
~~~

退出码 `2`。

~~~text
[INCOMPLETE] Benchmark report written (status=incomplete, gates=10/13)
[INCOMPLETE] tier_1a_azure_traces: AzureLLMInferenceTrace_code.csv: unavailable
[INCOMPLETE] tier_1b_burstgpt_50k: BurstGPT_1.csv: unavailable
[FAIL] azure_trace_coverage_and_accounting
[FAIL] burstgpt_trace_coverage_and_accounting
[FAIL] reserve_refund_tpm_reclaimed
~~~

### 前端测试

~~~powershell
cd frontend
npm test
~~~

退出码 `0`。

~~~text
> llm-quota-throttle-gateway-ui@0.1.0 test
> vitest --run


 RUN  v4.1.12 <workspace>/frontend


 Test Files  1 passed (1)
      Tests  8 passed (8)
   Start at  13:13:27
   Duration  422ms (transform 60ms, setup 0ms, import 83ms, tests 40ms, environment 0ms)
~~~

### TypeScript 和前端生产构建

~~~powershell
cd frontend
npm run build
~~~

退出码 `0`。

~~~text
> llm-quota-throttle-gateway-ui@0.1.0 build
> tsc --noEmit && vite build

vite v6.4.6 building for production...
transforming...
✓ 1580 modules transformed.
rendering chunks...
computing gzip size...
dist/index.html                   0.48 kB │ gzip:  0.31 kB
dist/assets/index-g8auRoqc.css    5.58 kB │ gzip:  1.80 kB
dist/assets/index-B7OMPpks.js   256.57 kB │ gzip: 78.16 kB
✓ built in 2.27s
~~~

### 官方 npm 安全公告复核

~~~powershell
cd frontend
npm audit --registry=https://registry.npmjs.org --json
~~~

退出码 `0`。

~~~text
{
  "auditReportVersion": 2,
  "vulnerabilities": {},
  "metadata": {
    "vulnerabilities": {
      "info": 0,
      "low": 0,
      "moderate": 0,
      "high": 0,
      "critical": 0,
      "total": 0
    },
    "dependencies": {
      "prod": 5,
      "dev": 144,
      "optional": 53,
      "peer": 0,
      "peerOptional": 0,
      "total": 148
    }
  }
}
~~~

### 官方源干净安装

~~~powershell
npm ci --ignore-scripts --no-audit --fund=false --registry=https://registry.npmjs.org
~~~

退出码 `0`。

~~~text
added 98 packages in 9s
~~~

### 导出 OpenAPI

~~~powershell
.\.venv\Scripts\python.exe backend\scripts\export_openapi.py
~~~

退出码 `0`。

~~~text
[OK] Exported OpenAPI schema -> backend/docs/openapi.json
~~~

### 离线构建 wheel

~~~powershell
uv build --offline --wheel --out-dir .benchmarks/audit/wheel backend
~~~

退出码 `0`。

~~~text
Building wheel...
Successfully built .benchmarks\audit\wheel\llm_quota_throttle_gateway-0.1.0-py3-none-any.whl
~~~

### 完整 OpenAPI、wheel 导入与 README 比对

本地 Python 对解包后实际导入的上游进行准入冒烟测试，检查 wheel 许可表达式，再比较 OpenAPI 全文。独立读取两份 README 指标区和新 JSON，逐项比较值。

~~~text
Installed-wheel smoke: vendored package import, upstream RPM admission, and FastAPI schema passed
Installed-wheel license expression: MIT AND BSD-3-Clause
OpenAPI full document equality: 11 paths, 12 operations
Bilingual README benchmark blocks: 21 identical rows, 50 values each match docs/test_metrics.json exactly
~~~

### 基线 Git blob 与发布候选检查

通过 GitHub Git Trees API 取得固定提交的 blob 清单。用 Git clean filter 计算拟发布文件的 blob SHA，用 SHA-256 复核工作树前后变化。使用 `git ls-files --cached --others --exclude-standard` 枚举发布候选。

~~~text
Vendored file coverage: 138 local / 138 remote
Exact Git blob matches: 15; CRLF-only matches: 123; other content differences: 0
Missing files: []
Unexpected files: []
Other differences: []
Vendored LICENSE byte-identical: False
Upstream attribution preserved: True
Pre-/post-audit worktree SHA-256: 139/139 unchanged (including UPSTREAM_META.json)
Git clean-filter blob hashes: 138/138 match the pinned upstream commit
Git clean-filter LICENSE byte equality: true
Git index entries: 0
Git-eligible release files: 190
Nonempty UTF-8 text files scanned: 169
PNG images inspected for text/EXIF chunks: 6
PNG files with text/EXIF chunks: 0
First-party marker hits (review candidates): 0
Vendored marker hits (review candidates): 4
backend/upstream_baseline/tests/asyncio/contrib/fastapi/test_limiter.py 159 drive_or_home_path
backend/upstream_baseline/tests/asyncio/contrib/fastapi/test_limiter.py 161 drive_or_home_path
backend/upstream_baseline/tests/contrib/flask/test_limiter.py 198 drive_or_home_path
backend/upstream_baseline/tests/contrib/flask/test_limiter.py 199 drive_or_home_path
Non-example address locations requiring review: 2
backend/upstream_baseline/pyproject.toml:10
backend/upstream_baseline/throttled/store/redis_pool.py:1
Temporary SQLite files under data/: 0
~~~

### 实装后端组件的公开公告

按本机实装版本查询 GitHub Advisory Database。

~~~text
fastapi@0.165.0: 0 GitHub advisories
starlette@1.10.0: 0 GitHub advisories
pydantic@2.14.4: 0 GitHub advisories
uvicorn@0.54.0: 0 GitHub advisories
h11@0.16.0: 0 GitHub advisories
~~~

## 最终验收与浏览器复核（Release Sign-Off）

验收结论：**Go（批准公开 GitHub 发布）**。

1. **全项独立复跑通过**：
   - `ruff check`：`All checks passed!`
   - `mypy --config-file backend/pyproject.toml`：`Success: no issues found in 19 source files`
   - `pytest tests/backend -v`：`29 passed`
   - `benchmarks/run_sealed_benchmarks.py`：`status=passed`，`13/13` 门禁通过（`overall_pass = true`）
   - `frontend`: `vitest --run` `8 passed`、`tsc --noEmit && vite build` 成功、`npm audit --registry=https://registry.npmjs.org --json` 为 `0` vulnerabilities（148 个依赖）。
2. **审计后新版前端真机浏览器复测通过**：
   - 在 `1440×900` 与 `375×812` 视口完成全部 5 个标签页真实交互，包括新建预留、`Settle… -> Confirm Settlement` 行内结算退还（`Refunded: 1200`）、DRR 公平模拟（`0.8596` vs FIFO `0.3333`）、OTLP/SSE 实时流、脱敏规则评估与 `Sealed Benchmarks` 面板。
   - 控制台错误 `0`、未捕获异常 `0`、HTTP `4xx/5xx` `0`、桌面与移动端零水平滚动溢出，并已更新 [docs/screenshots/](screenshots/01_compare_desktop.png) 下全部 6 张最新截图。
3. **上游开源合规项核实**：
   - `backend/upstream_baseline/` 中仅存的两条邮箱属于上游公开 MIT (`pyproject.toml`) 与 BSD-3-Clause (`redis_pool.py`) 法定署名/版权声明，并非第一方个人隐私泄露，依法原样保留。
   - 自研代码、文档、测试及截图第一方隐私与路径扫描为 `0` 命中，运行期 `data/` 已清空。
