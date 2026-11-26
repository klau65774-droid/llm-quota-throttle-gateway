# AGENTS.md — Workspace Engineering & Audit Rules

This repository (`llm-quota-throttle-gateway`) is a deterministic, CPU-only LLM gateway traffic, dual RPM+TPM quota, weighted DRR fairness, and provider failover control plane built atop `ZhuoZhuoCrayon/throttled-py` (`v3.5.0`, MIT).

## Engineering Standards
1. **Verbatim Upstream Baseline**: Keep `backend/upstream_baseline/` 100% unmodified for baseline comparison and upstream compatibility tests. Preserve upstream copyright in `LICENSE`.
2. **Strict Contract Adherence**: Follow `docs/API_CONTRACT.md` and `backend/docs/openapi.json` strictly for routes, schemas, status codes, and No-False-Greens telemetry.
3. **Zero Hardcoded Secrets, PII, or Local Paths**: Never embed API keys, private key headers, phone numbers, real names, local proxy ports, or absolute drive paths inside tracked files. Use environment variables from `.env.example`.
4. **CPU-Only Deterministic Quality**: All backend checks (`ruff`, `mypy --strict`, `pytest`), frontend checks (`vitest`, `tsc`, `vite build`), and 3-tier benchmarks (`benchmarks/run_sealed_benchmarks.py`) must run 100% locally on CPU with zero paid external LLM APIs. Never hardcode benchmark datasets inside `backend/llm_gateway/`.
5. **Independent Audit Gate**: When running in audit mode, verify source code, contracts, test outputs, and benchmarks against actual implementation evidence and write findings to `docs/AUDIT_AND_TEST_REPORT.md`.
