# Datasets & Provenance Guide

## Sample workload

[`sample/smoke_workload.json`](sample/smoke_workload.json) contains four synthetic request records for manual smoke checks. To replay it through `POST /api/v1/compare`, pass its `events` array as `requests` along with the quota fields.

## External trace provenance

Put the publisher's original CSVs in a directory outside the repository and set `GATEWAY_SEALED_BENCHMARK_DIR` to that directory. The runner uses `../_sealed_benchmarks/llm-quota-throttle-gateway` when the environment and `.env` omit the setting. Relative paths resolve from the repository root.

| Publisher file | Rows replayed | Git blob ID | SHA-256 |
| --- | ---: | --- | --- |
| [Azure code](https://github.com/Azure/AzurePublicDataset/blob/master/data/AzureLLMInferenceTrace_code.csv) | 8,819 | `62bd8109d0f2b1bc48206d397ecca48d8de03aab` | `54e9a6d2a4bd06ba1e060304b900abbc74cbea53de96506e60fe5bb4f2277fb6` |
| [Azure conversation](https://github.com/Azure/AzurePublicDataset/blob/master/data/AzureLLMInferenceTrace_conv.csv) | 19,366 | `5b9219f2caf93bbbf95ec29894e206115658e235` | `2f1e5b666d4e3055fdbba98598ce2ec307767b9064e03e2fa46676dbcc7d0bf8` |
| [BurstGPT v1 workload](https://github.com/HPMLL/BurstGPT/blob/main/data/BurstGPT_1.csv) | First 50,000 | `f95dd2c84454b067fdd0973858c55ecfe31bdad7` | `46fc9480ef0b748ecb2b51d512ff08c196b031782cbe6f78e28044d768e86d5a` |

The runner checks the exact byte count, SHA-256, UTF-8 CSV header, replay row count and ordering before scoring a trace. It verifies the complete BurstGPT file before replaying the first 50,000 rows. Both publishers license these datasets under CC BY 4.0 and provide attribution information. [Azure dataset card](https://github.com/Azure/AzurePublicDataset/blob/master/AzureLLMInferenceDataset2023.md) · [Azure license](https://github.com/Azure/AzurePublicDataset/blob/master/LICENSE) · [BurstGPT dataset card](https://github.com/HPMLL/BurstGPT#schema) · [BurstGPT license](https://github.com/HPMLL/BurstGPT/blob/main/LICENSE)

## Replay and verification

- Tier 1A merges both Azure streams by source time at 100 ns precision. It assigns eight synthetic tenant buckets per stream and applies per-tenant limits of 600 RPM and 1,500,000 TPM.
- Tier 1B follows BurstGPT's second-resolution timestamps in file order, including equal timestamps and zero-response records. It assigns twelve synthetic tenant buckets per source model and applies per-tenant limits of 500 RPM and 1,200,000 TPM.
- Both replays use source timing at its original speed in a virtual clock without sleeping between events. Each request reserves prompt tokens plus a fixed 2,048-token completion budget, then settles using actual prompt and completion counts at the same virtual instant. The accounting check is `reserved - refunded + overrun = actual`. Throughput and latency are wall-clock measurements of the in-memory replay, including settlement. CSV loading, HTTP, and SQLite are outside those timings.
- Tier 2 exercises capacity, expiry, quota refunds, overrun debt, and provider failover with deterministic synthetic inputs. The FIFO and DRR comparison uses three synthetic tenants with weights 1, 4, and 3 and reports Jain's index separately from the external traces.
- Tier 3 checks six synthetic positive cases, three benign cases, the block policy, and exact match counts on two high-density inputs. Its 50 ms budget applies to the measured inputs on the test host.

Run the CPU-only suite from the repository root.

```powershell
.\.venv\Scripts\python.exe benchmarks\run_sealed_benchmarks.py
```

The command writes `docs/test_metrics.json` atomically. Use `--output` to choose another destination. Exit status `0` means every regression gate passed. Status `1` means at least one gate failed. Status `2` means required data or the output was unavailable. The report retains results from available tiers when an input fails validation.
