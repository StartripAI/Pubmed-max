# Pubmed-max

**Content-first PubMed-centric medical retrieval and evidence synthesis pipeline.**

Mission: **build the world's most reliable, content-dense, and auditable medical retrieval workflow**.

The product goal is simple: **increase core medical evidence density with full traceability**.

## Human vs Automation Benchmark (core-data first)

This benchmark compares:

- **Human baseline**: one physician-authored extraction baseline (MD + PhD profile), manually curated over ~2-3 days.
- **Pubmed-max run**: same evidence goal, automated retrieval + download + extraction + workbook build.

| Metric | Human baseline | Pubmed-max | Delta |
|---|---:|---:|---:|
| Core non-empty values (Task1+2+3) | 186 | 198 | **+12 (+6.5%)** |
| Unique trials across Task1+2+3 | 23 | 30 | **+7 (+30.4%)** |
| Task1 core non-empty values | 64 | 72 | **+8 (+12.5%)** |
| Task2 core non-empty values | 44 | 46 | **+2 (+4.5%)** |
| Task3 core non-empty values | 78 | 80 | **+2 (+2.6%)** |
| Evidence rows (bottom evidence table) | 94 | 112 | **+18 (+19.1%)** |
| Incremental dynamic evidence level A | N/A | 11/11 | **100% A-level** |
| Runtime (end-to-end in this run) | ~2-3 days | **~3m30s** | **~800x-1200x faster** |

Notes:

- The speed benchmark used a measured run: `download-batch` (66 candidates, 14 full-text success) + workbook build.
- Quality gate thresholds were not relaxed; traceability remained strict.
- This is a real extraction benchmark, not a synthetic retrieval-only benchmark.

Detailed report: `reports/HUMAN_VS_AUTOMATION_BENCHMARK_2026-02-09.md`

## General benchmark (not case-specific)

This benchmark uses broad medical trial queries across oncology, cardiology, stroke, and endocrinology.

| Metric | Legacy MCP (PubMed+Crossref) | Native Router (PubMed+Crossref) | Delta | Native Router (5 sources) | Delta vs Native 2-source |
|---|---:|---:|---:|---:|---:|
| Deduped candidates | 157 | 170 | +8.28% | 388 | +128.24% |
| Dedup abstract rate | 55.41% | 54.71% | -0.70 pp | 59.28% | +4.57 pp |
| Dedup identifier rate | 100.00% | 100.00% | +0.00 pp | 96.13% | -3.87 pp |
| Runtime | 35.521s | 40.671s | +14.50% | 97.387s | +139.45% |
| Hard failures | 0 | 0 | 0 | 0 | 0 |

Semantic stress check (public rate limit conditions):

| Metric | Legacy MCP | Native Router | Delta |
|---|---:|---:|---:|
| Returned rows | 28 | 30 | +7.14% |
| Runtime | 21.566s | 15.561s | -27.84% |

Detailed report: `reports/GENERAL_BENCHMARK_2026-02-09.md`

## Release KPI status (general)

| KPI | Value |
|---|---:|
| Candidate growth, same source budget | +8.28% |
| Candidate growth, expanded OA source routing | +128.24% |
| Abstract coverage (expanded routing) | 59.28% |
| Identifier coverage (expanded routing) | 96.13% |
| Hard failures in benchmark run | 0 |
| Semantic stress runtime delta | -27.84% |

## What this repository includes

- `src/paper_hub.py` - unified retrieval, source routing, quality scoring, audit output
- `src/workbook_builder.py` - workbook generation with review-oriented formatting
- `src/dimensions_catalog.yaml` - dynamic dimension registry
- `src/source_registry.yaml` - source reliability and institution registry
- `reports/CORE_DATA_STATUS_2026-02-09.md` - content-first KPI snapshot
- `reports/HUMAN_VS_AUTOMATION_BENCHMARK_2026-02-09.md` - manual baseline vs automation benchmark
- `reports/COMPARISON_2026-02-09.md` - engine/path comparison
- `reports/GENERAL_BENCHMARK_2026-02-09.md` - general, non-case benchmark
- `reports/MARKET_POSITION_2026-02-09.md` - baseline-vs-product positioning with hard metrics
- `examples/` - output artifact examples

## Product principles

1. Content metrics are primary KPIs.
2. Every value must be traceable to evidence.
3. Quality gate should never be lowered for easier fill rates.
4. Reliability improvements are necessary, but not counted as core content progress.

## Current focus

- Increase high-grade endpoint extraction yield across medical topics.
- Keep evidence mapping and auditability at 100%.
