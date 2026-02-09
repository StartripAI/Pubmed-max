# Pubmed-max

**Content-first PubMed-centric medical retrieval and evidence synthesis pipeline.**

The product goal is simple: **increase core medical evidence density with full traceability**.

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

## Core data status (example workbook snapshot, 2026-02-09)

| Task | Core non-empty values | Core possible values | Coverage |
|---|---:|---:|---:|
| Task 1 (Survival endpoints) | 64 | 128 | 50.00% |
| Task 2 (Tumor control endpoints) | 43 | 176 | 24.43% |
| Task 3 (Safety/QoL endpoints) | 56 | 112 | 50.00% |

## Delta vs previous release

| Metric | Delta |
|---|---:|
| Task 1 core non-empty values | 0 |
| Task 2 core non-empty values | 0 |
| Task 3 core non-empty values | 0 |

This means the latest release improved runtime robustness and publication quality, but **did not increase core data values**.

## What this repository includes

- `src/paper_hub.py` - unified retrieval, source routing, quality scoring, audit output
- `src/workbook_builder.py` - workbook generation with review-oriented formatting
- `src/dimensions_catalog.yaml` - dynamic dimension registry
- `src/source_registry.yaml` - source reliability and institution registry
- `reports/CORE_DATA_STATUS_2026-02-09.md` - content-first KPI snapshot
- `reports/COMPARISON_2026-02-09.md` - engine/path comparison
- `reports/GENERAL_BENCHMARK_2026-02-09.md` - general, non-case benchmark
- `reports/MARKET_POSITION_2026-02-09.md` - baseline-vs-product positioning with hard metrics
- `examples/` - workbook and audit examples

## Product principles

1. Content metrics are primary KPIs.
2. Every value must be traceable to evidence.
3. Quality gate should never be lowered for easier fill rates.
4. Reliability improvements are necessary, but not counted as core content progress.

## Current focus

- Increase Task 2 and Task 3 core coverage with high-grade evidence.
- Keep evidence mapping and auditability at 100%.
