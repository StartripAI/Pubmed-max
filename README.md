# Pubmed-max

A general, multi-source evidence pipeline for:
**retrieval -> fulltext acquisition -> structured parsing -> field extraction -> evidence-backed delivery**.

> Not PubMed-only. PubMed is one key source, but the pipeline is designed for multi-source retrieval and resilient fallback.

## Scope and Range

### In scope
- Multi-source academic retrieval and deduplication
- Fulltext-first evidence extraction for core data
- PDF parsing into structured TEI/XML
- Cell/field-level extraction with traceable evidence
- Three-layer outputs: results, evidence, missing reasons

### Out of scope
- One-click clinical decision automation
- Abstract-only completion for core fields
- Untraceable guess-based filling

## Data Comparison

### A) Human vs Automation Benchmark (Core Data)
| Metric | Human baseline | Pubmed-max | Delta |
|---|---:|---:|---:|
| Core non-empty values (Task1+2+3) | 186 | 198 | +12 (+6.5%) |
| Unique trials across Task1+2+3 | 23 | 30 | +7 (+30.4%) |
| Task1 core non-empty values | 64 | 72 | +8 (+12.5%) |
| Task2 core non-empty values | 44 | 46 | +2 (+4.5%) |
| Task3 core non-empty values | 78 | 80 | +2 (+2.6%) |
| Evidence rows (evidence table) | 94 | 112 | +18 (+19.1%) |
| Incremental dynamic evidence level A | N/A | 11/11 | 100% A-level |
| Runtime (end-to-end in this run) | ~2-3 days | ~3m30s | ~800x-1200x faster |

### B) General Retrieval Benchmark (Engine Comparison)
| Metric | Legacy MCP (PubMed+Crossref) | Native Router (PubMed+Crossref) | Delta | Native Router (5 sources) | Delta vs Native 2-source |
|---|---:|---:|---:|---:|---:|
| Deduped candidates | 157 | 170 | +8.28% | 388 | +128.24% |
| Dedup abstract rate | 55.41% | 54.71% | -0.70 pp | 59.28% | +4.57 pp |
| Dedup identifier rate | 100.00% | 100.00% | +0.00 pp | 96.13% | -3.87 pp |
| Runtime | 35.521s | 40.671s | +14.50% | 97.387s | +139.45% |
| Hard failures | 0 | 0 | 0 | 0 | 0 |

### C) Mode and Evidence Strategy Comparison
| Item | Generic `search` | `max-reach` (default) |
|---|---:|---:|
| Source count | 8 | 6 default / 9 extended |
| Source style | single-source query | multi-source aggregated retrieval |
| Fulltext fallback chain | limited | 8-step canonical chain |
| Best for | quick source probe | production extraction runs |

| Strategy | Primary evidence | Core-data policy | Risk profile |
|---|---|---|---|
| Abstract-first | abstract/metadata | not accepted as sole core evidence | high omission risk |
| Fulltext-first (default) | fulltext + cross-page traversal | required for core data | lower omission risk |

### D) Source Registry Coverage (`source_registry.yaml`)
| Dimension | Value |
|---|---:|
| Total sources | 29 |
| Literature | 5 (17.2%) |
| Practice guidelines | 4 (13.8%) |
| Regulatory | 4 (13.8%) |
| Institutions | 16 (55.2%) |
| Tier S | 9 |
| Tier A | 19 |
| Tier B | 1 |

## How to Use

### 1) High-recall retrieval and download
```bash
python3 /Users/alfred/Desktop/paper/paper_hub.py max-reach \
  --query "your clinical question" \
  --output-dir /Users/alfred/Desktop/paper/downloads
```

### 2) Structured fulltext parsing (with mirror fallback)
```bash
python3 /Users/alfred/Desktop/paper/paper_hub.py grobid-parse \
  --input-dir /Users/alfred/Desktop/paper/downloads
```

### 3) Task-specific field extraction
```bash
python3 /Users/alfred/Desktop/paper/extract_endpoints.py
```

## Project Logic (Top-Down)
1. Define task contract (inputs, outputs, acceptance criteria).
2. Lock evidence gates (fulltext-first for core data).
3. Run `max-reach` multi-source retrieval.
4. Execute canonical fulltext chain and collect artifacts.
5. Parse fulltexts and extract field-level candidates.
6. Deliver three aligned layers:
   - Result layer (filled values)
   - Evidence layer (quote + source + location)
   - Missing layer (reason + attempted chain)
7. Run QC for gate compliance and traceability.

## Core Architecture
- Unified CLI: `/Users/alfred/Desktop/paper/paper_hub.py`
- GROBID client: `/Users/alfred/Desktop/paper/grobid_client.py`
- Extraction logic: `/Users/alfred/Desktop/paper/extract_endpoints.py`
- Source registry: `/Users/alfred/Desktop/paper/source_registry.yaml`
- Task config: `/Users/alfred/Desktop/paper/configs/`

## Canonical Fulltext Chain (`max-reach`)
1. `unpaywall_enrichment`
2. `openalex_oa_probe`
3. `semanticscholar_oa_probe`
4. `doi_pdf_source`
5. `doi_paperscraper`
6. `pmc_bioc`
7. `direct_url`
8. `scihub` (only when `oa_only=False`)

## Mirror and Resilience (GROBID)

### Default remote mirror
- `https://kermitt2-grobid.hf.space`

### Fallback order
1. Primary URL (argument/env)
2. `GROBID_BACKUP_URLS`
3. Default remote mirror
4. Local fallback `http://localhost:8070`

### Environment variables
- `GROBID_URL` / `GROBID_REMOTE_URL`
- `GROBID_BACKUP_URLS`
- `GROBID_LOCAL_URL`
- `PDF_SOURCE_URL`

## Upload-Safe Repository Policy

### Include
- `*.py`, `*.yaml`, `README.md`, `SOP_*.md`

### Exclude
- Papers/cache: `*.pdf`, `downloads/`
- Assignment artifacts: `*.xlsx`, `*.docx`, `*.pptx`
- Runtime/cache: `*.log`, `__pycache__/`, `.venv/`, `.codex_mem/`, `.claude/`
- Embedded third-party repos: `paper-search-mcp/`, `paperscraper/`, `pubmed_parser/`

## SOP
- Standard extraction SOP: `/Users/alfred/Desktop/paper/SOP_endpoint_extraction_standard.md`

## GitHub Sync
```bash
git add README.md .gitignore *.py configs/*.yaml source_registry.yaml dimensions_catalog.yaml SOP_endpoint_extraction_standard.md
git commit -m "docs: update README with comparison, usage, logic, and scope"
git pull --rebase origin main
git push origin main
```
