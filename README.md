# General Research + PubMed Pipeline

A production-oriented, multi-source research evidence pipeline for:
**search -> fulltext acquisition -> structured parsing -> field extraction -> evidence-backed output**.

## What This Project Is
- A backend workflow for research paper operations.
- Not user-facing product UI.
- Not PubMed-only: PubMed is one source among multiple retrieval and fallback channels.

## Scope

### In scope
- Multi-source academic retrieval and deduplication
- Fulltext-first extraction for core data
- PDF to TEI/XML parsing
- Cell/field-level extraction with evidence mapping
- Structured missing-reason reporting for unfilled targets

### Out of scope
- Abstract-only completion for core fields
- Untraceable guess-based filling
- Clinical decision automation

## Current Source Coverage

### Registry coverage (`source_registry.yaml`)
- Total sources: `29`
- Literature: `5`
- Practice guidelines: `4`
- Regulatory: `4`
- Institutions: `16`
- Tier S: `9`
- Tier A: `19`
- Tier B: `1`

### Retrieval coverage
- Generic `search` sources (8):
  - `arxiv`, `pubmed`, `biorxiv`, `medrxiv`, `google_scholar`, `iacr`, `semantic`, `crossref`
- `max-reach` default sources (6):
  - `pubmed`, `europe_pmc`, `openalex`, `crossref`, `medrxiv`, `biorxiv`
- `max-reach` extended sources (9):
  - `pubmed`, `europe_pmc`, `openalex`, `openaire`, `core`, `crossref`, `semantic`, `medrxiv`, `biorxiv`

## Execution Mode
- Default and canonical mode: `max-reach`
- Goal: maximize evidence recall with strict evidence traceability.

## Canonical Fulltext Chain (`max-reach`)
1. `unpaywall_enrichment`
2. `openalex_oa_probe`
3. `semanticscholar_oa_probe`
4. `doi_pdf_source`
5. `doi_paperscraper`
6. `pmc_bioc`
7. `direct_url`
8. `scihub` (only when `oa_only=False`)

## Fulltext Policy
- Core data is **fulltext-first**.
- Abstracts can assist context and missing-reason notes.
- Abstracts are not accepted as sole primary evidence for core fields.
- Per target unit, traversal must not stop at first hit.

## Pipeline Logic (Top-Down)
1. Define task contract: input/output schema and acceptance criteria.
2. Lock extraction gates: evidence policy and missing taxonomy.
3. Run multi-source retrieval in `max-reach` mode.
4. Download via canonical fulltext chain.
5. Parse fulltext artifacts.
6. Extract and normalize field-level candidates.
7. Produce aligned outputs:
   - Result layer
   - Evidence layer
   - Missing-reason layer
8. Run QC for gate compliance and traceability.

## Core Files
- CLI entrypoint: `/Users/alfred/Desktop/paper/paper_hub.py`
- GROBID client: `/Users/alfred/Desktop/paper/grobid_client.py`
- Extraction logic: `/Users/alfred/Desktop/paper/extract_endpoints.py`
- Source registry: `/Users/alfred/Desktop/paper/source_registry.yaml`
- Task configs: `/Users/alfred/Desktop/paper/configs/`
- SOP: `/Users/alfred/Desktop/paper/SOP_endpoint_extraction_standard.md`

## Mirror and Resilience (GROBID)

### Default remote mirror
- `https://kermitt2-grobid.hf.space`

### Fallback order
1. Primary URL (argument or env)
2. `GROBID_BACKUP_URLS`
3. Default remote mirror
4. Local fallback: `http://localhost:8070`

### Environment variables
- `GROBID_URL` / `GROBID_REMOTE_URL`
- `GROBID_BACKUP_URLS`
- `GROBID_LOCAL_URL`
- `PDF_SOURCE_URL`

## Quick Start

### 1) Retrieve and download
```bash
python3 /Users/alfred/Desktop/paper/paper_hub.py max-reach \
  --query "your research question" \
  --output-dir /Users/alfred/Desktop/paper/downloads
```

### 2) Parse fulltexts
```bash
python3 /Users/alfred/Desktop/paper/paper_hub.py grobid-parse \
  --input-dir /Users/alfred/Desktop/paper/downloads
```

### 3) Run extraction
```bash
python3 /Users/alfred/Desktop/paper/extract_endpoints.py
```

## Upload-Safe Repo Policy

### Include
- `*.py`, `*.yaml`, `README.md`, `SOP_*.md`

### Exclude
- `*.pdf`, `downloads/`
- `*.xlsx`, `*.docx`, `*.ppt`, `*.pptx`
- `*.log`, `__pycache__/`, `.venv/`, `.codex_mem/`, `.claude/`
- Embedded third-party repos: `paper-search-mcp/`, `paperscraper/`, `pubmed_parser/`
