#!/usr/bin/env python3
"""Unified CLI wrapper for pubmed_parser, paper-search-mcp and paperscraper.

This entrypoint keeps each upstream project in its own virtual environment,
then delegates execution to the corresponding interpreter.

Enhancements in this version:
- robust PubMed adapter via paperscraper (instead of fragile parser-only path)
- search-multi for multi-query multi-source recall-oriented retrieval
- download-batch with DOI/PMCID/URL fallback chain
- legal-max pipeline for OA-first retrieval + abstract backfill + access audit
- parse bioc mode for BioC XML
- medline parse fix (generator -> list)
- retry and error classification helpers
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import re
import ssl
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET
import yaml

ROOT = Path(__file__).resolve().parent
DEFAULT_DIMENSIONS_CATALOG_PATH = ROOT / "dimensions_catalog.yaml"
DEFAULT_SOURCE_REGISTRY_PATH = ROOT / "source_registry.yaml"

PROJECT_DIRS = {
    "pubmed_parser": ROOT / "pubmed_parser",
    "paper_search_mcp": ROOT / "paper-search-mcp",
    "paperscraper": ROOT / "paperscraper",
}

SEARCH_SOURCES = (
    "arxiv",
    "pubmed",
    "biorxiv",
    "medrxiv",
    "google_scholar",
    "iacr",
    "semantic",
    "crossref",
)

DOWNLOAD_SOURCES = SEARCH_SOURCES

SOURCE_DEFAULTS = ("pubmed", "crossref", "semantic", "google_scholar")
LEGAL_MAX_DEFAULT_SOURCES = (
    "pubmed",
    "europe_pmc",
    "openalex",
    "crossref",
    "medrxiv",
    "biorxiv",
)

LEGAL_MAX_SOURCES = (
    "pubmed",
    "europe_pmc",
    "openalex",
    "openaire",
    "core",
    "crossref",
    "semantic",
    "google_scholar",
    "medrxiv",
    "biorxiv",
)

QUERY_PACKS = {
    "trial": [
        "randomized",
        "randomised",
        "phase III",
        "phase II",
        "clinical trial",
    ],
    "survival": ["overall survival", "OS", "progression-free survival", "PFS"],
    "tumor_control": ["objective response", "ORR", "DCR", "R0", "resection rate", "pCR"],
    "safety": ["adverse event", "CTCAE", "grade 3", "grade 4", "treatment-related death"],
    "qol": ["quality of life", "QOL", "EQ-5D", "QLQ-C30", "pain"],
    "qaly": ["QALY", "quality-adjusted", "QALM", "cost-effectiveness"],
}

DIMENSION_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "os_median": {
        "name": "Median Overall Survival",
        "category": "survival",
        "unit": "months",
        "definition_source": "RECIST/oncology-trial standard endpoint definitions",
        "allowed_tasks": ["task1"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "core",
    },
    "os_hr_ci": {
        "name": "Overall Survival Hazard Ratio with CI",
        "category": "survival",
        "unit": "HR(95%CI)",
        "definition_source": "CONSORT/clinical trial reporting convention",
        "allowed_tasks": ["task1"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "candidate",
    },
    "pfs_median": {
        "name": "Median Progression-Free Survival",
        "category": "survival",
        "unit": "months",
        "definition_source": "RECIST/oncology-trial standard endpoint definitions",
        "allowed_tasks": ["task1"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "core",
    },
    "pfs_hr_ci": {
        "name": "Progression-Free Survival Hazard Ratio with CI",
        "category": "survival",
        "unit": "HR(95%CI)",
        "definition_source": "CONSORT/clinical trial reporting convention",
        "allowed_tasks": ["task1"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "candidate",
    },
    "orr": {
        "name": "Objective Response Rate",
        "category": "tumor_control",
        "unit": "%",
        "definition_source": "RECIST objective response definition",
        "allowed_tasks": ["task2"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "core",
    },
    "bicr_orr": {
        "name": "BICR Objective Response Rate",
        "category": "tumor_control",
        "unit": "%",
        "definition_source": "RECIST independent central review standard",
        "allowed_tasks": ["task2"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "candidate",
    },
    "dcr": {
        "name": "Disease Control Rate",
        "category": "tumor_control",
        "unit": "%",
        "definition_source": "RECIST disease-control endpoint convention",
        "allowed_tasks": ["task2"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "core",
    },
    "r0_resection_rate": {
        "name": "R0 Resection Rate",
        "category": "tumor_control",
        "unit": "%",
        "definition_source": "Surgical oncology margin-negative resection standard",
        "allowed_tasks": ["task2"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "candidate",
    },
    "pcr_rate": {
        "name": "Pathologic Complete Response Rate",
        "category": "tumor_control",
        "unit": "%",
        "definition_source": "Pathology response reporting standard",
        "allowed_tasks": ["task2"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "candidate",
    },
    "ca199_response_rate": {
        "name": "CA19-9 Response Rate",
        "category": "tumor_control",
        "unit": "%",
        "definition_source": "Pancreatic cancer biomarker response convention",
        "allowed_tasks": ["task2"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "candidate",
    },
    "ae_grade3plus": {
        "name": "Grade >=3 Adverse Event Rate",
        "category": "safety_qol",
        "unit": "%",
        "definition_source": "CTCAE grade >=3 toxicity convention",
        "allowed_tasks": ["task3"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "core",
    },
    "sae_rate": {
        "name": "Serious Adverse Event Rate",
        "category": "safety_qol",
        "unit": "%",
        "definition_source": "Serious adverse event reporting convention",
        "allowed_tasks": ["task3"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "candidate",
    },
    "trd_rate": {
        "name": "Treatment-Related Death Rate",
        "category": "safety_qol",
        "unit": "%",
        "definition_source": "Trial safety mortality reporting standard",
        "allowed_tasks": ["task3"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "core",
    },
    "ae_discontinuation_rate": {
        "name": "AE-driven Treatment Discontinuation Rate",
        "category": "safety_qol",
        "unit": "%",
        "definition_source": "Treatment exposure and tolerability reporting standard",
        "allowed_tasks": ["task3"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "candidate",
    },
    "ae_dose_reduction_rate": {
        "name": "AE-driven Dose Reduction Rate",
        "category": "safety_qol",
        "unit": "%",
        "definition_source": "Dose intensity and safety reporting convention",
        "allowed_tasks": ["task3"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "candidate",
    },
    "qol_score": {
        "name": "Quality of Life Composite Result",
        "category": "safety_qol",
        "unit": "score/text",
        "definition_source": "EORTC/FACT/PRO reporting convention",
        "allowed_tasks": ["task3"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "core",
    },
    "tudd": {
        "name": "Time Until Definitive Deterioration",
        "category": "safety_qol",
        "unit": "time",
        "definition_source": "PRO deterioration-time endpoint convention",
        "allowed_tasks": ["task3"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "candidate",
    },
    "qaly": {
        "name": "Quality Adjusted Life Year",
        "category": "safety_qol",
        "unit": "QALY",
        "definition_source": "Health technology assessment methodology",
        "allowed_tasks": ["task3"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "core",
    },
    "icer": {
        "name": "Incremental Cost-Effectiveness Ratio",
        "category": "safety_qol",
        "unit": "cost/QALY",
        "definition_source": "Health technology assessment methodology",
        "allowed_tasks": ["task3"],
        "promotion_rule": ">=2 independent source types and >=1 tier S/A source",
        "default_status": "candidate",
    },
}

SOURCE_REGISTRY_DEFAULT: List[Dict[str, Any]] = [
    {"source_id": "pubmed", "source_type": "literature", "tier": "S", "country": "US", "institution_tier": "", "reliability_rule": "indexed_biomedical_reference", "alias_keywords": []},
    {"source_id": "europe_pmc", "source_type": "literature", "tier": "S", "country": "EU", "institution_tier": "", "reliability_rule": "indexed_biomedical_reference", "alias_keywords": []},
    {"source_id": "pmc", "source_type": "literature", "tier": "S", "country": "US", "institution_tier": "", "reliability_rule": "fulltext_biomedical_archive", "alias_keywords": ["pmc", "pubmed central"]},
    {"source_id": "openalex", "source_type": "literature", "tier": "A", "country": "INTL", "institution_tier": "", "reliability_rule": "cross-indexed_scholarly_metadata", "alias_keywords": []},
    {"source_id": "crossref", "source_type": "literature", "tier": "A", "country": "INTL", "institution_tier": "", "reliability_rule": "doi_metadata_registry", "alias_keywords": []},
    {"source_id": "nccn", "source_type": "practice", "tier": "S", "country": "US", "institution_tier": "", "reliability_rule": "major_clinical_guideline", "alias_keywords": ["nccn"]},
    {"source_id": "esmo", "source_type": "practice", "tier": "S", "country": "EU", "institution_tier": "", "reliability_rule": "major_clinical_guideline", "alias_keywords": ["esmo"]},
    {"source_id": "asco", "source_type": "practice", "tier": "S", "country": "US", "institution_tier": "", "reliability_rule": "major_clinical_guideline", "alias_keywords": ["asco"]},
    {"source_id": "csco", "source_type": "practice", "tier": "A", "country": "CN", "institution_tier": "", "reliability_rule": "major_clinical_guideline", "alias_keywords": ["csco"]},
    {"source_id": "fda", "source_type": "regulatory", "tier": "S", "country": "US", "institution_tier": "", "reliability_rule": "regulatory_review_source", "alias_keywords": ["fda"]},
    {"source_id": "ema", "source_type": "regulatory", "tier": "S", "country": "EU", "institution_tier": "", "reliability_rule": "regulatory_review_source", "alias_keywords": ["ema"]},
    {"source_id": "pmda", "source_type": "regulatory", "tier": "S", "country": "JP", "institution_tier": "", "reliability_rule": "regulatory_review_source", "alias_keywords": ["pmda"]},
    {"source_id": "clinicaltrials_gov", "source_type": "regulatory", "tier": "A", "country": "US", "institution_tier": "", "reliability_rule": "trial_registry_result_source", "alias_keywords": ["clinicaltrials.gov", "nct"]},
    {"source_id": "mayo_clinic", "source_type": "institution", "tier": "A", "country": "US", "institution_tier": "top", "reliability_rule": "top_cancer_center", "alias_keywords": ["mayo clinic"]},
    {"source_id": "md_anderson", "source_type": "institution", "tier": "A", "country": "US", "institution_tier": "top", "reliability_rule": "top_cancer_center", "alias_keywords": ["md anderson"]},
    {"source_id": "cleveland_clinic", "source_type": "institution", "tier": "A", "country": "US", "institution_tier": "top", "reliability_rule": "top_cancer_center", "alias_keywords": ["cleveland clinic"]},
    {"source_id": "msk", "source_type": "institution", "tier": "A", "country": "US", "institution_tier": "top", "reliability_rule": "top_cancer_center", "alias_keywords": ["memorial sloan", "msk"]},
    {"source_id": "dana_farber", "source_type": "institution", "tier": "A", "country": "US", "institution_tier": "top", "reliability_rule": "top_cancer_center", "alias_keywords": ["dana-farber", "dana farber"]},
    {"source_id": "sheba", "source_type": "institution", "tier": "A", "country": "IL", "institution_tier": "top", "reliability_rule": "top_tertiary_center", "alias_keywords": ["sheba"]},
    {"source_id": "rambam", "source_type": "institution", "tier": "A", "country": "IL", "institution_tier": "top", "reliability_rule": "top_tertiary_center", "alias_keywords": ["rambam"]},
    {"source_id": "assuta", "source_type": "institution", "tier": "B", "country": "IL", "institution_tier": "high", "reliability_rule": "specialty_hospital", "alias_keywords": ["assuta"]},
    {"source_id": "karolinska", "source_type": "institution", "tier": "A", "country": "SE", "institution_tier": "top", "reliability_rule": "top_university_hospital", "alias_keywords": ["karolinska"]},
    {"source_id": "uclh", "source_type": "institution", "tier": "A", "country": "UK", "institution_tier": "top", "reliability_rule": "top_university_hospital", "alias_keywords": ["uclh", "university college london hospitals"]},
    {"source_id": "gustave_roussy", "source_type": "institution", "tier": "A", "country": "FR", "institution_tier": "top", "reliability_rule": "top_cancer_center", "alias_keywords": ["gustave roussy"]},
    {"source_id": "pumch", "source_type": "institution", "tier": "A", "country": "CN", "institution_tier": "top", "reliability_rule": "top_national_center", "alias_keywords": ["协和", "peking union medical college hospital", "pumch"]},
    {"source_id": "west_china", "source_type": "institution", "tier": "A", "country": "CN", "institution_tier": "top", "reliability_rule": "top_national_center", "alias_keywords": ["华西", "west china hospital"]},
    {"source_id": "sjtu", "source_type": "institution", "tier": "A", "country": "CN", "institution_tier": "top", "reliability_rule": "top_academic_center", "alias_keywords": ["上海交通大学", "shanghai jiao tong"]},
    {"source_id": "fudan", "source_type": "institution", "tier": "A", "country": "CN", "institution_tier": "top", "reliability_rule": "top_academic_center", "alias_keywords": ["复旦", "fudan"]},
    {"source_id": "sun_yat_sen", "source_type": "institution", "tier": "A", "country": "CN", "institution_tier": "top", "reliability_rule": "top_academic_center", "alias_keywords": ["中山", "sun yat-sen", "sysu"]},
]

SOURCE_CRED_BASE = {
    "pubmed": 18,
    "europe_pmc": 17,
    "openalex": 14,
    "crossref": 12,
    "semantic": 11,
    "google_scholar": 9,
    "core": 10,
    "openaire": 9,
    "medrxiv": 6,
    "biorxiv": 6,
    "arxiv": 5,
}

JOURNAL_TIER_A_KEYWORDS = (
    "lancet",
    "new england journal of medicine",
    "nejm",
    "jama",
    "journal of clinical oncology",
    "jco",
    "annals of oncology",
    "nature medicine",
    "bmj",
)

JOURNAL_TIER_B_KEYWORDS = (
    "clinical cancer research",
    "esmo open",
    "oncology",
    "cancer",
    "gastroenterology",
    "annals of surgery",
    "pancreatology",
    "pharmacoeconomics",
)

HIGH_INSTITUTION_KEYWORDS = (
    "harvard",
    "stanford",
    "oxford",
    "cambridge",
    "memorial sloan",
    "mayo clinic",
    "johns hopkins",
    "md anderson",
    "nih",
    "nci",
    "fudan",
    "peking",
    "tsinghua",
    "karolinska",
)

DESIGN_RANDOMIZED_PATTERNS = (
    r"\brandomized\b",
    r"\brandomised\b",
    r"\bphase\s*iii\b",
    r"\bphase\s*3\b",
    r"\bmulticenter\b",
    r"\bmulticentre\b",
)

PMC_IDCONV_URL = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
PMC_BIOC_URL = "https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_xml/{pmcid}/unicode"
EUROPE_PMC_SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
OPENALEX_WORKS_URL = "https://api.openalex.org/works"
OPENAIRE_SEARCH_URL = "https://api.openaire.eu/search/publications"
CORE_SEARCH_URL = "https://api.core.ac.uk/v3/search/works"
UNPAYWALL_URL = "https://api.unpaywall.org/v2/{doi}"
CROSSREF_WORKS_URL = "https://api.crossref.org/works"
SEMANTIC_GRAPH_SEARCH_URL = "https://api.semanticscholar.org/graph/v1/paper/search"

TRANSIENT_ERROR_HINTS = (
    "429",
    "rate limit",
    "timed out",
    "timeout",
    "Temporary failure",
    "Connection reset",
    "502",
    "503",
    "504",
    "ParseError",
    "not well-formed",
)

PMC_CACHE: Dict[str, str] = {}

try:
    import certifi  # type: ignore

    _SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except Exception:
    _SSL_CONTEXT = ssl.create_default_context()


# ---------------------------------------------------------------------------
# process helpers
# ---------------------------------------------------------------------------

def _project_python(project_key: str) -> Path:
    project_dir = PROJECT_DIRS[project_key]
    python_path = project_dir / ".venv" / "bin" / "python"
    if not python_path.exists():
        raise RuntimeError(
            f"Missing virtualenv interpreter: {python_path}\n"
            f"Install deps in {project_dir} first."
        )
    return python_path


def _run_python(
    project_key: str,
    code: str,
    args: List[str],
    cwd: Path | None = None,
    timeout: int | None = None,
) -> subprocess.CompletedProcess[str]:
    if cwd is None:
        cwd = PROJECT_DIRS[project_key]
    cmd = [str(_project_python(project_key)), "-c", code, *args]
    return subprocess.run(cmd, cwd=str(cwd), text=True, capture_output=True, timeout=timeout)


def _is_transient_error(text: str) -> bool:
    low = text.lower()
    return any(h.lower() in low for h in TRANSIENT_ERROR_HINTS)


def _run_python_retry(
    project_key: str,
    code: str,
    args: List[str],
    context: str,
    retries: int = 2,
    backoff_seconds: float = 1.2,
    timeout: int | None = None,
) -> subprocess.CompletedProcess[str]:
    attempts = max(1, retries + 1)
    last = None

    for i in range(attempts):
        result = _run_python(project_key, code, args, timeout=timeout)
        combined = f"{result.stderr or ''}\n{result.stdout or ''}"
        if result.returncode == 0:
            return result
        last = result
        if i < attempts - 1 and _is_transient_error(combined):
            time.sleep(backoff_seconds * (2 ** i))
            continue
        return result

    if last is None:
        raise RuntimeError(f"{context} failed before execution")
    return last


def _die(message: str, details: str | None = None, exit_code: int = 1) -> None:
    print(message, file=sys.stderr)
    if details:
        print(details.rstrip(), file=sys.stderr)
    raise SystemExit(exit_code)


def _print_json(data: Any) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2, default=str))


def _extract_json_or_die(result: subprocess.CompletedProcess[str], context: str) -> Any:
    if result.returncode != 0:
        _die(
            f"{context} failed with exit code {result.returncode}",
            result.stderr or result.stdout,
        )

    payload = (result.stdout or "").strip()
    if result.stderr:
        print(result.stderr.rstrip(), file=sys.stderr)

    if not payload:
        return None

    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        for line in reversed(payload.splitlines()):
            candidate = line.strip()
            if not candidate:
                continue
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                continue
        _die(f"{context} produced non-JSON output", payload)


def _extract_json_safe(result: subprocess.CompletedProcess[str]) -> Tuple[Any, Optional[str]]:
    payload = (result.stdout or "").strip()
    if result.returncode != 0:
        return None, (result.stderr or result.stdout or "process_failed").strip()

    if not payload:
        return None, None

    try:
        return json.loads(payload), None
    except json.JSONDecodeError:
        for line in reversed(payload.splitlines()):
            candidate = line.strip()
            if not candidate:
                continue
            try:
                return json.loads(candidate), None
            except json.JSONDecodeError:
                continue
        return None, payload


# ---------------------------------------------------------------------------
# generic network/data helpers
# ---------------------------------------------------------------------------

def _http_get_json(url: str, timeout: int = 30) -> Dict[str, Any]:
    payload = _http_get(url, timeout=timeout)
    data = json.loads(payload.decode("utf-8", errors="ignore"))
    if not isinstance(data, dict):
        return {}
    return data


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_doi(doi: str) -> str:
    d = str(doi or "").strip()
    if not d:
        return ""
    d = re.sub(r"^https?://(dx\.)?doi\.org/", "", d, flags=re.IGNORECASE)
    return d.strip()


def _extract_pmid_from_url(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"(\d+)", url)
    return m.group(1) if m else ""


def _extract_pmcid_from_url(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"(PMC\d+)", url, flags=re.IGNORECASE)
    if not m:
        return ""
    return m.group(1).upper()


def _to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    try:
        return int(float(text))
    except Exception:
        return default


def _openalex_abstract_from_index(index: Any) -> str:
    if not isinstance(index, dict) or not index:
        return ""
    pos_to_word: Dict[int, str] = {}
    for word, positions in index.items():
        if not isinstance(positions, list):
            continue
        for p in positions:
            if isinstance(p, int):
                pos_to_word[p] = str(word)
    if not pos_to_word:
        return ""
    max_pos = max(pos_to_word.keys())
    words = [pos_to_word.get(i, "") for i in range(max_pos + 1)]
    return _clean_text(" ".join(w for w in words if w))

# ---------------------------------------------------------------------------
# search adapters
# ---------------------------------------------------------------------------

def _normalize_pubmed_date(value: str | None) -> str:
    if not value:
        return ""
    v = value.strip()
    if not v:
        return ""
    if re.fullmatch(r"\d{4}", v):
        return f"{v}/01/01"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        return v.replace("-", "/")
    if re.fullmatch(r"\d{4}/\d{2}/\d{2}", v):
        return v
    return ""


def _apply_pubmed_date_filter(query: str, date_from: str | None, date_to: str | None) -> str:
    start = _normalize_pubmed_date(date_from)
    end = _normalize_pubmed_date(date_to)
    if not start and not end:
        return query
    if not start:
        start = "1900/01/01"
    if not end:
        end = datetime.utcnow().strftime("%Y/%m/%d")
    return f"({query}) AND (\"{start}\"[Date - Publication] : \"{end}\"[Date - Publication])"


def _search_pubmed_adapter(
    query: str,
    max_results: int,
    retstart: int = 0,
    date_from: str | None = None,
    date_to: str | None = None,
    retries: int = 2,
) -> List[Dict[str, Any]]:
    filtered_query = _apply_pubmed_date_filter(query, date_from, date_to)

    code = r'''
import json
import logging
import sys

logging.disable(logging.CRITICAL)

from paperscraper.pubmed.pubmed import get_pubmed_papers

query = sys.argv[1]
max_results = int(sys.argv[2])
retstart = int(sys.argv[3])

fields = ["title", "authors", "date", "abstract", "journal", "doi", "pubmed_id"]
try:
    # pymed_paperscraper does not expose retstart directly; emulate pagination
    # by fetching a larger prefix then slicing locally.
    fetch_limit = max_results + max(retstart, 0)
    df = get_pubmed_papers(query, fields=fields, max_results=fetch_limit)
    if df is None:
        print("[]")
    else:
        df = df.fillna("")
        if retstart > 0:
            df = df.iloc[retstart : retstart + max_results]
        else:
            df = df.iloc[:max_results]
        print(json.dumps(df.to_dict(orient="records"), ensure_ascii=False, default=str))
except Exception as e:
    print(json.dumps({"_error": str(e)}, ensure_ascii=False))
'''

    result = _run_python_retry(
        "paperscraper",
        code,
        [filtered_query, str(max_results), str(retstart)],
        context="search(pubmed-adapter)",
        retries=retries,
        timeout=120,
    )
    data = _extract_json_or_die(result, "search(pubmed-adapter)")
    if isinstance(data, dict) and data.get("_error"):
        _die("search(pubmed-adapter) error", data["_error"])
    if not isinstance(data, list):
        return []
    return data


def _search_mcp_source(
    source: str,
    query: str,
    max_results: int,
    year: str | None = None,
    retries: int = 2,
) -> List[Dict[str, Any]]:
    code = r'''
import json
import sys

from paper_search_mcp.academic_platforms.arxiv import ArxivSearcher
from paper_search_mcp.academic_platforms.biorxiv import BioRxivSearcher
from paper_search_mcp.academic_platforms.crossref import CrossRefSearcher
from paper_search_mcp.academic_platforms.google_scholar import GoogleScholarSearcher
from paper_search_mcp.academic_platforms.iacr import IACRSearcher
from paper_search_mcp.academic_platforms.medrxiv import MedRxivSearcher
from paper_search_mcp.academic_platforms.pubmed import PubMedSearcher
from paper_search_mcp.academic_platforms.semantic import SemanticSearcher

source = sys.argv[1]
query = sys.argv[2]
max_results = int(sys.argv[3])
year = sys.argv[4] if len(sys.argv) > 4 else ""

searchers = {
    "arxiv": ArxivSearcher,
    "pubmed": PubMedSearcher,
    "biorxiv": BioRxivSearcher,
    "medrxiv": MedRxivSearcher,
    "google_scholar": GoogleScholarSearcher,
    "iacr": IACRSearcher,
    "semantic": SemanticSearcher,
    "crossref": CrossRefSearcher,
}

searcher = searchers[source]()
kwargs = {"max_results": max_results}
if source == "semantic" and year:
    kwargs["year"] = year
papers = searcher.search(query, **kwargs)
print(json.dumps([p.to_dict() for p in papers], ensure_ascii=False))
'''

    result = _run_python_retry(
        "paper_search_mcp",
        code,
        [source, query, str(max_results), year or ""],
        context=f"search({source})",
        retries=retries,
        timeout=120,
    )
    data = _extract_json_or_die(result, f"search({source})")
    if not isinstance(data, list):
        return []
    return data


def _coerce_year(value: str | None) -> Optional[int]:
    m = re.search(r"(19\d{2}|20\d{2})", str(value or ""))
    if not m:
        return None
    return int(m.group(1))


def _search_europe_pmc(
    query: str,
    max_results: int,
    date_from: str | None = None,
    date_to: str | None = None,
    retries: int = 2,
) -> List[Dict[str, Any]]:
    q = query.strip()
    y_from = _coerce_year(date_from)
    y_to = _coerce_year(date_to)
    if y_from and y_to:
        q = f"({q}) AND FIRST_PDATE:[{y_from} TO {y_to}]"
    elif y_from:
        q = f"({q}) AND FIRST_PDATE:[{y_from} TO *]"
    elif y_to:
        q = f"({q}) AND FIRST_PDATE:[1900 TO {y_to}]"

    params = {
        "query": q,
        "format": "json",
        "resultType": "core",
        "pageSize": max(1, min(max_results, 1000)),
    }
    url = f"{EUROPE_PMC_SEARCH_URL}?{urlencode(params)}"

    last_err = None
    for i in range(max(1, retries + 1)):
        try:
            data = _http_get_json(url, timeout=45)
            raw = data.get("resultList", {}).get("result", []) if isinstance(data, dict) else []
            if not isinstance(raw, list):
                return []
            out: List[Dict[str, Any]] = []
            for rec in raw:
                if not isinstance(rec, dict):
                    continue
                doi = _normalize_doi(str(rec.get("doi", "") or ""))
                pmid = str(rec.get("pmid", "") or "").strip()
                pmcid = str(rec.get("pmcid", "") or "").strip().upper()
                title = _clean_text(rec.get("title", ""))
                abstract = _clean_text(rec.get("abstractText", ""))
                year = _coerce_year(str(rec.get("pubYear", "") or ""))
                source_id = str(rec.get("id", "") or "").strip()
                source_db = str(rec.get("source", "MED") or "MED").strip()
                url = f"https://europepmc.org/article/{source_db}/{source_id}" if source_id else ""
                is_oa = str(rec.get("isOpenAccess", "") or "").upper() in {"Y", "TRUE", "1"}
                out.append(
                    {
                        "title": title,
                        "abstract": abstract,
                        "doi": doi,
                        "pmid": pmid,
                        "pmcid": pmcid,
                        "year": year,
                        "url": url,
                        "open_access": is_oa or bool(pmcid),
                        "journal": _clean_text(rec.get("journalTitle", "")),
                        "cited_by_count": _to_int(rec.get("citedByCount"), 0),
                        "institution_names": [],
                        "retracted_flag": str(rec.get("isRetracted", "") or "").upper() in {"Y", "TRUE", "1"},
                        "preprint_flag": source_db.upper() in {"PPR", "PPRR"},
                    }
                )
            return out
        except Exception as e:  # noqa: BLE001
            last_err = e
            if i < retries:
                time.sleep(1.2 * (2 ** i))
    if last_err:
        raise RuntimeError(f"europe_pmc_search_failed: {last_err}") from last_err
    return []


def _search_openalex(
    query: str,
    max_results: int,
    date_from: str | None = None,
    date_to: str | None = None,
    retries: int = 2,
) -> List[Dict[str, Any]]:
    per_page = max(1, min(max_results, 200))
    params: Dict[str, str] = {
        "search": query,
        "per-page": str(per_page),
    }
    filters: List[str] = []
    y_from = _coerce_year(date_from)
    y_to = _coerce_year(date_to)
    if y_from and y_to:
        filters.append(f"from_publication_date:{y_from}-01-01,to_publication_date:{y_to}-12-31")
    elif y_from:
        filters.append(f"from_publication_date:{y_from}-01-01")
    elif y_to:
        filters.append(f"to_publication_date:{y_to}-12-31")
    if filters:
        params["filter"] = ",".join(filters)

    url = f"{OPENALEX_WORKS_URL}?{urlencode(params)}"
    last_err = None
    for i in range(max(1, retries + 1)):
        try:
            data = _http_get_json(url, timeout=45)
            raw = data.get("results", []) if isinstance(data, dict) else []
            if not isinstance(raw, list):
                return []
            out: List[Dict[str, Any]] = []
            for rec in raw:
                if not isinstance(rec, dict):
                    continue
                ids = rec.get("ids", {}) if isinstance(rec.get("ids"), dict) else {}
                doi = _normalize_doi(str(ids.get("doi", "") or rec.get("doi", "") or ""))
                pmid = _extract_pmid_from_url(str(ids.get("pmid", "") or ""))
                pmcid = _extract_pmcid_from_url(str(ids.get("pmcid", "") or ""))
                title = _clean_text(rec.get("title", ""))
                abstract = _openalex_abstract_from_index(rec.get("abstract_inverted_index"))
                year = _coerce_year(str(rec.get("publication_year", "") or ""))
                loc = rec.get("primary_location", {}) if isinstance(rec.get("primary_location"), dict) else {}
                url = str(loc.get("landing_page_url", "") or rec.get("id", "") or "").strip()
                open_access = False
                oa_info = rec.get("open_access", {})
                if isinstance(oa_info, dict):
                    open_access = bool(oa_info.get("is_oa"))
                host_source = ""
                host = loc.get("source", {}) if isinstance(loc.get("source"), dict) else {}
                if isinstance(host, dict):
                    host_source = _clean_text(host.get("display_name", ""))
                inst_names: List[str] = []
                for auth in rec.get("authorships", []) if isinstance(rec.get("authorships"), list) else []:
                    if not isinstance(auth, dict):
                        continue
                    for inst in auth.get("institutions", []) if isinstance(auth.get("institutions"), list) else []:
                        if not isinstance(inst, dict):
                            continue
                        name = _clean_text(inst.get("display_name", ""))
                        if name:
                            inst_names.append(name)
                out.append(
                    {
                        "title": title,
                        "abstract": abstract,
                        "doi": doi,
                        "pmid": pmid,
                        "pmcid": pmcid,
                        "year": year,
                        "url": url,
                        "open_access": open_access or bool(pmcid),
                        "journal": host_source,
                        "cited_by_count": _to_int(rec.get("cited_by_count"), 0),
                        "institution_names": inst_names,
                        "retracted_flag": bool(rec.get("is_retracted")),
                        "preprint_flag": False,
                    }
                )
            return out
        except Exception as e:  # noqa: BLE001
            last_err = e
            if i < retries:
                time.sleep(1.2 * (2 ** i))
    if last_err:
        raise RuntimeError(f"openalex_search_failed: {last_err}") from last_err
    return []


def _year_in_range(year: Optional[int], date_from: str | None, date_to: str | None) -> bool:
    if year is None:
        return True
    y_from = _coerce_year(date_from)
    y_to = _coerce_year(date_to)
    if y_from and year < y_from:
        return False
    if y_to and year > y_to:
        return False
    return True


def _crossref_pick_year(item: Dict[str, Any]) -> Optional[int]:
    for key in ("published-print", "published-online", "issued", "created", "published"):
        node = item.get(key)
        if not isinstance(node, dict):
            continue
        parts = node.get("date-parts")
        if not isinstance(parts, list) or not parts:
            continue
        first = parts[0]
        if not isinstance(first, list) or not first:
            continue
        try:
            y = int(str(first[0]))
        except Exception:
            continue
        if 1800 <= y <= 2100:
            return y
    return None


def _search_crossref_native(
    query: str,
    max_results: int,
    date_from: str | None = None,
    date_to: str | None = None,
    retries: int = 2,
) -> List[Dict[str, Any]]:
    rows = max(1, min(max_results, 1000))
    params: Dict[str, str] = {
        "query.bibliographic": query,
        "rows": str(rows),
        "mailto": "paper-hub@example.org",
    }
    filters: List[str] = []
    y_from = _coerce_year(date_from)
    y_to = _coerce_year(date_to)
    if y_from:
        filters.append(f"from-pub-date:{y_from}-01-01")
    if y_to:
        filters.append(f"until-pub-date:{y_to}-12-31")
    if filters:
        params["filter"] = ",".join(filters)
    url = f"{CROSSREF_WORKS_URL}?{urlencode(params)}"

    last_err = None
    for i in range(max(1, retries + 1)):
        try:
            data = _http_get_json(url, timeout=45)
            msg = data.get("message", {}) if isinstance(data, dict) else {}
            raw = msg.get("items", []) if isinstance(msg, dict) else []
            if not isinstance(raw, list):
                return []

            out: List[Dict[str, Any]] = []
            for rec in raw:
                if not isinstance(rec, dict):
                    continue
                title_list = rec.get("title", [])
                title = _clean_text(title_list[0] if isinstance(title_list, list) and title_list else rec.get("title", ""))
                if not title:
                    continue
                abstract = _clean_text(rec.get("abstract", ""))
                doi = _normalize_doi(str(rec.get("DOI", "") or ""))
                year = _crossref_pick_year(rec)
                if not _year_in_range(year, date_from, date_to):
                    continue
                journal_list = rec.get("container-title", [])
                journal = _clean_text(journal_list[0] if isinstance(journal_list, list) and journal_list else "")
                cited_by = _to_int(rec.get("is-referenced-by-count"), 0)
                url_val = str(rec.get("URL", "") or "").strip()
                rtype = _clean_text(rec.get("type", "")).lower()
                preprint_flag = "posted-content" in rtype or "preprint" in rtype

                inst_names: List[str] = []
                for auth in rec.get("author", []) if isinstance(rec.get("author"), list) else []:
                    if not isinstance(auth, dict):
                        continue
                    for aff in auth.get("affiliation", []) if isinstance(auth.get("affiliation"), list) else []:
                        if not isinstance(aff, dict):
                            continue
                        nm = _clean_text(aff.get("name", ""))
                        if nm:
                            inst_names.append(nm)

                out.append(
                    {
                        "title": title,
                        "abstract": abstract,
                        "doi": doi,
                        "pmid": "",
                        "pmcid": "",
                        "year": year,
                        "url": url_val,
                        "open_access": False,
                        "journal": journal,
                        "cited_by_count": cited_by,
                        "institution_names": inst_names,
                        "retracted_flag": False,
                        "preprint_flag": preprint_flag,
                    }
                )
            return out
        except Exception as e:  # noqa: BLE001
            last_err = e
            if i < retries:
                time.sleep(1.2 * (2 ** i))

    if last_err:
        return []
    return []


def _search_semantic_native(
    query: str,
    max_results: int,
    year: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    retries: int = 2,
) -> List[Dict[str, Any]]:
    limit = max(1, min(max_results, 100))
    params: Dict[str, str] = {
        "query": query,
        "limit": str(limit),
        "fields": "title,abstract,year,venue,externalIds,url,citationCount,isOpenAccess,authors",
    }
    if year and re.fullmatch(r"\d{4}", str(year).strip()):
        params["year"] = str(year).strip()
    url = f"{SEMANTIC_GRAPH_SEARCH_URL}?{urlencode(params)}"

    last_err = None
    for i in range(max(1, retries + 1)):
        try:
            data = _http_get_json(url, timeout=45)
            raw = data.get("data", []) if isinstance(data, dict) else []
            if not isinstance(raw, list):
                return []

            out: List[Dict[str, Any]] = []
            for rec in raw:
                if not isinstance(rec, dict):
                    continue
                title = _clean_text(rec.get("title", ""))
                if not title:
                    continue
                abstract = _clean_text(rec.get("abstract", ""))
                ext = rec.get("externalIds", {}) if isinstance(rec.get("externalIds"), dict) else {}
                doi = _normalize_doi(str(ext.get("DOI", "") or ""))
                pmid = str(ext.get("PubMed", "") or "").strip()
                pmcid = str(ext.get("PubMedCentral", "") or "").strip().upper()
                y = _coerce_year(str(rec.get("year", "") or ""))
                if not _year_in_range(y, date_from, date_to):
                    continue
                venue = _clean_text(rec.get("venue", ""))
                cited_by = _to_int(rec.get("citationCount"), 0)
                url_val = str(rec.get("url", "") or "").strip()
                is_oa = bool(rec.get("isOpenAccess"))

                inst_names: List[str] = []
                for auth in rec.get("authors", []) if isinstance(rec.get("authors"), list) else []:
                    if not isinstance(auth, dict):
                        continue
                    for aff in auth.get("affiliations", []) if isinstance(auth.get("affiliations"), list) else []:
                        if isinstance(aff, str):
                            nm = _clean_text(aff)
                            if nm:
                                inst_names.append(nm)

                source_hint = f"{venue} {url_val}".lower()
                preprint_flag = any(x in source_hint for x in ("medrxiv", "biorxiv", "arxiv", "preprint"))
                out.append(
                    {
                        "title": title,
                        "abstract": abstract,
                        "doi": doi,
                        "pmid": pmid,
                        "pmcid": pmcid,
                        "year": y,
                        "url": url_val,
                        "open_access": is_oa or bool(pmcid),
                        "journal": venue,
                        "cited_by_count": cited_by,
                        "institution_names": inst_names,
                        "retracted_flag": False,
                        "preprint_flag": preprint_flag,
                    }
                )
            return out
        except HTTPError as e:
            last_err = e
            if e.code == 429 and i < retries:
                time.sleep(2.0 * (2 ** i))
                continue
            return []
        except URLError as e:
            last_err = e
            if i < retries:
                time.sleep(1.5 * (2 ** i))
                continue
            return []
        except Exception as e:  # noqa: BLE001
            last_err = e
            if i < retries:
                time.sleep(1.2 * (2 ** i))

    if last_err:
        return []
    return []


def _search_openaire(
    query: str,
    max_results: int,
    retries: int = 1,
) -> List[Dict[str, Any]]:
    params = {
        "keywords": query,
        "size": max(1, min(max_results, 100)),
        "format": "json",
    }
    url = f"{OPENAIRE_SEARCH_URL}?{urlencode(params)}"
    last_err = None
    for i in range(max(1, retries + 1)):
        try:
            data = _http_get_json(url, timeout=45)
            result_container = data.get("response", {}).get("results", {}).get("result", [])
            raw = result_container if isinstance(result_container, list) else []
            out: List[Dict[str, Any]] = []
            for rec in raw:
                metadata = rec.get("metadata", {}) if isinstance(rec, dict) else {}
                title = _clean_text(json.dumps(metadata, ensure_ascii=False))[:500]
                if not title:
                    continue
                doi_match = re.search(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", title, flags=re.IGNORECASE)
                doi = _normalize_doi(doi_match.group(0)) if doi_match else ""
                year = _coerce_year(title)
                out.append(
                    {
                        "title": title,
                        "abstract": "",
                        "doi": doi,
                        "pmid": "",
                        "pmcid": "",
                        "year": year,
                        "url": "",
                        "open_access": False,
                        "journal": "",
                        "cited_by_count": 0,
                        "institution_names": [],
                        "retracted_flag": False,
                        "preprint_flag": False,
                    }
                )
            return out
        except Exception as e:  # noqa: BLE001
            last_err = e
            if i < retries:
                time.sleep(1.2 * (2 ** i))
    if last_err:
        return []
    return []


def _search_core(
    query: str,
    max_results: int,
    core_api_key: str | None,
    retries: int = 1,
) -> List[Dict[str, Any]]:
    if not core_api_key:
        return []
    payload = {"q": query, "limit": max(1, min(max_results, 100))}
    body = json.dumps(payload).encode("utf-8")

    last_err = None
    for i in range(max(1, retries + 1)):
        try:
            req = Request(
                CORE_SEARCH_URL,
                data=body,
                headers={
                    "User-Agent": "paper_hub/1.0",
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {core_api_key}",
                },
                method="POST",
            )
            with urlopen(req, timeout=45, context=_SSL_CONTEXT) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="ignore"))
            raw = data.get("results", []) if isinstance(data, dict) else []
            out: List[Dict[str, Any]] = []
            for rec in raw if isinstance(raw, list) else []:
                if not isinstance(rec, dict):
                    continue
                doi = _normalize_doi(str(rec.get("doi", "") or ""))
                title = _clean_text(rec.get("title", ""))
                abstract = _clean_text(rec.get("abstract", ""))
                year = _coerce_year(str(rec.get("yearPublished", "") or ""))
                url = str(rec.get("downloadUrl", "") or "").strip()
                if not url:
                    fulltext_urls = rec.get("sourceFulltextUrls", [])
                    if isinstance(fulltext_urls, list) and fulltext_urls:
                        url = str(fulltext_urls[0] or "").strip()
                out.append(
                    {
                        "title": title,
                        "abstract": abstract,
                        "doi": doi,
                        "pmid": "",
                        "pmcid": "",
                        "year": year,
                        "url": url,
                        "open_access": bool(url),
                        "journal": _clean_text(rec.get("publisher", "")),
                        "cited_by_count": _to_int(rec.get("citationCount"), 0),
                        "institution_names": [],
                        "retracted_flag": False,
                        "preprint_flag": False,
                    }
                )
            return out
        except Exception as e:  # noqa: BLE001
            last_err = e
            if i < retries:
                time.sleep(1.1 * (2 ** i))
    if last_err:
        return []
    return []


# ---------------------------------------------------------------------------
# normalization, scoring, coverage
# ---------------------------------------------------------------------------

def _first_year(text: str) -> Optional[int]:
    m = re.search(r"(19\d{2}|20\d{2})", text or "")
    if not m:
        return None
    return int(m.group(1))


def _safe_lower(text: Any) -> str:
    return str(text or "").lower()


def _coverage_flags(title: str, abstract: str) -> Dict[str, bool]:
    text = f"{title} {abstract}".lower()
    return {
        "os": bool(re.search(r"\boverall survival\b|\bos\b", text)),
        "pfs": bool(re.search(r"progression[- ]free survival|\bpfs\b", text)),
        "orr": bool(re.search(r"objective response|\borr\b|\bdcr\b|\bcr\b", text)),
        "ae": bool(re.search(r"adverse event|ctcae|grade\s*[34]|treatment-related death", text)),
        "qol": bool(re.search(r"quality of life|\bqol\b|eq-5d|qlq-c30|pain", text)),
        "qaly": bool(re.search(r"\bqaly\b|quality-adjusted|\bqalm\b|cost-effectiveness", text)),
    }


def _relevance_score(record: Dict[str, Any], strategy: str) -> float:
    title = _safe_lower(record.get("title"))
    abstract = _safe_lower(record.get("abstract"))
    text = f"{title} {abstract}"

    score = 0.0

    if "pancreatic" in text:
        score += 2.0
    if "cancer" in text or "adenocarcinoma" in text:
        score += 1.2
    if "random" in text:
        score += 1.2
    if "phase iii" in text or "phase 3" in text:
        score += 1.2
    if "metastatic" in text or "locally advanced" in text or "unresectable" in text:
        score += 1.0

    flags = record.get("coverage_flags", {})
    score += 0.8 * sum(1 for k in ("os", "pfs", "orr", "ae", "qol", "qaly") if flags.get(k))

    source = str(record.get("source", ""))
    if source == "pubmed":
        score += 0.5
    if source in ("crossref", "semantic"):
        score += 0.2

    if strategy == "precision":
        if "pancreatic" not in text:
            score -= 3.0
        if "random" not in text:
            score -= 1.5
    elif strategy == "recall":
        score += 0.2

    return round(score, 4)


def _yaml_load(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        parsed = yaml.safe_load(path.read_text(encoding="utf-8"))
        return default if parsed is None else parsed
    except Exception:
        return default


def _yaml_dump(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _default_dimension_entry(dimension_id: str, run_id: str) -> Dict[str, Any]:
    base = DIMENSION_DEFINITIONS.get(dimension_id, {})
    return {
        "dimension_id": dimension_id,
        "name": str(base.get("name", dimension_id)),
        "category": str(base.get("category", "custom")),
        "unit": str(base.get("unit", "text")),
        "definition_source": str(base.get("definition_source", "auto_discovered_from_biomedical_text")),
        "allowed_tasks": list(base.get("allowed_tasks", ["task1", "task2", "task3"])),
        "status": str(base.get("default_status", "candidate")),
        "first_seen_run": run_id,
        "last_seen_run": run_id,
        "promotion_rule": str(base.get("promotion_rule", ">=2 independent source types and >=1 tier S/A source")),
        "missing_runs": 0,
    }


def _dimension_priority(dimension_id: str) -> int:
    order = [
        "os_median",
        "pfs_median",
        "orr",
        "dcr",
        "ae_grade3plus",
        "qol_score",
        "qaly",
        "icer",
    ]
    if dimension_id in order:
        return order.index(dimension_id)
    return len(order) + 10


def _discover_dimension_ids(record: Dict[str, Any]) -> List[str]:
    flags = record.get("coverage_flags", {})
    text = _safe_lower(f"{record.get('title', '')} {record.get('abstract', '')}")
    found: List[str] = []

    if bool(flags.get("os")):
        found.append("os_median")
    if bool(flags.get("pfs")):
        found.append("pfs_median")
    if bool(flags.get("orr")):
        found.extend(["orr", "dcr"])
    if bool(flags.get("ae")):
        found.append("ae_grade3plus")
    if bool(flags.get("qol")):
        found.append("qol_score")
    if bool(flags.get("qaly")):
        found.extend(["qaly", "icer"])

    if re.search(r"hazard ratio|\bhr\b|\b95% ci\b", text):
        found.extend(["os_hr_ci", "pfs_hr_ci"])
    if re.search(r"\bbicr\b|independent central review", text):
        found.append("bicr_orr")
    if re.search(r"\bsae\b|serious adverse event", text):
        found.append("sae_rate")
    if re.search(r"treatment[- ]related death|grade\s*5", text):
        found.append("trd_rate")
    if re.search(r"dose reduction|reduced dose", text):
        found.append("ae_dose_reduction_rate")
    if re.search(r"discontinuation|treatment interruption|stopped treatment", text):
        found.append("ae_discontinuation_rate")
    if re.search(r"ca19-?9", text):
        found.append("ca199_response_rate")
    if re.search(r"\br0\b|margin[- ]negative resection", text):
        found.append("r0_resection_rate")
    if re.search(r"\bpcr\b|pathologic complete response", text):
        found.append("pcr_rate")
    if re.search(r"tudd|time until definitive deterioration", text):
        found.append("tudd")

    uniq = sorted({f for f in found if f}, key=_dimension_priority)
    if not uniq:
        uniq = ["custom_clinical_signal"]
    return uniq


def _ensure_source_registry(path: Path) -> List[Dict[str, Any]]:
    raw = _yaml_load(path, {})
    entries: List[Dict[str, Any]] = []
    if isinstance(raw, dict):
        maybe_entries = raw.get("sources", [])
        if isinstance(maybe_entries, list):
            entries = [x for x in maybe_entries if isinstance(x, dict)]
    elif isinstance(raw, list):
        entries = [x for x in raw if isinstance(x, dict)]

    if not entries:
        entries = SOURCE_REGISTRY_DEFAULT
        _yaml_dump(path, {"registry_version": "1.0", "sources": entries})
        return entries

    # Ensure requested columns exist for every row.
    normalized: List[Dict[str, Any]] = []
    for row in entries:
        normalized.append(
            {
                "source_id": str(row.get("source_id", "")).strip(),
                "source_type": str(row.get("source_type", "literature")).strip(),
                "tier": str(row.get("tier", "C")).strip().upper(),
                "country": str(row.get("country", "INTL")).strip(),
                "institution_tier": str(row.get("institution_tier", "")).strip(),
                "reliability_rule": str(row.get("reliability_rule", "")).strip(),
                "alias_keywords": row.get("alias_keywords", []) if isinstance(row.get("alias_keywords", []), list) else [],
            }
        )
    _yaml_dump(path, {"registry_version": "1.0", "sources": normalized})
    return normalized


def _ensure_dimensions_catalog(path: Path, run_id: str) -> Dict[str, Any]:
    raw = _yaml_load(path, {})
    dims: List[Dict[str, Any]] = []
    if isinstance(raw, dict):
        maybe_dims = raw.get("dimensions", [])
        if isinstance(maybe_dims, list):
            dims = [x for x in maybe_dims if isinstance(x, dict)]
    elif isinstance(raw, list):
        dims = [x for x in raw if isinstance(x, dict)]

    if not dims:
        dims = [_default_dimension_entry(did, run_id) for did in sorted(DIMENSION_DEFINITIONS.keys())]
        catalog = {"catalog_version": "1.0", "dimensions": dims}
        _yaml_dump(path, catalog)
        return catalog

    normalized: List[Dict[str, Any]] = []
    for d in dims:
        did = str(d.get("dimension_id", "")).strip()
        if not did:
            continue
        base = _default_dimension_entry(did, run_id)
        base.update(d)
        base["allowed_tasks"] = d.get("allowed_tasks", base["allowed_tasks"])
        if not isinstance(base["allowed_tasks"], list):
            base["allowed_tasks"] = list(_default_dimension_entry(did, run_id)["allowed_tasks"])
        base["missing_runs"] = int(d.get("missing_runs", 0) or 0)
        normalized.append(base)
    catalog = {"catalog_version": "1.0", "dimensions": normalized}
    _yaml_dump(path, catalog)
    return catalog


def _country_group(country: str) -> str:
    c = _safe_lower(country)
    developed = {"us", "usa", "uk", "gb", "eu", "fr", "de", "it", "es", "nl", "se", "ch", "ca", "au", "jp", "kr", "il", "sg"}
    if c in {"cn", "china", "prc"}:
        return "china_top_centers"
    if c in developed:
        return "developed_markets"
    return "other"


def _index_source_registry(entries: List[Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    source_map: Dict[str, Dict[str, Any]] = {}
    institution_rows: List[Dict[str, Any]] = []
    for row in entries:
        sid = _safe_lower(row.get("source_id", "")).strip()
        if not sid:
            continue
        source_map[sid] = row
        if _safe_lower(row.get("source_type", "")) == "institution":
            institution_rows.append(row)
    return source_map, institution_rows


def _match_institution(names: List[str], institution_rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not names:
        return None
    low_names = " ".join(_safe_lower(x) for x in names)
    best: Optional[Dict[str, Any]] = None
    best_score = -1
    for row in institution_rows:
        aliases = row.get("alias_keywords", [])
        if not isinstance(aliases, list):
            aliases = []
        score = 0
        for a in aliases:
            aa = _safe_lower(a).strip()
            if aa and aa in low_names:
                score += 1
        if score > best_score and score > 0:
            best_score = score
            best = row
    return best


def _annotate_record_source_and_dimension(
    record: Dict[str, Any],
    source_map: Dict[str, Dict[str, Any]],
    institution_rows: List[Dict[str, Any]],
    catalog_by_dimension: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    source = _safe_lower(record.get("source", "")).strip()
    source_meta = source_map.get(source, {})
    source_tier = str(source_meta.get("tier", "C") or "C").upper()
    source_type_class = str(source_meta.get("source_type", "literature") or "literature")
    source_country = str(source_meta.get("country", "INTL") or "INTL")

    names = record.get("institution_names", [])
    if not isinstance(names, list):
        names = []
    inst_meta = _match_institution(names, institution_rows)

    institution_name = ""
    institution_tier = ""
    country = source_country
    if inst_meta:
        institution_name = str(inst_meta.get("source_id", "")).strip()
        institution_tier = str(inst_meta.get("institution_tier", "")).strip()
        if str(inst_meta.get("country", "")).strip():
            country = str(inst_meta.get("country", "")).strip()

    dim_ids = _discover_dimension_ids(record)
    primary_dim = dim_ids[0] if dim_ids else "custom_clinical_signal"
    dim_meta = catalog_by_dimension.get(primary_dim, _default_dimension_entry(primary_dim, "runtime"))

    journal = str(record.get("journal", "") or "").strip()
    value_source = f"{record.get('source', '')}"
    if journal:
        value_source += f"|{journal}"
    if institution_name:
        value_source += f"|{institution_name}"

    record["dimension_ids"] = dim_ids
    record["dimension_id"] = primary_dim
    record["dimension_version"] = "v1"
    record["definition_source"] = str(dim_meta.get("definition_source", "auto_discovered_from_biomedical_text"))
    record["value_source"] = value_source
    record["source_tier"] = source_tier
    record["source_type_class"] = source_type_class
    record["institution_name"] = institution_name
    record["institution_tier"] = institution_tier
    record["country"] = country
    record["country_group"] = _country_group(country)
    return record


def _build_dimension_stats(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    stats: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        dim_ids = rec.get("dimension_ids", [])
        if not isinstance(dim_ids, list):
            dim_ids = [str(rec.get("dimension_id", "")).strip()] if rec.get("dimension_id") else []
        for did in dim_ids:
            if did not in stats:
                stats[did] = {"source_types": set(), "source_tiers": set(), "count": 0}
            stats[did]["source_types"].add(str(rec.get("source_type_class", "literature")))
            stats[did]["source_tiers"].add(str(rec.get("source_tier", "C")))
            if str(rec.get("institution_tier", "")).strip():
                stats[did]["source_types"].add("institution")
            stats[did]["count"] += 1
    return stats


def _update_dimensions_catalog(
    catalog_path: Path,
    catalog: Dict[str, Any],
    records: List[Dict[str, Any]],
    run_id: str,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    dims = catalog.get("dimensions", [])
    if not isinstance(dims, list):
        dims = []
    existing = {str(d.get("dimension_id", "")): d for d in dims if isinstance(d, dict) and d.get("dimension_id")}

    stats = _build_dimension_stats(records)
    observed = set(stats.keys())
    changelog: List[Dict[str, Any]] = []

    for did in sorted(observed):
        st = stats.get(did, {})
        source_types = st.get("source_types", set())
        source_tiers = st.get("source_tiers", set())
        qualifies_core = len(source_types) >= 2 and any(t in {"S", "A"} for t in source_tiers)

        if did not in existing:
            row = _default_dimension_entry(did, run_id)
            row["status"] = "core" if qualifies_core else row.get("status", "candidate")
            row["first_seen_run"] = run_id
            row["last_seen_run"] = run_id
            row["missing_runs"] = 0
            existing[did] = row
            changelog.append(
                {
                    "run_id": run_id,
                    "dimension_id": did,
                    "action": "added",
                    "old_status": "",
                    "new_status": row["status"],
                    "reason": "observed_in_current_run",
                    "source_types": "|".join(sorted(source_types)),
                    "source_tiers": "|".join(sorted(source_tiers)),
                    "count": st.get("count", 0),
                }
            )
            continue

        row = existing[did]
        old_status = str(row.get("status", "candidate"))
        row["last_seen_run"] = run_id
        row["missing_runs"] = 0
        if old_status == "candidate" and qualifies_core:
            row["status"] = "core"
            changelog.append(
                {
                    "run_id": run_id,
                    "dimension_id": did,
                    "action": "promoted",
                    "old_status": old_status,
                    "new_status": "core",
                    "reason": "met_promotion_rule",
                    "source_types": "|".join(sorted(source_types)),
                    "source_tiers": "|".join(sorted(source_tiers)),
                    "count": st.get("count", 0),
                }
            )

    for did, row in existing.items():
        if did in observed:
            continue
        missed = int(row.get("missing_runs", 0) or 0) + 1
        row["missing_runs"] = missed
        if missed >= 2 and str(row.get("status", "")) != "deprecated":
            old_status = str(row.get("status", "candidate"))
            row["status"] = "deprecated"
            changelog.append(
                {
                    "run_id": run_id,
                    "dimension_id": did,
                    "action": "deprecated",
                    "old_status": old_status,
                    "new_status": "deprecated",
                    "reason": "missing_for_two_consecutive_runs",
                    "source_types": "",
                    "source_tiers": "",
                    "count": 0,
                }
            )

    merged = sorted(existing.values(), key=lambda x: str(x.get("dimension_id", "")))
    new_catalog = {"catalog_version": "1.0", "dimensions": merged, "last_run": run_id}
    _yaml_dump(catalog_path, new_catalog)
    catalog_map = {str(x.get("dimension_id", "")): x for x in merged}
    return new_catalog, changelog, catalog_map


def _write_csv_rows(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})


def _build_institution_provenance_rows(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for rec in records:
        if not str(rec.get("institution_tier", "")).strip():
            continue
        rows.append(
            {
                "uid": rec.get("uid", ""),
                "title": rec.get("title", ""),
                "source": rec.get("source", ""),
                "value_source": rec.get("value_source", ""),
                "source_tier": rec.get("source_tier", ""),
                "source_type_class": rec.get("source_type_class", ""),
                "institution_name": rec.get("institution_name", ""),
                "institution_tier": rec.get("institution_tier", ""),
                "country": rec.get("country", ""),
                "country_group": rec.get("country_group", ""),
                "quality_gate": rec.get("quality_gate", ""),
            }
        )
    return rows


def _quality_guard_metrics(records: List[Dict[str, Any]]) -> Dict[str, float]:
    core = [r for r in records if str(r.get("quality_gate", "")) == "core_pass"]
    if not core:
        return {
            "core_median_credibility_score": 0.0,
            "core_ab_tier_ratio": 0.0,
            "core_abstract_only_ratio": 1.0,
            "unresolved_conflict_count": 0.0,
        }
    scores = [float(r.get("credibility_score", 0) or 0) for r in core]
    ab = sum(1 for r in core if str(r.get("journal_tier", "")) in {"A", "B"})
    abstract_only = sum(1 for r in core if str(r.get("content_level", "")) != "fulltext")
    return {
        "core_median_credibility_score": float(round(statistics.median(scores), 6)),
        "core_ab_tier_ratio": float(round(ab / len(core), 6)),
        "core_abstract_only_ratio": float(round(abstract_only / len(core), 6)),
        "unresolved_conflict_count": 0.0,
    }


def _quality_guard_diff(baseline: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
    b_score = float(baseline.get("core_median_credibility_score", 0) or 0)
    c_score = float(current.get("core_median_credibility_score", 0) or 0)
    b_ab = float(baseline.get("core_ab_tier_ratio", 0) or 0)
    c_ab = float(current.get("core_ab_tier_ratio", 0) or 0)
    b_abstract = float(baseline.get("core_abstract_only_ratio", 1) or 1)
    c_abstract = float(current.get("core_abstract_only_ratio", 1) or 1)
    b_conf = float(baseline.get("unresolved_conflict_count", 0) or 0)
    c_conf = float(current.get("unresolved_conflict_count", 0) or 0)

    diff = {
        "core_median_credibility_score_diff": round(c_score - b_score, 6),
        "core_ab_tier_ratio_diff": round(c_ab - b_ab, 6),
        "core_abstract_only_ratio_diff": round(b_abstract - c_abstract, 6),
        "unresolved_conflict_count_diff": round(b_conf - c_conf, 6),
    }
    diff["quality_guard_pass"] = all(float(v) >= -1e-9 for v in diff.values())
    return diff


def _infer_discipline_profile(record: Dict[str, Any]) -> str:
    text = _safe_lower(f"{record.get('title', '')} {record.get('abstract', '')}")
    if re.search(r"\bqaly\b|quality-adjusted|cost-effectiveness|icer|qalm|pharmacoeconomic", text):
        return "qol_health_econ"
    if re.search(r"real[- ]world|registry|retrospective|cohort|observational|database analysis", text):
        return "observational_realworld"
    return "clinical_trial"


def _topic_mismatch(record: Dict[str, Any]) -> bool:
    text = _safe_lower(f"{record.get('title', '')} {record.get('abstract', '')}")
    if "pancrea" in text:
        return False
    return True


def _journal_tier(journal: str) -> str:
    j = _safe_lower(journal)
    if not j:
        return "U"
    if any(k in j for k in JOURNAL_TIER_A_KEYWORDS):
        return "A"
    if any(k in j for k in JOURNAL_TIER_B_KEYWORDS):
        return "B"
    return "C"


def _journal_cred(tier: str) -> int:
    if tier == "A":
        return 25
    if tier == "B":
        return 18
    if tier == "C":
        return 10
    return 5


def _source_cred(record: Dict[str, Any]) -> int:
    source = str(record.get("source", "") or "").strip()
    base = int(SOURCE_CRED_BASE.get(source, 8))
    if record.get("pmid"):
        base += 1
    if record.get("pmcid"):
        base += 1
    return max(0, min(20, base))


def _institution_signal(record: Dict[str, Any]) -> str:
    names = record.get("institution_names", [])
    if not isinstance(names, list):
        names = []
    low_names = " ".join(_safe_lower(n) for n in names)
    if any(k in low_names for k in HIGH_INSTITUTION_KEYWORDS):
        return "high"
    if low_names.strip():
        return "medium"
    return "low"


def _design_cred(record: Dict[str, Any], profile: str) -> Tuple[int, bool]:
    text = _safe_lower(f"{record.get('title', '')} {record.get('abstract', '')} {record.get('study_design', '')}")
    hits = sum(1 for p in DESIGN_RANDOMIZED_PATTERNS if re.search(p, text))
    strong = hits >= 1
    score = 0
    if profile == "clinical_trial":
        score = 10 + hits * 5
    elif profile == "qol_health_econ":
        score = 8 + hits * 4
    else:
        score = 6 + hits * 3
    return max(0, min(25, score)), strong


def _citation_stats(record: Dict[str, Any], citation_age_window: int, current_year: int) -> Tuple[int, int, float]:
    cited = _to_int(record.get("cited_by_count"), 0)
    year = record.get("year")
    y = int(year) if isinstance(year, int) else _coerce_year(str(year or ""))
    if not y:
        y = current_year
    age = max(1, current_year - y + 1)
    adjusted = cited / (age ** 0.7)
    if age <= max(1, citation_age_window):
        adjusted = adjusted * 1.15
    return cited, age, round(adjusted, 4)


def _citation_cred(profile: str, cited: int, age: int, adjusted: float, citation_age_window: int) -> int:
    if profile == "qol_health_econ":
        raw = adjusted * 3.2
    elif profile == "observational_realworld":
        raw = adjusted * 2.4
    else:
        raw = adjusted * 2.1
    if age <= max(1, citation_age_window) and cited <= 2:
        raw += 2.0
    return max(0, min(20, int(round(raw))))


def _integrity_cred(record: Dict[str, Any]) -> int:
    score = 0
    if _clean_text(record.get("abstract", "")):
        score += 4
    if record.get("doi"):
        score += 2
    if record.get("pmid") or record.get("pmcid"):
        score += 2
    if record.get("open_access_flag"):
        score += 1
    if _safe_lower(record.get("source", "")) in {"pubmed", "europe_pmc", "openalex"}:
        score += 1
    return max(0, min(10, score))


def _quality_penalty(
    record: Dict[str, Any],
    topic_mismatch: bool,
    has_identifier: bool,
    profile: str,
    preprint_flag: bool,
) -> Tuple[int, List[str]]:
    penalty = 0
    reasons: List[str] = []

    if topic_mismatch:
        penalty += 35
        reasons.append("topic_mismatch")

    if not has_identifier:
        penalty += 35
        reasons.append("no_identifier")

    if preprint_flag:
        penalty += 10
        reasons.append("preprint")

    if bool(record.get("retracted_flag")):
        penalty += 40
        reasons.append("retracted")

    if not _clean_text(record.get("abstract", "")):
        penalty += 6
        reasons.append("missing_abstract")

    source = str(record.get("source", "") or "")
    if SOURCE_CRED_BASE.get(source, 8) <= 7:
        penalty += 4
        reasons.append("low_source_confidence")

    if profile == "observational_realworld" and "random" not in _safe_lower(record.get("title", "")):
        penalty += 2
        reasons.append("non_randomized_design")

    return max(0, min(40, penalty)), reasons


def _credibility_tier(gate: str, score: int) -> str:
    if gate == "core_pass":
        return "high"
    if gate in {"extended_review", "preprint_extended"}:
        return "medium"
    if score >= 70:
        return "high"
    if score >= 50:
        return "medium"
    return "low"


def _apply_quality_scoring(
    records: List[Dict[str, Any]],
    quality_filter: str,
    core_threshold: int,
    extended_threshold: int,
    citation_age_window: int,
    preprint_policy: str,
) -> Dict[str, int]:
    now_year = datetime.now(timezone.utc).year
    summary = {
        "core_pass": 0,
        "extended_review": 0,
        "reject": 0,
        "preprint_extended": 0,
    }

    for rec in records:
        profile = _infer_discipline_profile(rec)
        source_cred = _source_cred(rec)
        journal_tier = _journal_tier(str(rec.get("journal", "") or ""))
        journal_cred = _journal_cred(journal_tier)
        cited, age_years, citation_adjusted = _citation_stats(rec, citation_age_window, now_year)
        citation_cred = _citation_cred(profile, cited, age_years, citation_adjusted, citation_age_window)
        design_cred, design_strong = _design_cred(rec, profile)
        integrity_cred = _integrity_cred(rec)
        institution_signal = _institution_signal(rec)
        preprint_flag = bool(rec.get("preprint_flag")) or str(rec.get("source", "") or "").strip() in {"medrxiv", "biorxiv", "arxiv"}
        topic_bad = _topic_mismatch(rec)
        has_identifier = bool(rec.get("doi") or rec.get("pmid") or rec.get("pmcid"))
        penalty, reasons = _quality_penalty(
            rec,
            topic_mismatch=topic_bad,
            has_identifier=has_identifier,
            profile=profile,
            preprint_flag=preprint_flag,
        )

        raw_score = source_cred + journal_cred + citation_cred + design_cred + integrity_cred - penalty
        score = int(max(0, min(100, raw_score)))

        hard_reject_reasons: List[str] = []
        if bool(rec.get("retracted_flag")):
            hard_reject_reasons.append("retracted")
        if topic_bad:
            hard_reject_reasons.append("topic_mismatch")
        if not has_identifier:
            hard_reject_reasons.append("no_identifier")

        young_compensated = False
        if age_years <= citation_age_window and citation_cred <= 4:
            if journal_tier == "A" or institution_signal == "high" or design_strong:
                young_compensated = True
                score = max(score, extended_threshold)

        gate = "core_pass"
        rejection_reason = ""

        if quality_filter == "off":
            gate = "core_pass"
        else:
            if hard_reject_reasons:
                gate = "reject"
                rejection_reason = ",".join(sorted(set(hard_reject_reasons)))
            elif preprint_flag and preprint_policy == "separate_sheet":
                gate = "preprint_extended"
                rejection_reason = "preprint_separate_sheet"
            elif score >= core_threshold:
                gate = "core_pass"
            elif score >= extended_threshold:
                gate = "extended_review"
                rejection_reason = "below_core_threshold"
            else:
                if young_compensated:
                    gate = "extended_review"
                    rejection_reason = "young_article_compensated"
                else:
                    gate = "reject"
                    rejection_reason = "low_credibility_score"

        rec["discipline_profile"] = profile
        rec["source_cred"] = source_cred
        rec["journal_tier"] = journal_tier
        rec["journal_cred"] = journal_cred
        rec["cited_by_count"] = cited
        rec["citation_age_years"] = age_years
        rec["citation_age_adjusted"] = citation_adjusted
        rec["citation_cred"] = citation_cred
        rec["design_cred"] = design_cred
        rec["integrity_cred"] = integrity_cred
        rec["institution_signal"] = institution_signal
        rec["preprint_flag"] = preprint_flag
        rec["credibility_score"] = score
        rec["quality_gate"] = gate
        rec["rejection_reason"] = rejection_reason
        rec["credibility_tier"] = _credibility_tier(gate, score)
        rec["quality_penalty"] = penalty
        rec["quality_penalty_reasons"] = ",".join(reasons)

        summary[gate] = summary.get(gate, 0) + 1

    return summary


def _normalize_title_for_key(title: str) -> str:
    t = re.sub(r"\s+", " ", (title or "")).strip().lower()
    t = re.sub(r"[^a-z0-9\s]", "", t)
    return t[:220]


def _make_uid(doi: str, pmid: str, title: str, year: Optional[int], source: str) -> str:
    if doi:
        return f"doi:{doi.lower()}"
    if pmid:
        return f"pmid:{pmid}"
    digest = hashlib.sha1(f"{_normalize_title_for_key(title)}|{year or ''}|{source}".encode("utf-8")).hexdigest()[:16]
    return f"hash:{digest}"


def _is_open_access_hint(source: str, url: str, pdf_url: str, pmcid: str) -> bool:
    if pmcid:
        return True
    if source in {"arxiv", "biorxiv", "medrxiv"}:
        return True
    combined = f"{url} {pdf_url}".lower()
    return "pmc" in combined or "bioarxiv" in combined or "medrxiv" in combined or combined.endswith(".pdf")


def _normalize_pubmed_record(rec: Dict[str, Any], strategy: str) -> Dict[str, Any]:
    title = str(rec.get("title", "")).strip()
    abstract = str(rec.get("abstract", "")).strip()
    doi = str(rec.get("doi", "")).strip()
    pmid = str(rec.get("pubmed_id", "")).strip()
    date = str(rec.get("date", "")).strip()
    year = _first_year(date) or _first_year(title) or _first_year(abstract)

    out = {
        "uid": _make_uid(doi, pmid, title, year, "pubmed"),
        "title": title,
        "abstract": abstract,
        "doi": doi,
        "pmid": pmid,
        "pmcid": "",
        "year": year,
        "journal": _clean_text(rec.get("journal", "")),
        "cited_by_count": 0,
        "institution_names": [],
        "study_design": "",
        "preprint_flag": False,
        "retracted_flag": False,
        "source": "pubmed",
        "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else "",
        "coverage_flags": _coverage_flags(title, abstract),
        "open_access_flag": False,
        "reason_not_parsed": "missing_abstract" if not abstract else "",
        "matched_query": "",
        "abstract_source": "pubmed" if abstract else "",
        "oa_locations": [],
        "rights_status": "",
        "content_level": "metadata",
        "reason_abstract_missing": "not_backfilled" if not abstract else "",
    }
    out["relevance_score"] = _relevance_score(out, strategy)
    return out


def _normalize_mcp_record(rec: Dict[str, Any], source: str, strategy: str) -> Dict[str, Any]:
    title = str(rec.get("title", "")).strip()
    abstract = str(rec.get("abstract", "")).strip()
    doi = str(rec.get("doi", "")).strip()
    paper_id = str(rec.get("paper_id", "")).strip()

    pmid = ""
    if source == "pubmed" and paper_id.isdigit():
        pmid = paper_id

    url = str(rec.get("url", "")).strip()
    pdf_url = str(rec.get("pdf_url", "")).strip()

    year = _first_year(str(rec.get("published_date", ""))) or _first_year(title) or _first_year(abstract)

    out = {
        "uid": _make_uid(doi, pmid, title, year, source),
        "title": title,
        "abstract": abstract,
        "doi": doi,
        "pmid": pmid,
        "pmcid": "",
        "year": year,
        "journal": _clean_text(rec.get("journal", "")),
        "cited_by_count": _to_int(rec.get("citation_count"), 0),
        "institution_names": [],
        "study_design": "",
        "preprint_flag": source in {"medrxiv", "biorxiv", "arxiv"},
        "retracted_flag": bool(rec.get("is_retracted", False)),
        "source": source,
        "url": url,
        "coverage_flags": _coverage_flags(title, abstract),
        "open_access_flag": _is_open_access_hint(source, url, pdf_url, ""),
        "reason_not_parsed": "missing_abstract" if not abstract else "",
        "matched_query": "",
        "abstract_source": source if abstract else "",
        "oa_locations": [],
        "rights_status": "",
        "content_level": "metadata",
        "reason_abstract_missing": "not_backfilled" if not abstract else "",
    }
    out["relevance_score"] = _relevance_score(out, strategy)
    return out


def _normalize_external_record(rec: Dict[str, Any], source: str, strategy: str) -> Dict[str, Any]:
    title = _clean_text(rec.get("title", ""))
    abstract = _clean_text(rec.get("abstract", ""))
    doi = _normalize_doi(str(rec.get("doi", "") or ""))
    pmid = str(rec.get("pmid", "") or "").strip()
    pmcid = str(rec.get("pmcid", "") or "").strip().upper()
    year = _coerce_year(str(rec.get("year", "") or "")) or _first_year(title) or _first_year(abstract)
    url = str(rec.get("url", "") or "").strip()
    open_access = bool(rec.get("open_access")) or bool(pmcid)

    out = {
        "uid": _make_uid(doi, pmid, title, year, source),
        "title": title,
        "abstract": abstract,
        "doi": doi,
        "pmid": pmid,
        "pmcid": pmcid,
        "year": year,
        "journal": _clean_text(rec.get("journal", "")),
        "cited_by_count": _to_int(rec.get("cited_by_count"), 0),
        "institution_names": rec.get("institution_names", []) if isinstance(rec.get("institution_names"), list) else [],
        "study_design": "",
        "preprint_flag": bool(rec.get("preprint_flag")),
        "retracted_flag": bool(rec.get("retracted_flag")),
        "source": source,
        "url": url,
        "coverage_flags": _coverage_flags(title, abstract),
        "open_access_flag": open_access,
        "reason_not_parsed": "missing_abstract" if not abstract else "",
        "matched_query": "",
        "abstract_source": source if abstract else "",
        "oa_locations": [],
        "rights_status": "open" if open_access else "",
        "content_level": "metadata",
        "reason_abstract_missing": "not_backfilled" if not abstract else "",
    }
    out["relevance_score"] = _relevance_score(out, strategy)
    return out


def _dedupe_normalized(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best: Dict[str, Dict[str, Any]] = {}

    for rec in records:
        doi = str(rec.get("doi", "")).strip().lower()
        pmid = str(rec.get("pmid", "")).strip()
        title_key = _normalize_title_for_key(str(rec.get("title", "")))
        year = rec.get("year")

        if doi:
            key = f"doi:{doi}"
        elif pmid:
            key = f"pmid:{pmid}"
        else:
            key = f"title:{title_key}:{year or ''}"

        if key not in best:
            best[key] = rec
            continue

        prev = best[key]
        if float(rec.get("relevance_score", 0.0)) > float(prev.get("relevance_score", 0.0)):
            best[key] = rec

    rows = list(best.values())
    rows.sort(key=lambda x: (float(x.get("relevance_score", 0.0)), int(x.get("year") or 0)), reverse=True)
    return rows


# ---------------------------------------------------------------------------
# PMCID lookup adapter
# ---------------------------------------------------------------------------

def _http_get(url: str, timeout: int = 30) -> bytes:
    req = Request(url, headers={"User-Agent": "paper_hub/1.0"})
    with urlopen(req, timeout=timeout, context=_SSL_CONTEXT) as resp:
        return resp.read()


def _lookup_pmcid(doi: str = "", pmid: str = "", retries: int = 2) -> str:
    cache_key = f"doi:{doi.lower()}" if doi else f"pmid:{pmid}"
    if cache_key in PMC_CACHE:
        return PMC_CACHE[cache_key]

    if not doi and not pmid:
        return ""

    ids = doi or pmid
    params = {
        "tool": "paper_hub",
        "email": "paper-hub@example.org",
        "ids": ids,
        "format": "json",
    }
    url = f"{PMC_IDCONV_URL}?{urlencode(params)}"

    last_err = ""
    for i in range(max(1, retries + 1)):
        try:
            payload = _http_get(url, timeout=25)
            data = json.loads(payload.decode("utf-8", errors="ignore"))
            records = data.get("records", []) if isinstance(data, dict) else []
            pmcid = ""
            if records and isinstance(records[0], dict):
                pmcid = str(records[0].get("pmcid", "") or "").strip()
            PMC_CACHE[cache_key] = pmcid
            return pmcid
        except Exception as e:  # noqa: BLE001
            last_err = str(e)
            if i < retries:
                time.sleep(1.2 * (2 ** i))

    PMC_CACHE[cache_key] = ""
    if last_err:
        return ""
    return ""


def _enrich_with_pmcid(records: List[Dict[str, Any]], limit: int = 200, retries: int = 2) -> None:
    count = 0
    for rec in records:
        if count >= limit:
            break
        if rec.get("pmcid"):
            continue
        doi = str(rec.get("doi", "")).strip()
        pmid = str(rec.get("pmid", "")).strip()
        if not doi and not pmid:
            continue
        pmcid = _lookup_pmcid(doi=doi, pmid=pmid, retries=retries)
        if pmcid:
            rec["pmcid"] = pmcid
            rec["open_access_flag"] = True
        count += 1


def _lookup_unpaywall(doi: str, email: str, retries: int = 2) -> Dict[str, Any]:
    if not doi:
        return {}
    safe_doi = quote(_normalize_doi(doi), safe="")
    url = f"{UNPAYWALL_URL.format(doi=safe_doi)}?{urlencode({'email': email})}"
    last_err = None
    for i in range(max(1, retries + 1)):
        try:
            return _http_get_json(url, timeout=35)
        except Exception as e:  # noqa: BLE001
            last_err = e
            if i < retries:
                time.sleep(1.1 * (2 ** i))
    if last_err:
        return {}
    return {}


def _enrich_oa_locations(records: List[Dict[str, Any]], email: str, limit: int = 400, retries: int = 2) -> None:
    n = 0
    for rec in records:
        if n >= limit:
            break
        doi = _normalize_doi(str(rec.get("doi", "") or ""))
        if not doi:
            continue
        n += 1
        data = _lookup_unpaywall(doi, email=email, retries=retries)
        if not isinstance(data, dict) or not data:
            continue
        is_oa = bool(data.get("is_oa"))
        rec["open_access_flag"] = bool(rec.get("open_access_flag")) or is_oa
        rec["rights_status"] = str(data.get("oa_status", "") or rec.get("rights_status", "")).strip()
        locations: List[str] = []
        for loc in data.get("oa_locations", []) if isinstance(data.get("oa_locations"), list) else []:
            if not isinstance(loc, dict):
                continue
            pdf = str(loc.get("url_for_pdf", "") or "").strip()
            lnk = str(loc.get("url", "") or "").strip()
            if pdf:
                locations.append(pdf)
            if lnk:
                locations.append(lnk)
        best = data.get("best_oa_location", {})
        if isinstance(best, dict):
            bpdf = str(best.get("url_for_pdf", "") or "").strip()
            burl = str(best.get("url", "") or "").strip()
            if bpdf:
                locations.append(bpdf)
            if burl:
                locations.append(burl)
        deduped: List[str] = []
        seen = set()
        for u in locations:
            if not u or u in seen:
                continue
            seen.add(u)
            deduped.append(u)
        if deduped:
            rec["oa_locations"] = deduped
            if not rec.get("url"):
                rec["url"] = deduped[0]


def _extract_pubmed_abstract_from_xml(xml_bytes: bytes) -> str:
    try:
        root = ET.fromstring(xml_bytes.decode("utf-8", errors="ignore"))
    except Exception:
        return ""
    texts: List[str] = []
    for node in root.findall(".//Abstract/AbstractText"):
        label = (node.attrib.get("Label", "") or "").strip()
        txt = _clean_text("".join(node.itertext()))
        if not txt:
            continue
        if label:
            texts.append(f"{label}: {txt}")
        else:
            texts.append(txt)
    return _clean_text("\n".join(texts))


def _fetch_pubmed_abstract_by_pmid(pmid: str, retries: int = 2) -> str:
    if not pmid:
        return ""
    url = (
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
        f"?db=pubmed&id={quote(pmid)}&retmode=xml"
    )
    last_err = None
    for i in range(max(1, retries + 1)):
        try:
            body = _http_get(url, timeout=35)
            return _extract_pubmed_abstract_from_xml(body)
        except Exception as e:  # noqa: BLE001
            last_err = e
            if i < retries:
                time.sleep(1.1 * (2 ** i))
    if last_err:
        return ""
    return ""


def _fetch_europe_pmc_abstract(doi: str = "", pmid: str = "", retries: int = 2) -> str:
    clause = ""
    if pmid:
        clause = f"EXT_ID:{pmid} AND SRC:MED"
    elif doi:
        clause = f"DOI:{doi}"
    if not clause:
        return ""
    params = {
        "query": clause,
        "format": "json",
        "resultType": "core",
        "pageSize": 1,
    }
    url = f"{EUROPE_PMC_SEARCH_URL}?{urlencode(params)}"
    last_err = None
    for i in range(max(1, retries + 1)):
        try:
            data = _http_get_json(url, timeout=35)
            raw = data.get("resultList", {}).get("result", []) if isinstance(data, dict) else []
            if not isinstance(raw, list) or not raw:
                return ""
            return _clean_text(raw[0].get("abstractText", ""))
        except Exception as e:  # noqa: BLE001
            last_err = e
            if i < retries:
                time.sleep(1.1 * (2 ** i))
    if last_err:
        return ""
    return ""


def _fetch_crossref_abstract(doi: str, retries: int = 2) -> str:
    if not doi:
        return ""
    url = f"https://api.crossref.org/works/{quote(_normalize_doi(doi), safe='')}"
    last_err = None
    for i in range(max(1, retries + 1)):
        try:
            data = _http_get_json(url, timeout=35)
            message = data.get("message", {}) if isinstance(data, dict) else {}
            abstract = _clean_text(message.get("abstract", ""))
            return abstract
        except Exception as e:  # noqa: BLE001
            last_err = e
            if i < retries:
                time.sleep(1.1 * (2 ** i))
    if last_err:
        return ""
    return ""


def _fetch_openalex_abstract(doi: str, retries: int = 2) -> str:
    if not doi:
        return ""
    params = {
        "filter": f"doi:{_normalize_doi(doi)}",
        "per-page": 1,
    }
    url = f"{OPENALEX_WORKS_URL}?{urlencode(params)}"
    last_err = None
    for i in range(max(1, retries + 1)):
        try:
            data = _http_get_json(url, timeout=35)
            raw = data.get("results", []) if isinstance(data, dict) else []
            if not isinstance(raw, list) or not raw:
                return ""
            return _openalex_abstract_from_index(raw[0].get("abstract_inverted_index"))
        except Exception as e:  # noqa: BLE001
            last_err = e
            if i < retries:
                time.sleep(1.1 * (2 ** i))
    if last_err:
        return ""
    return ""


def _backfill_abstract_for_record(record: Dict[str, Any], retries: int = 2) -> Dict[str, Any]:
    abstract = _clean_text(record.get("abstract", ""))
    if abstract:
        record["abstract"] = abstract
        record["abstract_source"] = str(record.get("abstract_source") or record.get("source") or "").strip()
        record["reason_abstract_missing"] = ""
        return record

    doi = _normalize_doi(str(record.get("doi", "") or ""))
    pmid = str(record.get("pmid", "") or "").strip()

    if pmid:
        candidate = _fetch_pubmed_abstract_by_pmid(pmid, retries=retries)
        if candidate:
            record["abstract"] = candidate
            record["abstract_source"] = "pubmed"
            record["reason_abstract_missing"] = ""
            record["coverage_flags"] = _coverage_flags(str(record.get("title", "")), candidate)
            return record

    candidate = _fetch_europe_pmc_abstract(doi=doi, pmid=pmid, retries=retries)
    if candidate:
        record["abstract"] = candidate
        record["abstract_source"] = "europe_pmc"
        record["reason_abstract_missing"] = ""
        record["coverage_flags"] = _coverage_flags(str(record.get("title", "")), candidate)
        return record

    if doi:
        candidate = _fetch_crossref_abstract(doi, retries=retries)
        if candidate:
            record["abstract"] = candidate
            record["abstract_source"] = "crossref"
            record["reason_abstract_missing"] = ""
            record["coverage_flags"] = _coverage_flags(str(record.get("title", "")), candidate)
            return record

        candidate = _fetch_openalex_abstract(doi, retries=retries)
        if candidate:
            record["abstract"] = candidate
            record["abstract_source"] = "openalex"
            record["reason_abstract_missing"] = ""
            record["coverage_flags"] = _coverage_flags(str(record.get("title", "")), candidate)
            return record

    record["reason_abstract_missing"] = "not_found_in_pubmed_europepmc_crossref_openalex"
    return record


def _backfill_abstracts(records: List[Dict[str, Any]], max_workers: int = 6, retries: int = 2) -> None:
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as ex:
        fut_map = {ex.submit(_backfill_abstract_for_record, rec, retries): i for i, rec in enumerate(records)}
        for fut in as_completed(fut_map):
            i = fut_map[fut]
            try:
                records[i] = fut.result()
            except Exception:
                records[i]["reason_abstract_missing"] = "backfill_runtime_error"


# ---------------------------------------------------------------------------
# search-multi
# ---------------------------------------------------------------------------

def _split_sources(text: str | None) -> List[str]:
    if not text:
        return list(SOURCE_DEFAULTS)
    out = []
    for p in text.split(","):
        v = p.strip()
        if not v:
            continue
        if v not in SEARCH_SOURCES:
            _die(f"Unsupported source in --sources: {v}")
        out.append(v)
    if not out:
        return list(SOURCE_DEFAULTS)
    return out


def _split_legal_sources(text: str | None) -> List[str]:
    if not text:
        return list(LEGAL_MAX_DEFAULT_SOURCES)
    out: List[str] = []
    for p in text.split(","):
        v = p.strip()
        if not v:
            continue
        if v not in LEGAL_MAX_SOURCES:
            _die(f"Unsupported source in --sources: {v}")
        out.append(v)
    if not out:
        return list(LEGAL_MAX_DEFAULT_SOURCES)
    return out


def _read_queries_file(path: str) -> List[str]:
    p = Path(path).expanduser().resolve()
    if not p.exists():
        _die(f"queries file not found: {p}")
    lines = []
    for line in p.read_text(encoding="utf-8").splitlines():
        t = line.strip()
        if not t or t.startswith("#"):
            continue
        lines.append(t)
    return lines


def _expand_queries(base_queries: List[str], strategy: str) -> List[str]:
    expanded: List[str] = []

    for q in base_queries:
        expanded.append(q)

        if strategy == "precision":
            expanded.append(f"({q}) AND (randomized OR randomised) AND (phase III OR phase 3)")
            continue

        if strategy == "balance":
            expanded.append(f"({q}) AND (overall survival OR progression-free survival OR ORR)")
            expanded.append(f"({q}) AND (quality of life OR adverse event OR CTCAE)")
            continue

        # recall
        expanded.append(f"({q}) AND (randomized OR randomised OR clinical trial)")
        expanded.append(f"({q}) AND (overall survival OR progression-free survival OR ORR OR DCR)")
        expanded.append(f"({q}) AND (adverse event OR CTCAE OR grade 3 OR treatment-related death)")
        expanded.append(f"({q}) AND (quality of life OR QOL OR EQ-5D OR QLQ-C30 OR pain)")
        expanded.append(f"({q}) AND (QALY OR quality-adjusted OR QALM OR cost-effectiveness)")

    dedup = []
    seen = set()
    for q in expanded:
        k = q.lower().strip()
        if k in seen:
            continue
        seen.add(k)
        dedup.append(q)
    return dedup


def _search_legal_source_job(
    source: str,
    query: str,
    retmax: int,
    strategy: str,
    year: str | None,
    date_from: str | None,
    date_to: str | None,
    retries: int,
    core_api_key: str | None,
) -> Tuple[str, str, List[Dict[str, Any]], Optional[str]]:
    try:
        normalized: List[Dict[str, Any]] = []

        if source == "pubmed":
            rows = _search_pubmed_adapter(
                query=query,
                max_results=retmax,
                retstart=0,
                date_from=date_from,
                date_to=date_to,
                retries=retries,
            )
            for r in rows:
                n = _normalize_pubmed_record(r, strategy=strategy)
                n["matched_query"] = query
                normalized.append(n)
            return source, query, normalized, None

        if source == "crossref":
            rows = _search_crossref_native(
                query=query,
                max_results=retmax,
                date_from=date_from,
                date_to=date_to,
                retries=retries,
            )
            for r in rows:
                n = _normalize_external_record(r, source=source, strategy=strategy)
                n["matched_query"] = query
                normalized.append(n)
            return source, query, normalized, None

        if source == "semantic":
            rows = _search_semantic_native(
                query=query,
                max_results=retmax,
                year=year,
                date_from=date_from,
                date_to=date_to,
                retries=retries,
            )
            for r in rows:
                n = _normalize_external_record(r, source=source, strategy=strategy)
                n["matched_query"] = query
                normalized.append(n)
            return source, query, normalized, None

        if source in {"google_scholar", "medrxiv", "biorxiv"}:
            rows = _search_mcp_source(source, query, max_results=retmax, year=year, retries=retries)
            for r in rows:
                n = _normalize_mcp_record(r, source=source, strategy=strategy)
                n["matched_query"] = query
                normalized.append(n)
            return source, query, normalized, None

        if source == "europe_pmc":
            rows = _search_europe_pmc(
                query=query,
                max_results=retmax,
                date_from=date_from,
                date_to=date_to,
                retries=retries,
            )
            for r in rows:
                n = _normalize_external_record(r, source=source, strategy=strategy)
                n["matched_query"] = query
                normalized.append(n)
            return source, query, normalized, None

        if source == "openalex":
            rows = _search_openalex(
                query=query,
                max_results=retmax,
                date_from=date_from,
                date_to=date_to,
                retries=retries,
            )
            for r in rows:
                n = _normalize_external_record(r, source=source, strategy=strategy)
                n["matched_query"] = query
                normalized.append(n)
            return source, query, normalized, None

        if source == "openaire":
            rows = _search_openaire(query=query, max_results=retmax, retries=max(1, retries - 1))
            for r in rows:
                n = _normalize_external_record(r, source=source, strategy=strategy)
                n["matched_query"] = query
                normalized.append(n)
            return source, query, normalized, None

        if source == "core":
            rows = _search_core(
                query=query,
                max_results=retmax,
                core_api_key=core_api_key,
                retries=max(1, retries - 1),
            )
            for r in rows:
                n = _normalize_external_record(r, source=source, strategy=strategy)
                n["matched_query"] = query
                normalized.append(n)
            return source, query, normalized, None

        return source, query, [], f"unsupported_source:{source}"
    except Exception as e:  # noqa: BLE001
        return source, query, [], str(e)


def _search_source_job(
    source: str,
    query: str,
    retmax: int,
    strategy: str,
    year: str | None,
    date_from: str | None,
    date_to: str | None,
    retries: int,
) -> Tuple[str, str, List[Dict[str, Any]], Optional[str]]:
    try:
        if source == "pubmed":
            rows = _search_pubmed_adapter(
                query=query,
                max_results=retmax,
                retstart=0,
                date_from=date_from,
                date_to=date_to,
                retries=retries,
            )
            normalized = []
            for r in rows:
                n = _normalize_pubmed_record(r, strategy=strategy)
                n["matched_query"] = query
                normalized.append(n)
            return source, query, normalized, None

        if source == "crossref":
            rows = _search_crossref_native(
                query=query,
                max_results=retmax,
                date_from=date_from,
                date_to=date_to,
                retries=retries,
            )
            normalized = []
            for r in rows:
                n = _normalize_external_record(r, source=source, strategy=strategy)
                n["matched_query"] = query
                normalized.append(n)
            return source, query, normalized, None

        if source == "semantic":
            rows = _search_semantic_native(
                query=query,
                max_results=retmax,
                year=year,
                date_from=date_from,
                date_to=date_to,
                retries=retries,
            )
            normalized = []
            for r in rows:
                n = _normalize_external_record(r, source=source, strategy=strategy)
                n["matched_query"] = query
                normalized.append(n)
            return source, query, normalized, None

        rows = _search_mcp_source(source, query, max_results=retmax, year=year, retries=retries)
        normalized = []
        for r in rows:
            n = _normalize_mcp_record(r, source=source, strategy=strategy)
            n["matched_query"] = query
            normalized.append(n)
        return source, query, normalized, None
    except Exception as e:
        return source, query, [], str(e)


def _write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _load_records(path: Path) -> List[Dict[str, Any]]:
    if path.suffix.lower() == ".jsonl":
        rows: List[Dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            t = line.strip()
            if not t:
                continue
            rows.append(json.loads(t))
        return rows

    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return data
        _die(f"Expected list in JSON input: {path}")

    _die(f"Unsupported input file extension: {path}")
    return []


# ---------------------------------------------------------------------------
# download adapters
# ---------------------------------------------------------------------------

def _download_doi_internal(doi: str, output_base: Path, api_keys: str | None, retries: int) -> Tuple[bool, Dict[str, Any], str]:
    output_base.parent.mkdir(parents=True, exist_ok=True)

    code = r'''
import json
import sys
from pathlib import Path

from paperscraper.pdf import save_pdf

doi = sys.argv[1]
out_path = Path(sys.argv[2])
api_keys = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else None
paper = {"doi": doi, "title": doi}

try:
    ok = save_pdf(paper, str(out_path), api_keys=api_keys)
    print(json.dumps({
        "ok": bool(ok),
        "pdf": str(out_path.with_suffix(".pdf")),
        "xml": str(out_path.with_suffix(".xml")),
    }, ensure_ascii=False))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
'''

    result = _run_python_retry(
        "paperscraper",
        code,
        [doi, str(output_base), api_keys or ""],
        context="download-doi-internal",
        retries=retries,
        timeout=180,
    )

    data, err = _extract_json_safe(result)
    if err:
        return False, {}, err
    if not isinstance(data, dict):
        return False, {}, "invalid_download_payload"

    ok = bool(data.get("ok"))
    if ok:
        return True, data, ""
    return False, data, str(data.get("error", "download_failed"))


def _download_pmc_bioc(pmcid: str, output_base: Path, timeout: int = 60, retries: int = 2) -> Tuple[bool, str, str]:
    pmc = pmcid if str(pmcid).startswith("PMC") else f"PMC{pmcid}"
    url = PMC_BIOC_URL.format(pmcid=pmc)
    xml_path = output_base.with_suffix(".xml")

    for i in range(max(1, retries + 1)):
        try:
            body = _http_get(url, timeout=timeout)
            if body.startswith(b"[Error] : No result can be found"):
                return False, "", "pmc_no_result"
            xml_path.parent.mkdir(parents=True, exist_ok=True)
            xml_path.write_bytes(body)
            return True, str(xml_path), ""
        except Exception as e:
            if i < retries:
                time.sleep(1.2 * (2 ** i))
            else:
                return False, "", str(e)

    return False, "", "pmc_download_failed"


def _download_direct_url(url: str, output_pdf: Path, timeout: int = 45, retries: int = 1) -> Tuple[bool, str]:
    if not url:
        return False, "empty_url"

    for i in range(max(1, retries + 1)):
        try:
            req = Request(url, headers={"User-Agent": "paper_hub/1.0"})
            with urlopen(req, timeout=timeout, context=_SSL_CONTEXT) as resp:
                content_type = (resp.headers.get("Content-Type") or "").lower()
                body = resp.read()
            if body[:4] != b"%PDF" and "pdf" not in content_type:
                return False, "not_pdf"
            output_pdf.parent.mkdir(parents=True, exist_ok=True)
            output_pdf.write_bytes(body)
            return True, ""
        except Exception as e:
            if i < retries:
                time.sleep(1.1 * (2 ** i))
            else:
                return False, str(e)

    return False, "download_failed"


def _classify_download_error(message: str) -> str:
    m = (message or "").lower()
    if not m:
        return "unknown"
    if "429" in m or "rate" in m:
        return "rate_limit"
    if "403" in m or "forbidden" in m:
        return "forbidden"
    if "timed out" in m or "timeout" in m:
        return "timeout"
    if "pmc" in m and "no" in m:
        return "pmc_not_found"
    if "not_pdf" in m:
        return "not_pdf"
    if "download" in m and "failed" in m:
        return "download_failed"
    if "provide direct" in m or "paywall" in m:
        return "paywall_or_no_direct_access"
    return "other_error"


def _sanitize_uid(uid: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", uid)[:120]


def _download_one_record(
    record: Dict[str, Any],
    output_dir: Path,
    api_keys_file: str | None,
    oa_only: bool,
    retries: int,
    timeout: int,
) -> Dict[str, Any]:
    uid = str(record.get("uid") or "")
    title = str(record.get("title") or "")
    doi = str(record.get("doi") or "").strip()
    pmid = str(record.get("pmid") or "").strip()
    pmcid = str(record.get("pmcid") or "").strip()
    url = str(record.get("url") or "").strip()
    source = str(record.get("source") or "")
    open_access_flag = bool(record.get("open_access_flag"))
    oa_locations = record.get("oa_locations", [])
    if not isinstance(oa_locations, list):
        oa_locations = []

    if not uid:
        uid = _make_uid(doi, pmid, title, record.get("year"), source)

    base = output_dir / _sanitize_uid(uid) / "paper"
    base.parent.mkdir(parents=True, exist_ok=True)

    result = {
        "uid": uid,
        "title": title,
        "doi": doi,
        "pmid": pmid,
        "pmcid": pmcid,
        "status": "failed",
        "channel": "",
        "local_path": "",
        "error_code": "",
        "error_message": "",
        "reason_not_downloaded": "",
    }

    if oa_only and not open_access_flag and not pmcid:
        if doi or pmid:
            looked = _lookup_pmcid(doi=doi, pmid=pmid, retries=retries)
            if looked:
                pmcid = looked
                result["pmcid"] = pmcid
            else:
                result.update(
                    {
                        "error_code": "oa_only_filtered",
                        "error_message": "oa-only enabled and no OA path (pmcid/open_access_flag) found",
                        "reason_not_downloaded": "oa_only_filtered",
                    }
                )
                return result
        else:
            result.update(
                {
                    "error_code": "oa_only_filtered",
                    "error_message": "oa-only enabled and record has no doi/pmcid",
                    "reason_not_downloaded": "oa_only_filtered",
                }
            )
            return result

    # 1) DOI via paperscraper
    if doi and not (oa_only and not open_access_flag and not pmcid):
        ok, data, err = _download_doi_internal(doi, base, api_keys_file, retries=retries)
        if ok:
            pdf = str(data.get("pdf", ""))
            xml = str(data.get("xml", ""))
            local = ""
            if pdf and Path(pdf).exists():
                local = pdf
            elif xml and Path(xml).exists():
                local = xml
            result.update(
                {
                    "status": "success",
                    "channel": "doi_paperscraper",
                    "local_path": local,
                    "error_code": "",
                    "error_message": "",
                    "reason_not_downloaded": "",
                }
            )
            return result
        doi_err = err
    else:
        doi_err = "doi_skipped"

    # 2) PMCID fallback
    if not pmcid and (doi or pmid):
        pmcid = _lookup_pmcid(doi=doi, pmid=pmid, retries=retries)
        if pmcid:
            result["pmcid"] = pmcid

    if pmcid:
        ok, local, err = _download_pmc_bioc(pmcid, base, timeout=timeout, retries=retries)
        if ok:
            result.update(
                {
                    "status": "success",
                    "channel": "pmc_bioc",
                    "local_path": local,
                    "error_code": "",
                    "error_message": "",
                    "reason_not_downloaded": "",
                }
            )
            return result
        pmc_err = err
    else:
        pmc_err = "pmcid_not_found"

    # 3) direct URL fallback (OA locations + canonical URL)
    candidate_urls: List[str] = []
    seen = set()
    for u in oa_locations:
        uu = str(u or "").strip()
        if uu and uu not in seen:
            seen.add(uu)
            candidate_urls.append(uu)
    if url and url not in seen:
        candidate_urls.append(url)

    if candidate_urls and (not oa_only or open_access_flag or bool(pmcid)):
        url_err = "download_failed"
        for i, u in enumerate(candidate_urls):
            target_pdf = base.with_suffix(".pdf") if i == 0 else base.with_name(f"paper_{i}").with_suffix(".pdf")
            ok, err = _download_direct_url(
                u,
                target_pdf,
                timeout=timeout,
                retries=max(0, retries - 1),
            )
            if ok:
                result.update(
                    {
                        "status": "success",
                        "channel": "direct_url",
                        "local_path": str(target_pdf),
                        "error_code": "",
                        "error_message": "",
                        "reason_not_downloaded": "",
                    }
                )
                return result
            url_err = err
    else:
        url_err = "url_skipped"

    msg = f"doi:{doi_err}; pmc:{pmc_err}; url:{url_err}"
    result.update(
        {
            "error_code": _classify_download_error(msg),
            "error_message": msg,
            "reason_not_downloaded": _classify_download_error(msg),
        }
    )
    return result


# ---------------------------------------------------------------------------
# parse modes
# ---------------------------------------------------------------------------

def _parse_bioc(path: Path) -> Dict[str, Any]:
    import xml.etree.ElementTree as ET

    root = ET.fromstring(path.read_text(encoding="utf-8", errors="ignore"))
    passages = root.findall(".//passage")

    def _first_infon(key: str) -> str:
        for p in passages:
            for inf in p.findall("infon"):
                if inf.attrib.get("key") == key and (inf.text or "").strip():
                    return (inf.text or "").strip()
        return ""

    title = ""
    abstract_chunks: List[str] = []
    sections: List[Dict[str, str]] = []

    for p in passages:
        sec_type = ""
        kind = ""
        for inf in p.findall("infon"):
            if inf.attrib.get("key") == "section_type":
                sec_type = (inf.text or "").strip()
            if inf.attrib.get("key") == "type":
                kind = (inf.text or "").strip()

        text = " ".join((p.findtext("text") or "").split())
        if not text:
            continue

        if sec_type == "TITLE" and not title:
            title = text
        if sec_type == "ABSTRACT":
            abstract_chunks.append(text)

        sections.append(
            {
                "section_type": sec_type,
                "type": kind,
                "text": text,
            }
        )

    out = {
        "full_title": title,
        "abstract": "\n".join(abstract_chunks),
        "journal": "",
        "pmid": _first_infon("article-id_pmid"),
        "pmc": _first_infon("article-id_pmc"),
        "doi": _first_infon("article-id_doi"),
        "publisher_id": _first_infon("article-id_publisher-id"),
        "publication_year": _first_infon("year"),
        "sections": sections,
        "section_count": len(sections),
    }
    return out


def _compute_content_level(record: Dict[str, Any], download_row: Dict[str, Any]) -> str:
    status = str(download_row.get("status", "") or "")
    if status == "success":
        return "fulltext"
    abstract = _clean_text(record.get("abstract", ""))
    if abstract:
        return "abstract"
    return "metadata"


def _next_step_recommendation(record: Dict[str, Any], download_row: Dict[str, Any]) -> str:
    if str(download_row.get("status", "") or "") == "success":
        return "parsed_or_ready_for_extraction"
    if not record.get("doi") and not record.get("pmid"):
        return "find_doi_or_pmid"
    if record.get("pmcid"):
        return "retry_pmc_fetch"
    if record.get("open_access_flag"):
        return "retry_oa_location_or_direct_url"
    if _clean_text(record.get("abstract", "")):
        return "metadata_and_abstract_available; consider institution access"
    return "run_author_recovery_or_registry_backfill"


def _write_access_audit(
    records: List[Dict[str, Any]],
    downloads: Dict[str, Dict[str, Any]],
    output_csv: Path,
) -> Dict[str, Any]:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "uid",
        "title",
        "doi",
        "pmid",
        "pmcid",
        "source",
        "dimension_id",
        "dimension_version",
        "definition_source",
        "value_source",
        "source_tier",
        "institution_tier",
        "country_group",
        "credibility_score",
        "credibility_tier",
        "quality_gate",
        "rejection_reason",
        "open_access_flag",
        "download_status",
        "channel",
        "content_level",
        "reason_not_downloaded",
        "reason_abstract_missing",
        "reason_not_parsed",
        "next_step",
    ]

    counts = {"fulltext": 0, "abstract": 0, "metadata": 0}
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            uid = str(rec.get("uid", "") or "")
            drow = downloads.get(uid, {})
            content_level = _compute_content_level(rec, drow)
            counts[content_level] = counts.get(content_level, 0) + 1
            writer.writerow(
                {
                    "uid": uid,
                    "title": rec.get("title", ""),
                    "doi": rec.get("doi", ""),
                    "pmid": rec.get("pmid", ""),
                    "pmcid": rec.get("pmcid", ""),
                    "source": rec.get("source", ""),
                    "dimension_id": rec.get("dimension_id", ""),
                    "dimension_version": rec.get("dimension_version", ""),
                    "definition_source": rec.get("definition_source", ""),
                    "value_source": rec.get("value_source", ""),
                    "source_tier": rec.get("source_tier", ""),
                    "institution_tier": rec.get("institution_tier", ""),
                    "country_group": rec.get("country_group", ""),
                    "credibility_score": rec.get("credibility_score", ""),
                    "credibility_tier": rec.get("credibility_tier", ""),
                    "quality_gate": rec.get("quality_gate", ""),
                    "rejection_reason": rec.get("rejection_reason", ""),
                    "open_access_flag": bool(rec.get("open_access_flag")),
                    "download_status": drow.get("status", "failed"),
                    "channel": drow.get("channel", ""),
                    "content_level": content_level,
                    "reason_not_downloaded": drow.get("reason_not_downloaded") or drow.get("error_code", ""),
                    "reason_abstract_missing": rec.get("reason_abstract_missing", ""),
                    "reason_not_parsed": rec.get("reason_not_parsed", ""),
                    "next_step": _next_step_recommendation(rec, drow),
                }
            )
    return counts


def _write_quality_scoring_csv(records: List[Dict[str, Any]], output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "uid",
        "title",
        "source",
        "year",
        "doi",
        "pmid",
        "pmcid",
        "dimension_id",
        "dimension_version",
        "definition_source",
        "value_source",
        "source_tier",
        "institution_tier",
        "country_group",
        "journal",
        "discipline_profile",
        "source_cred",
        "journal_cred",
        "citation_cred",
        "design_cred",
        "integrity_cred",
        "quality_penalty",
        "quality_penalty_reasons",
        "credibility_score",
        "credibility_tier",
        "quality_gate",
        "rejection_reason",
        "journal_tier",
        "citation_age_years",
        "citation_age_adjusted",
        "cited_by_count",
        "preprint_flag",
        "retracted_flag",
        "institution_signal",
    ]
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            writer.writerow({k: rec.get(k, "") for k in fieldnames})


def _write_author_recovery_queue(
    records: List[Dict[str, Any]],
    downloads: Dict[str, Dict[str, Any]],
    output_csv: Path,
) -> int:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "uid",
        "title",
        "doi",
        "pmid",
        "pmcid",
        "source",
        "reason",
        "suggested_action",
    ]
    queued = 0
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            uid = str(rec.get("uid", "") or "")
            drow = downloads.get(uid, {})
            status = str(drow.get("status", "") or "")
            if status == "success":
                continue
            doi = str(rec.get("doi", "") or "").strip()
            pmid = str(rec.get("pmid", "") or "").strip()
            if not doi and not pmid:
                continue
            reason = drow.get("reason_not_downloaded") or drow.get("error_code") or "not_downloaded"
            writer.writerow(
                {
                    "uid": uid,
                    "title": rec.get("title", ""),
                    "doi": doi,
                    "pmid": pmid,
                    "pmcid": rec.get("pmcid", ""),
                    "source": rec.get("source", ""),
                    "reason": reason,
                    "suggested_action": "collect_author_copy_or_institutional_repository_version",
                }
            )
            queued += 1
    return queued


# ---------------------------------------------------------------------------
# commands
# ---------------------------------------------------------------------------

def cmd_search(args: argparse.Namespace) -> None:
    if args.source == "pubmed":
        papers = _search_pubmed_adapter(
            query=args.query,
            max_results=args.max_results,
            retstart=args.retstart,
            date_from=args.date_from,
            date_to=args.date_to,
            retries=args.retry,
        )
    else:
        if args.retstart:
            print("warning: --retstart currently only applies to --source pubmed", file=sys.stderr)
        papers = _search_mcp_source(
            source=args.source,
            query=args.query,
            max_results=args.max_results,
            year=args.year,
            retries=args.retry,
        )

    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(papers, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.raw:
        _print_json(papers)
        return

    if not papers:
        print("No results.")
        return

    for idx, paper in enumerate(papers, start=1):
        if args.source == "pubmed":
            pid = paper.get("pubmed_id", "")
            title = str(paper.get("title", "")).replace("\n", " ").strip()
            title = title[:160] + ("..." if len(title) > 160 else "")
            print(f"{idx}. [{pid}] {title}")
            print(f"   source=pubmed date={paper.get('date','')} doi={paper.get('doi','')}")
            if pid:
                print(f"   url=https://pubmed.ncbi.nlm.nih.gov/{pid}/")
            else:
                print("   url=")
            continue

        title = str(paper.get("title", "")).replace("\n", " ").strip()
        title = title[:160] + ("..." if len(title) > 160 else "")
        print(f"{idx}. [{paper.get('paper_id','')}] {title}")
        print(f"   source={paper.get('source','')} date={paper.get('published_date','')} doi={paper.get('doi','')}")
        print(f"   url={paper.get('url','')}")


def cmd_search_multi(args: argparse.Namespace) -> None:
    base_queries: List[str] = []

    if args.query:
        for q in args.query:
            qq = q.strip()
            if qq:
                base_queries.append(qq)

    if args.queries_file:
        base_queries.extend(_read_queries_file(args.queries_file))

    if not base_queries:
        _die("search-multi requires at least one --query or --queries-file")

    sources = _split_sources(args.sources)
    expanded_queries = _expand_queries(base_queries, strategy=args.strategy)

    jobs: List[Tuple[str, str]] = []
    for q in expanded_queries:
        for s in sources:
            jobs.append((s, q))

    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []

    with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as ex:
        fut_map = {
            ex.submit(
                _search_source_job,
                s,
                q,
                args.retmax_per_query,
                args.strategy,
                args.year,
                args.date_from,
                args.date_to,
                args.retry,
            ): (s, q)
            for s, q in jobs
        }
        for fut in as_completed(fut_map):
            source, query = fut_map[fut]
            try:
                s, q, out, err = fut.result()
            except Exception as e:
                errors.append({"source": source, "query": query, "error": str(e)})
                continue
            rows.extend(out)
            if err:
                errors.append({"source": s, "query": q, "error": err})

    deduped = _dedupe_normalized(rows)
    _enrich_with_pmcid(deduped, limit=args.pmc_lookup_limit, retries=args.retry)

    # recompute open-access flags after PMCID enrichment
    for rec in deduped:
        rec["open_access_flag"] = _is_open_access_hint(
            source=str(rec.get("source", "")),
            url=str(rec.get("url", "")),
            pdf_url="",
            pmcid=str(rec.get("pmcid", "")),
        )

    output_path = (
        Path(args.output_jsonl).expanduser().resolve()
        if args.output_jsonl
        else (ROOT / "downloads" / "candidate_papers_enriched.jsonl")
    )
    _write_jsonl(output_path, deduped)

    if args.raw:
        _print_json(deduped)
        return

    summary = {
        "base_queries": len(base_queries),
        "expanded_queries": len(expanded_queries),
        "sources": sources,
        "raw_candidates": len(rows),
        "deduped_candidates": len(deduped),
        "errors": len(errors),
        "output_jsonl": str(output_path),
    }
    _print_json(summary)

    if errors and args.error_log:
        err_path = Path(args.error_log).expanduser().resolve()
        err_path.parent.mkdir(parents=True, exist_ok=True)
        err_path.write_text(json.dumps(errors, ensure_ascii=False, indent=2), encoding="utf-8")


def cmd_legal_max(args: argparse.Namespace) -> None:
    base_queries: List[str] = []
    if args.query:
        base_queries.extend([q.strip() for q in args.query if q.strip()])
    if args.queries_file:
        base_queries.extend(_read_queries_file(args.queries_file))
    if not base_queries:
        _die("legal-max requires at least one --query or --queries-file")

    sources = _split_legal_sources(args.sources)
    expanded_queries = _expand_queries(base_queries, strategy=args.strategy)
    jobs = [(s, q) for q in expanded_queries for s in sources]

    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []

    with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as ex:
        fut_map = {
            ex.submit(
                _search_legal_source_job,
                s,
                q,
                args.retmax_per_query,
                args.strategy,
                args.year,
                args.date_from,
                args.date_to,
                args.retry,
                args.core_api_key,
            ): (s, q)
            for s, q in jobs
        }
        for fut in as_completed(fut_map):
            source, query = fut_map[fut]
            try:
                s, q, out, err = fut.result()
            except Exception as e:  # noqa: BLE001
                errors.append({"source": source, "query": query, "error": str(e)})
                continue
            rows.extend(out)
            if err:
                errors.append({"source": s, "query": q, "error": err})

    deduped = _dedupe_normalized(rows)
    abstract_before = sum(1 for r in deduped if _clean_text(r.get("abstract", "")))

    _enrich_with_pmcid(deduped, limit=args.pmc_lookup_limit, retries=args.retry)
    _enrich_oa_locations(
        deduped,
        email=args.unpaywall_email,
        limit=args.unpaywall_lookup_limit,
        retries=args.retry,
    )
    _backfill_abstracts(deduped, max_workers=max(1, args.max_workers), retries=args.retry)
    abstract_after = sum(1 for r in deduped if _clean_text(r.get("abstract", "")))

    for rec in deduped:
        rec["open_access_flag"] = bool(rec.get("open_access_flag")) or bool(rec.get("pmcid")) or bool(rec.get("oa_locations"))
        rec["relevance_score"] = _relevance_score(rec, args.strategy)
        rec["reason_not_parsed"] = "" if _clean_text(rec.get("abstract", "")) else "missing_abstract"
        rec["content_level"] = "abstract" if _clean_text(rec.get("abstract", "")) else "metadata"

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    source_registry_entries = _ensure_source_registry(DEFAULT_SOURCE_REGISTRY_PATH)
    source_map, institution_rows = _index_source_registry(source_registry_entries)
    dimensions_catalog = _ensure_dimensions_catalog(DEFAULT_DIMENSIONS_CATALOG_PATH, run_id=run_id)
    catalog_by_dimension = {
        str(x.get("dimension_id", "")): x
        for x in dimensions_catalog.get("dimensions", [])
        if isinstance(x, dict) and x.get("dimension_id")
    }

    for rec in deduped:
        _annotate_record_source_and_dimension(
            rec,
            source_map=source_map,
            institution_rows=institution_rows,
            catalog_by_dimension=catalog_by_dimension,
        )

    dimensions_catalog, dimension_changelog, catalog_by_dimension = _update_dimensions_catalog(
        DEFAULT_DIMENSIONS_CATALOG_PATH,
        dimensions_catalog,
        deduped,
        run_id=run_id,
    )
    for rec in deduped:
        dim_meta = catalog_by_dimension.get(str(rec.get("dimension_id", "")), {})
        rec["definition_source"] = str(dim_meta.get("definition_source", rec.get("definition_source", "")))

    quality_filter = str(args.quality_filter or "on").strip().lower()
    if quality_filter not in {"on", "off"}:
        quality_filter = "on"
    preprint_policy = str(args.preprint_policy or "separate_sheet").strip().lower()
    if preprint_policy not in {"separate_sheet", "allow_core"}:
        preprint_policy = "separate_sheet"

    quality_summary = _apply_quality_scoring(
        deduped,
        quality_filter=quality_filter,
        core_threshold=max(0, min(100, int(args.quality_core_threshold))),
        extended_threshold=max(0, min(100, int(args.quality_extended_threshold))),
        citation_age_window=max(1, int(args.citation_age_window)),
        preprint_policy=preprint_policy,
    )

    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    quality_guard_baseline_path = output_dir / "quality_guard_baseline.json"
    quality_guard_after_path = output_dir / "quality_guard_after.json"
    quality_guard_diff_path = output_dir / "quality_guard_diff.json"

    if quality_guard_baseline_path.exists():
        baseline_guard = _yaml_load(quality_guard_baseline_path, {})
    else:
        baseline_guard = _quality_guard_metrics(deduped)
        quality_guard_baseline_path.write_text(
            json.dumps(baseline_guard, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    current_guard = _quality_guard_metrics(deduped)
    guard_diff = _quality_guard_diff(baseline_guard, current_guard)

    quality_guard_after_path.write_text(
        json.dumps(current_guard, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    quality_guard_diff_path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "baseline": baseline_guard,
                "current": current_guard,
                "diff": guard_diff,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    if not bool(guard_diff.get("quality_guard_pass")):
        for rec in deduped:
            if str(rec.get("quality_gate", "")) == "core_pass":
                rec["quality_gate"] = "extended_review"
                reason = str(rec.get("rejection_reason", "") or "")
                suffix = "quality_guard_holdout"
                rec["rejection_reason"] = f"{reason},{suffix}".strip(",")
        quality_summary = {
            "core_pass": sum(1 for r in deduped if r.get("quality_gate") == "core_pass"),
            "extended_review": sum(1 for r in deduped if r.get("quality_gate") == "extended_review"),
            "reject": sum(1 for r in deduped if r.get("quality_gate") == "reject"),
            "preprint_extended": sum(1 for r in deduped if r.get("quality_gate") == "preprint_extended"),
        }

    core_records = [r for r in deduped if r.get("quality_gate") == "core_pass"]
    extended_records = [r for r in deduped if r.get("quality_gate") == "extended_review"]
    preprint_records = [r for r in deduped if r.get("quality_gate") == "preprint_extended"]
    rejected_records = [r for r in deduped if r.get("quality_gate") == "reject"]
    download_candidates = [r for r in deduped if r.get("quality_gate") != "reject"]

    candidate_path = output_dir / "candidate_papers_enriched.jsonl"
    _write_jsonl(candidate_path, deduped)
    core_path = output_dir / "core_records.jsonl"
    extended_path = output_dir / "extended_records.jsonl"
    preprint_path = output_dir / "preprint_extended_records.jsonl"
    rejected_path = output_dir / "rejected_records.jsonl"
    quality_csv_path = output_dir / "quality_scoring.csv"
    dimension_changelog_path = output_dir / "dimension_changelog.csv"
    institution_provenance_path = output_dir / "institution_provenance.csv"

    _write_jsonl(core_path, core_records)
    _write_jsonl(extended_path, extended_records)
    _write_jsonl(preprint_path, preprint_records)
    _write_jsonl(rejected_path, rejected_records)
    _write_quality_scoring_csv(deduped, quality_csv_path)
    _write_csv_rows(
        dimension_changelog_path,
        dimension_changelog,
        [
            "run_id",
            "dimension_id",
            "action",
            "old_status",
            "new_status",
            "reason",
            "source_types",
            "source_tiers",
            "count",
        ],
    )
    _write_csv_rows(
        institution_provenance_path,
        _build_institution_provenance_rows(deduped),
        [
            "uid",
            "title",
            "source",
            "value_source",
            "source_tier",
            "source_type_class",
            "institution_name",
            "institution_tier",
            "country",
            "country_group",
            "quality_gate",
        ],
    )

    download_rows: List[Dict[str, Any]] = []
    manifest_path = output_dir / "download_manifest.csv"
    if args.skip_download:
        for rec in deduped:
            gate = str(rec.get("quality_gate", "") or "")
            if gate == "reject":
                status = "filtered_out"
                reason = str(rec.get("rejection_reason", "") or "quality_reject")
            else:
                status = "skipped"
                reason = "download_skipped"
            download_rows.append(
                {
                    "uid": rec.get("uid", ""),
                    "title": rec.get("title", ""),
                    "doi": rec.get("doi", ""),
                    "pmid": rec.get("pmid", ""),
                    "pmcid": rec.get("pmcid", ""),
                    "status": status,
                    "channel": "",
                    "local_path": "",
                    "error_code": "",
                    "error_message": "",
                    "reason_not_downloaded": reason,
                }
            )
    else:
        fulltext_dir = output_dir / "fulltext"
        fulltext_dir.mkdir(parents=True, exist_ok=True)
        with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as ex:
            fut_map = {
                ex.submit(
                    _download_one_record,
                    rec,
                    fulltext_dir,
                    args.api_keys_file,
                    True,  # legal-max is OA-first
                    args.retry,
                    args.timeout,
                ): rec
                for rec in download_candidates
            }
            for fut in as_completed(fut_map):
                rec = fut_map[fut]
                try:
                    row = fut.result()
                except Exception as e:  # noqa: BLE001
                    row = {
                        "uid": rec.get("uid", ""),
                        "title": rec.get("title", ""),
                        "doi": rec.get("doi", ""),
                        "pmid": rec.get("pmid", ""),
                        "pmcid": rec.get("pmcid", ""),
                        "status": "failed",
                        "channel": "",
                        "local_path": "",
                        "error_code": "other_error",
                        "error_message": str(e),
                        "reason_not_downloaded": "other_error",
                    }
                download_rows.append(row)

        for rec in rejected_records:
            download_rows.append(
                {
                    "uid": rec.get("uid", ""),
                    "title": rec.get("title", ""),
                    "doi": rec.get("doi", ""),
                    "pmid": rec.get("pmid", ""),
                    "pmcid": rec.get("pmcid", ""),
                    "status": "filtered_out",
                    "channel": "",
                    "local_path": "",
                    "error_code": "quality_reject",
                    "error_message": str(rec.get("rejection_reason", "") or "quality_reject"),
                    "reason_not_downloaded": str(rec.get("rejection_reason", "") or "quality_reject"),
                }
            )

    status_rank = {"success": 0, "skipped": 1, "failed": 2, "filtered_out": 3}
    download_rows.sort(key=lambda x: (status_rank.get(str(x.get("status", "")), 99), x.get("uid", "")))
    manifest_fields = [
        "uid",
        "title",
        "doi",
        "pmid",
        "pmcid",
        "status",
        "channel",
        "local_path",
        "error_code",
        "error_message",
        "reason_not_downloaded",
    ]
    with manifest_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=manifest_fields)
        writer.writeheader()
        for row in download_rows:
            writer.writerow({k: row.get(k, "") for k in manifest_fields})

    download_map = {str(r.get("uid", "")): r for r in download_rows}
    access_audit_path = output_dir / "access_audit.csv"
    content_counts = _write_access_audit(deduped, download_map, access_audit_path)

    author_queue_path = output_dir / "author_recovery_queue.csv"
    queued = _write_author_recovery_queue(download_candidates, download_map, author_queue_path)

    error_log_path = None
    if errors:
        error_log_path = output_dir / "source_errors.json"
        error_log_path.write_text(json.dumps(errors, ensure_ascii=False, indent=2), encoding="utf-8")

    summary = {
        "queries": len(base_queries),
        "expanded_queries": len(expanded_queries),
        "sources": sources,
        "raw_candidates": len(rows),
        "deduped_candidates": len(deduped),
        "abstract_before": abstract_before,
        "abstract_after": abstract_after,
        "abstract_coverage_delta": abstract_after - abstract_before,
        "download_success": sum(1 for r in download_rows if r.get("status") == "success"),
        "download_failed": sum(1 for r in download_rows if r.get("status") == "failed"),
        "download_filtered_out": sum(1 for r in download_rows if r.get("status") == "filtered_out"),
        "quality_filter": quality_filter,
        "quality_core_threshold": int(args.quality_core_threshold),
        "quality_extended_threshold": int(args.quality_extended_threshold),
        "citation_age_window": int(args.citation_age_window),
        "preprint_policy": preprint_policy,
        "quality_counts": quality_summary,
        "quality_guard_pass": bool(guard_diff.get("quality_guard_pass")),
        "core_records": len(core_records),
        "extended_records": len(extended_records),
        "preprint_records": len(preprint_records),
        "rejected_records": len(rejected_records),
        "content_levels": content_counts,
        "author_recovery_queue": queued,
        "candidate_papers_enriched_jsonl": str(candidate_path),
        "core_records_jsonl": str(core_path),
        "extended_records_jsonl": str(extended_path),
        "preprint_records_jsonl": str(preprint_path),
        "rejected_records_jsonl": str(rejected_path),
        "quality_scoring_csv": str(quality_csv_path),
        "dimension_changelog_csv": str(dimension_changelog_path),
        "institution_provenance_csv": str(institution_provenance_path),
        "quality_guard_baseline_json": str(quality_guard_baseline_path),
        "quality_guard_after_json": str(quality_guard_after_path),
        "quality_guard_diff_json": str(quality_guard_diff_path),
        "dimensions_catalog_yaml": str(DEFAULT_DIMENSIONS_CATALOG_PATH),
        "source_registry_yaml": str(DEFAULT_SOURCE_REGISTRY_PATH),
        "download_manifest_csv": str(manifest_path),
        "access_audit_csv": str(access_audit_path),
        "author_recovery_queue_csv": str(author_queue_path),
        "source_error_log": str(error_log_path) if error_log_path else "",
    }
    _print_json(summary)


def cmd_download_id(args: argparse.Namespace) -> None:
    save_dir = Path(args.save_dir).expanduser().resolve()
    save_dir.mkdir(parents=True, exist_ok=True)

    code = r'''
import json
import sys

from paper_search_mcp.academic_platforms.arxiv import ArxivSearcher
from paper_search_mcp.academic_platforms.biorxiv import BioRxivSearcher
from paper_search_mcp.academic_platforms.crossref import CrossRefSearcher
from paper_search_mcp.academic_platforms.google_scholar import GoogleScholarSearcher
from paper_search_mcp.academic_platforms.iacr import IACRSearcher
from paper_search_mcp.academic_platforms.medrxiv import MedRxivSearcher
from paper_search_mcp.academic_platforms.pubmed import PubMedSearcher
from paper_search_mcp.academic_platforms.semantic import SemanticSearcher

source = sys.argv[1]
paper_id = sys.argv[2]
save_path = sys.argv[3]

searchers = {
    "arxiv": ArxivSearcher,
    "pubmed": PubMedSearcher,
    "biorxiv": BioRxivSearcher,
    "medrxiv": MedRxivSearcher,
    "google_scholar": GoogleScholarSearcher,
    "iacr": IACRSearcher,
    "semantic": SemanticSearcher,
    "crossref": CrossRefSearcher,
}

searcher = searchers[source]()
try:
    out = searcher.download_pdf(paper_id, save_path)
    print(json.dumps({"ok": True, "result": out}, ensure_ascii=False))
except Exception as e:
    print(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
'''

    result = _run_python_retry(
        "paper_search_mcp",
        code,
        [args.source, args.paper_id, str(save_dir)],
        context="download-id",
        retries=args.retry,
        timeout=120,
    )
    data = _extract_json_or_die(result, "download-id")
    _print_json(data)


def cmd_download_doi(args: argparse.Namespace) -> None:
    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    ok, data, err = _download_doi_internal(
        doi=args.doi,
        output_base=output_path,
        api_keys=args.api_keys,
        retries=args.retry,
    )

    payload = data if isinstance(data, dict) else {}
    payload.setdefault("ok", ok)
    payload.setdefault("pdf", str(output_path.with_suffix(".pdf")))
    payload.setdefault("xml", str(output_path.with_suffix(".xml")))
    if err and "error" not in payload:
        payload["error"] = err
    _print_json(payload)


def cmd_download_batch(args: argparse.Namespace) -> None:
    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        _die(f"Input file not found: {input_path}")

    records = _load_records(input_path)
    out_dir = Path(args.output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    results: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as ex:
        fut_map = {
            ex.submit(
                _download_one_record,
                rec,
                out_dir,
                args.api_keys_file,
                args.oa_only,
                args.retry,
                args.timeout,
            ): rec
            for rec in records
        }
        for fut in as_completed(fut_map):
            rec = fut_map[fut]
            try:
                row = fut.result()
            except Exception as e:
                row = {
                    "uid": rec.get("uid", ""),
                    "title": rec.get("title", ""),
                    "doi": rec.get("doi", ""),
                    "pmid": rec.get("pmid", ""),
                    "pmcid": rec.get("pmcid", ""),
                    "status": "failed",
                    "channel": "",
                    "local_path": "",
                    "error_code": "other_error",
                    "error_message": str(e),
                    "reason_not_downloaded": "other_error",
                }
            results.append(row)

    results.sort(key=lambda x: (x.get("status") != "success", x.get("uid", "")))

    manifest_path = (
        Path(args.manifest_output).expanduser().resolve()
        if args.manifest_output
        else out_dir / "download_manifest.csv"
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "uid",
        "title",
        "doi",
        "pmid",
        "pmcid",
        "status",
        "channel",
        "local_path",
        "error_code",
        "error_message",
        "reason_not_downloaded",
    ]
    with manifest_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            w.writerow({k: r.get(k, "") for k in fieldnames})

    if args.raw:
        _print_json(results)
        return

    success = sum(1 for r in results if r.get("status") == "success")
    summary = {
        "input_records": len(records),
        "downloaded": success,
        "failed": len(results) - success,
        "manifest": str(manifest_path),
    }
    _print_json(summary)


def cmd_parse(args: argparse.Namespace) -> None:
    input_path = Path(args.path).expanduser().resolve()
    if not input_path.exists():
        _die(f"Input file not found: {input_path}")

    if args.mode == "bioc":
        data = _parse_bioc(input_path)
    else:
        code = r'''
import json
import sys

import pubmed_parser as pp

mode = sys.argv[1]
path = sys.argv[2]

if mode == "oa":
    data = pp.parse_pubmed_xml(path)
elif mode == "refs":
    data = pp.parse_pubmed_references(path)
elif mode == "medline":
    data = list(pp.parse_medline_xml(path))
else:
    raise ValueError(f"Unsupported mode: {mode}")

print(json.dumps(data, ensure_ascii=False, default=str))
'''
        result = _run_python_retry(
            "pubmed_parser",
            code,
            [args.mode, str(input_path)],
            context="parse",
            retries=args.retry,
            timeout=120,
        )
        data = _extract_json_or_die(result, "parse")

    if isinstance(data, list) and args.limit is not None:
        data = data[: args.limit]

    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    _print_json(data)


def cmd_benchmark(args: argparse.Namespace) -> None:
    queries = [
        "pancreatic cancer randomized phase III overall survival progression-free survival",
        "pancreatic cancer ORR DCR objective response randomized trial",
        "pancreatic cancer CTCAE grade 3 adverse events randomized",
        "pancreatic cancer quality of life EQ-5D QLQ-C30 trial",
        "locally advanced pancreatic cancer chemoradiotherapy randomized",
        "metastatic pancreatic cancer FOLFIRINOX gemcitabine phase III",
        "pancreatic cancer treatment-related death safety profile trial",
        "pancreatic cancer QALY cost-effectiveness randomized",
    ]

    out_dir = Path(args.output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    raw_rows: List[Dict[str, Any]] = []

    for q in queries:
        for s in _split_sources(args.sources):
            src, qq, rows, err = _search_source_job(
                source=s,
                query=q,
                retmax=args.retmax_per_query,
                strategy=args.strategy,
                year=args.year,
                date_from=args.date_from,
                date_to=args.date_to,
                retries=args.retry,
            )
            raw_rows.extend(rows)
            if err:
                raw_rows.append(
                    {
                        "uid": f"err:{hashlib.sha1(f'{src}|{qq}|{err}'.encode()).hexdigest()[:10]}",
                        "title": "",
                        "abstract": "",
                        "doi": "",
                        "pmid": "",
                        "pmcid": "",
                        "year": "",
                        "source": src,
                        "url": "",
                        "relevance_score": 0,
                        "open_access_flag": False,
                        "coverage_flags": {},
                        "reason_not_parsed": err,
                        "matched_query": qq,
                    }
                )

    deduped = _dedupe_normalized(raw_rows)

    baseline_json = out_dir / f"baseline_{ts}.json"
    baseline_json.write_text(json.dumps(deduped, ensure_ascii=False, indent=2), encoding="utf-8")

    metrics = {
        "timestamp": ts,
        "queries": len(queries),
        "sources": _split_sources(args.sources),
        "raw_candidates": len(raw_rows),
        "deduped_candidates": len(deduped),
        "output": str(baseline_json),
    }
    metrics_path = out_dir / f"baseline_metrics_{ts}.json"
    metrics_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")

    _print_json(metrics)


# ---------------------------------------------------------------------------
# parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="paper_hub",
        description="Unified CLI wrapper for pubmed_parser, paper-search-mcp and paperscraper.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_search = sub.add_parser("search", help="Search papers via source backends (pubmed via paperscraper adapter).")
    p_search.add_argument("--source", choices=SEARCH_SOURCES, required=True)
    p_search.add_argument("--query", required=True)
    p_search.add_argument("--max-results", type=int, default=50, help="Default raised to 50 for better recall.")
    p_search.add_argument("--retstart", type=int, default=0, help="PubMed pagination offset (source=pubmed only).")
    p_search.add_argument("--date-from", default=None, help="Optional publication date lower bound (YYYY or YYYY-MM-DD).")
    p_search.add_argument("--date-to", default=None, help="Optional publication date upper bound (YYYY or YYYY-MM-DD).")
    p_search.add_argument("--year", default=None, help="Only used for semantic source.")
    p_search.add_argument("--retry", type=int, default=2, help="Retry count for transient errors.")
    p_search.add_argument("--output", default=None, help="Optional path to save raw JSON results.")
    p_search.add_argument("--raw", action="store_true", help="Print raw JSON instead of concise list output.")
    p_search.set_defaults(func=cmd_search)

    p_search_multi = sub.add_parser("search-multi", help="Run multi-query/multi-source retrieval and output normalized JSONL.")
    p_search_multi.add_argument("--query", action="append", default=[], help="Repeatable base query.")
    p_search_multi.add_argument("--queries-file", default=None, help="Path to newline-delimited queries file.")
    p_search_multi.add_argument("--sources", default=",".join(SOURCE_DEFAULTS), help="Comma-separated sources.")
    p_search_multi.add_argument("--retmax-per-query", type=int, default=50)
    p_search_multi.add_argument("--strategy", choices=("recall", "balance", "precision"), default="recall")
    p_search_multi.add_argument("--date-from", default=None)
    p_search_multi.add_argument("--date-to", default=None)
    p_search_multi.add_argument("--year", default=None, help="Semantic year filter.")
    p_search_multi.add_argument("--max-workers", type=int, default=6)
    p_search_multi.add_argument("--retry", type=int, default=2)
    p_search_multi.add_argument("--pmc-lookup-limit", type=int, default=200)
    p_search_multi.add_argument("--output-jsonl", default=None, help="Default: downloads/candidate_papers_enriched.jsonl")
    p_search_multi.add_argument("--error-log", default=None, help="Optional JSON file for per-job errors.")
    p_search_multi.add_argument("--raw", action="store_true")
    p_search_multi.set_defaults(func=cmd_search_multi)

    p_legal = sub.add_parser(
        "legal-max",
        help="OA-first end-to-end pipeline: multi-source retrieval, abstract backfill, download, access audit.",
    )
    p_legal.add_argument("--query", action="append", default=[], help="Repeatable base query.")
    p_legal.add_argument("--queries-file", default=None, help="Path to newline-delimited queries file.")
    p_legal.add_argument("--sources", default=",".join(LEGAL_MAX_DEFAULT_SOURCES), help="Comma-separated legal-max sources.")
    p_legal.add_argument("--retmax-per-query", type=int, default=30)
    p_legal.add_argument("--strategy", choices=("recall", "balance", "precision"), default="recall")
    p_legal.add_argument("--date-from", default=None)
    p_legal.add_argument("--date-to", default=None)
    p_legal.add_argument("--year", default=None, help="Semantic year filter.")
    p_legal.add_argument("--max-workers", type=int, default=6)
    p_legal.add_argument("--retry", type=int, default=2)
    p_legal.add_argument("--pmc-lookup-limit", type=int, default=300)
    p_legal.add_argument("--unpaywall-lookup-limit", type=int, default=400)
    p_legal.add_argument("--unpaywall-email", default="paper-hub@example.org")
    p_legal.add_argument("--core-api-key", default=None, help="Optional CORE API key.")
    p_legal.add_argument("--api-keys-file", default=None, help="Optional paperscraper publisher keys file.")
    p_legal.add_argument("--quality-filter", choices=("on", "off"), default="on")
    p_legal.add_argument("--quality-core-threshold", type=int, default=70)
    p_legal.add_argument("--quality-extended-threshold", type=int, default=50)
    p_legal.add_argument("--citation-age-window", type=int, default=5)
    p_legal.add_argument("--preprint-policy", choices=("separate_sheet", "allow_core"), default="separate_sheet")
    p_legal.add_argument("--timeout", type=int, default=60)
    p_legal.add_argument("--skip-download", action="store_true")
    p_legal.add_argument("--output-dir", default=str(ROOT / "downloads"))
    p_legal.set_defaults(func=cmd_legal_max)

    p_download_id = sub.add_parser(
        "download-id",
        help="Download PDF by source-specific paper id via paper-search-mcp searchers.",
    )
    p_download_id.add_argument("--source", choices=DOWNLOAD_SOURCES, required=True)
    p_download_id.add_argument("--paper-id", required=True)
    p_download_id.add_argument("--save-dir", default=str(ROOT / "downloads"))
    p_download_id.add_argument("--retry", type=int, default=2)
    p_download_id.set_defaults(func=cmd_download_id)

    p_download_doi = sub.add_parser(
        "download-doi",
        help="Download full text by DOI via paperscraper (PDF/XML fallback supported).",
    )
    p_download_doi.add_argument("--doi", required=True)
    p_download_doi.add_argument(
        "--output",
        required=True,
        help="Output path without extension preferred (e.g., ./downloads/my-paper)",
    )
    p_download_doi.add_argument(
        "--api-keys",
        default=None,
        help="Optional path to API key file for publisher fallbacks.",
    )
    p_download_doi.add_argument("--retry", type=int, default=2)
    p_download_doi.set_defaults(func=cmd_download_doi)

    p_download_batch = sub.add_parser(
        "download-batch",
        help="Batch download from normalized records (DOI -> PMCID -> URL fallback).",
    )
    p_download_batch.add_argument("--input", required=True, help="JSON/JSONL file from search-multi.")
    p_download_batch.add_argument("--output-dir", default=str(ROOT / "downloads" / "batch"))
    p_download_batch.add_argument("--api-keys-file", default=None)
    p_download_batch.add_argument("--oa-only", action="store_true", help="Only attempt open-access paths.")
    p_download_batch.add_argument("--max-workers", type=int, default=4)
    p_download_batch.add_argument("--retry", type=int, default=2)
    p_download_batch.add_argument("--timeout", type=int, default=60)
    p_download_batch.add_argument("--manifest-output", default=None)
    p_download_batch.add_argument("--raw", action="store_true")
    p_download_batch.set_defaults(func=cmd_download_batch)

    p_parse = sub.add_parser("parse", help="Parse OA/MEDLINE/REFS via pubmed_parser; parse BioC XML via native parser.")
    p_parse.add_argument("--mode", choices=("oa", "refs", "medline", "bioc"), required=True)
    p_parse.add_argument("--path", required=True)
    p_parse.add_argument("--limit", type=int, default=None, help="Limit list length for refs/medline output.")
    p_parse.add_argument("--output", default=None, help="Optional path to save parsed JSON.")
    p_parse.add_argument("--retry", type=int, default=2)
    p_parse.set_defaults(func=cmd_parse)

    p_bench = sub.add_parser("benchmark", help="Generate baseline benchmark output under downloads/benchmarks.")
    p_bench.add_argument("--output-dir", default=str(ROOT / "downloads" / "benchmarks"))
    p_bench.add_argument("--sources", default=",".join(SOURCE_DEFAULTS))
    p_bench.add_argument("--retmax-per-query", type=int, default=50)
    p_bench.add_argument("--strategy", choices=("recall", "balance", "precision"), default="recall")
    p_bench.add_argument("--date-from", default=None)
    p_bench.add_argument("--date-to", default=None)
    p_bench.add_argument("--year", default=None)
    p_bench.add_argument("--retry", type=int, default=2)
    p_bench.set_defaults(func=cmd_benchmark)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    try:
        args.func(args)
    except RuntimeError as e:
        _die(str(e))


if __name__ == "__main__":
    main()
