"""
Capture golden fixtures from the public cBioPortal site for msk_chord_2024.

Run once manually to generate fixture files:
    uv run python tests/capture_golden.py
    uv run python tests/capture_golden.py --study msk_chord_2024 --baseline-only

Requires Playwright with Chromium:
    uv run playwright install chromium

Strategy
--------
Load the study view page with Playwright and intercept the XHR API responses
that the React app makes.  This gives us structured JSON directly rather than
fragile DOM scraping.

    clinical-data-counts/fetch  → fixture["clinical"][<attributeId>]
    mutated-genes/fetch         → fixture["mutated_genes"]
    structural-variant-genes/fetch → fixture["sv_genes"]
    copy-number-alterations/fetch  → fixture["cna_genes"]

DOM scraping is kept as a fallback for genomic tables if the API capture fails.

Fixture schema
--------------
baseline.json
{
  "scenario": "baseline",
  "study_id": "...",
  "clinical": {
    "<ATTRIBUTE_ID>": [{"value": "...", "count": N}, ...]   # sorted count desc
  },
  "mutated_genes": [{"gene": "...", "n_mut": N, "n_samples": N, "freq": F}, ...],
  "cna_genes":     [{"gene": "...", "cna_type": "AMP|HOMDEL", "n_samples": N, "freq": F}, ...],
  "sv_genes":      [{"gene": "...", "n_sv": N, "n_samples": N, "freq": F}, ...]
}
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

from playwright.sync_api import sync_playwright, Page, Response

STUDY_ID = "msk_chord_2024"
BASE_URL = "https://www.cbioportal.org"
STUDY_URL = f"{BASE_URL}/study/summary?id={STUDY_ID}"
FIXTURES_DIR = Path(__file__).parent / "fixtures"

LOAD_TIMEOUT_MS = 120_000
SETTLE_MS = 30_000  # wait after networkidle for React rendering


# ---------------------------------------------------------------------------
# Network interception helpers
# ---------------------------------------------------------------------------

def _safe_json(response: Response) -> Any:
    try:
        return response.json()
    except Exception:
        return None


def _freq_pct(val: float | None) -> float:
    """Normalise a raw freq value to a percentage (0–100)."""
    if val is None:
        return 0.0
    # API returns 0–1 fraction; convert to percentage
    return round(float(val) * 100 if float(val) <= 1.0 else float(val), 1)


def _norm_clinical(counts: list[dict]) -> list[dict]:
    return [{"value": r["value"], "count": r["count"]}
            for r in sorted(counts, key=lambda x: -x.get("count", 0))]


def _norm_mutated(rows: list[dict]) -> list[dict]:
    """Normalise mutated-genes API response.

    Public API fields:
      numberOfAlteredCases → n_samples
      totalCount           → n_mut
      numberOfProfiledCases → n_profiled (used to compute freq)
    """
    out = []
    for r in sorted(rows, key=lambda x: -x.get("numberOfAlteredCases", x.get("uniqueSampleCount", 0))):
        n_samples = r.get("numberOfAlteredCases", r.get("uniqueSampleCount", r.get("sampleCount", 0)))
        n_profiled = r.get("numberOfProfiledCases", 0)
        freq = round(n_samples / n_profiled * 100, 1) if n_profiled else 0.0
        out.append({
            "gene": r.get("hugoGeneSymbol", r.get("gene", "")),
            "n_mut": r.get("totalCount", r.get("mutationCount", 0)),
            "n_samples": n_samples,
            "freq": freq,
        })
    return out


def _norm_cna(rows: list[dict]) -> list[dict]:
    """Normalise CNA genes API response.

    Public API fields (column-store/cna-genes/fetch):
      numberOfAlteredCases → n_samples
      numberOfProfiledCases → used to compute freq
      alteration: 2 = AMP, -2 = HOMDEL
    """
    out = []
    for r in sorted(rows, key=lambda x: -x.get("numberOfAlteredCases", x.get("uniqueSampleCount", 0))):
        n_samples = r.get("numberOfAlteredCases", r.get("uniqueSampleCount", r.get("sampleCount", 0)))
        n_profiled = r.get("numberOfProfiledCases", 0)
        freq = round(n_samples / n_profiled * 100, 1) if n_profiled else 0.0
        # alteration: 2 = AMP, -2 = HOMDEL; also handle string altType
        alt_int = r.get("alteration", 0)
        alt_str = (r.get("altType") or r.get("alterationType") or "").upper()
        if alt_int == 2 or "AMP" in alt_str:
            cna_type = "AMP"
        else:
            cna_type = "HOMDEL"
        out.append({
            "gene": r.get("hugoGeneSymbol", r.get("gene", "")),
            "cna_type": cna_type,
            "n_samples": n_samples,
            "freq": freq,
        })
    return out


def _norm_sv(rows: list[dict]) -> list[dict]:
    """Normalise SV genes API response."""
    out = []
    for r in sorted(rows, key=lambda x: -x.get("numberOfAlteredCases", x.get("uniqueSampleCount", 0))):
        n_samples = r.get("numberOfAlteredCases", r.get("uniqueSampleCount", r.get("sampleCount", 0)))
        n_profiled = r.get("numberOfProfiledCases", 0)
        freq = round(n_samples / n_profiled * 100, 1) if n_profiled else 0.0
        out.append({
            "gene": r.get("hugoGeneSymbol", r.get("gene", "")),
            "n_sv": r.get("totalCount", r.get("svCount", 0)),
            "n_samples": n_samples,
            "freq": freq,
        })
    return out


# ---------------------------------------------------------------------------
# DOM scraping fallbacks (genomic tables)
# ---------------------------------------------------------------------------

def _parse_int(text: str) -> int:
    try:
        return int(text.strip().replace(",", ""))
    except ValueError:
        return 0


def _parse_freq(text: str) -> float:
    try:
        return float(text.strip().rstrip("%"))
    except ValueError:
        return 0.0


def _text_lines(text: str) -> list[str]:
    return [l.strip() for l in text.splitlines() if l.strip()]


def _skip_to_data(lines: list[str], col_headers: set[str]) -> list[str]:
    last_header_idx = -1
    for idx, line in enumerate(lines):
        if line.lower() in col_headers:
            last_header_idx = idx
    return lines[last_header_idx + 1:]


def _dom_scrape_mutated_genes(page: Page, limit: int = 20) -> list[dict]:
    sel = f"[data-test='chart-container-{STUDY_ID}_mutations']"
    el = page.locator(sel)
    if el.count() == 0:
        return []
    lines = _text_lines(el.inner_text())
    data_lines = _skip_to_data(lines, {"gene", "# mut", "#", "freq"})
    results = []
    i = 0
    while i + 3 < len(data_lines) and len(results) < limit:
        results.append({
            "gene": data_lines[i],
            "n_mut": _parse_int(data_lines[i + 1]),
            "n_samples": _parse_int(data_lines[i + 2]),
            "freq": _parse_freq(data_lines[i + 3]),
        })
        i += 4
    return results


def _dom_scrape_cna_genes(page: Page, limit: int = 20) -> list[dict]:
    sel = f"[data-test='chart-container-{STUDY_ID}_cna']"
    el = page.locator(sel)
    if el.count() == 0:
        return []
    lines = _text_lines(el.inner_text())
    data_lines = _skip_to_data(lines, {"gene", "cytoband", "cna", "#", "freq"})
    results = []
    i = 0
    while i + 4 < len(data_lines) and len(results) < limit:
        alt = data_lines[i + 2].upper()
        cna_type = "AMP" if "AMP" in alt else "HOMDEL"
        results.append({
            "gene": data_lines[i],
            "cna_type": cna_type,
            "n_samples": _parse_int(data_lines[i + 3]),
            "freq": _parse_freq(data_lines[i + 4]),
        })
        i += 5
    return results


def _dom_scrape_sv_genes(page: Page, limit: int = 20) -> list[dict]:
    sel = f"[data-test='chart-container-STRUCTURAL_VARIANT_GENES_TABLE;{STUDY_ID}_structural_variants']"
    el = page.locator(sel)
    if el.count() == 0:
        return []
    lines = _text_lines(el.inner_text())
    data_lines = _skip_to_data(lines, {"gene", "# sv", "#", "freq"})
    results = []
    i = 0
    while i + 3 < len(data_lines) and len(results) < limit:
        freq_str = data_lines[i + 3]
        results.append({
            "gene": data_lines[i],
            "n_sv": _parse_int(data_lines[i + 1]),
            "n_samples": _parse_int(data_lines[i + 2]),
            "freq": _parse_freq(freq_str) if freq_str != "NA" else 0.0,
        })
        i += 4
    return results


# ---------------------------------------------------------------------------
# Scenario capture
# ---------------------------------------------------------------------------

def _load_page_and_collect(page: Page, url: str) -> dict[str, Any]:
    """Navigate to url, wait for all charts to render, collect intercepted API data."""
    collected: dict[str, Any] = {
        "clinical": {},
        "mutated_genes_raw": [],
        "cna_genes_raw": [],
        "sv_genes_raw": [],
    }

    def on_response(response: Response) -> None:
        if response.status != 200:
            return
        rurl = response.url
        if "clinical-data-counts/fetch" in rurl:
            data = _safe_json(response)
            if isinstance(data, list):
                for entry in data:
                    aid = entry.get("attributeId")
                    if aid:
                        counts = _norm_clinical(entry.get("counts", []))
                        # merge: if we already have this attr, keep (first call wins)
                        if aid not in collected["clinical"]:
                            collected["clinical"][aid] = counts
        elif "mutated-genes/fetch" in rurl:
            data = _safe_json(response)
            if isinstance(data, list) and data:
                # keep the biggest result set (unfiltered cohort)
                if len(data) > len(collected["mutated_genes_raw"]):
                    collected["mutated_genes_raw"] = data
        elif "column-store/cna-genes/fetch" in rurl or "copy-number-alterations/fetch" in rurl or "cna-genes/fetch" in rurl:
            data = _safe_json(response)
            if isinstance(data, list) and data:
                if len(data) > len(collected["cna_genes_raw"]):
                    collected["cna_genes_raw"] = data
        elif "structural-variant-genes/fetch" in rurl:
            data = _safe_json(response)
            if isinstance(data, list) and data:
                if len(data) > len(collected["sv_genes_raw"]):
                    collected["sv_genes_raw"] = data

    page.on("response", on_response)
    page.goto(url, timeout=LOAD_TIMEOUT_MS)

    # Wait for networkidle then an extra settle period
    try:
        page.wait_for_load_state("networkidle", timeout=LOAD_TIMEOUT_MS)
    except Exception:
        pass
    page.wait_for_timeout(SETTLE_MS)

    page.remove_listener("response", on_response)
    return collected


def capture_baseline(page: Page, study_id: str) -> dict:
    study_url = f"{BASE_URL}/study/summary?id={study_id}"
    print(f"  Navigating to {study_url}")
    collected = _load_page_and_collect(page, study_url)

    clinical = collected["clinical"]
    print(f"  clinical attrs captured: {len(clinical)}")

    mutated_genes = _norm_mutated(collected["mutated_genes_raw"])
    if not mutated_genes:
        print("  API gave no mutated genes — falling back to DOM scrape")
        mutated_genes = _dom_scrape_mutated_genes(page)
    print(f"  mutated genes: {len(mutated_genes)}")

    cna_genes = _norm_cna(collected["cna_genes_raw"])
    if not cna_genes:
        print("  API gave no CNA genes — falling back to DOM scrape")
        cna_genes = _dom_scrape_cna_genes(page)
    print(f"  CNA genes: {len(cna_genes)}")

    sv_genes = _norm_sv(collected["sv_genes_raw"])
    if not sv_genes:
        print("  API gave no SV genes — falling back to DOM scrape")
        sv_genes = _dom_scrape_sv_genes(page)
    print(f"  SV genes: {len(sv_genes)}")

    return {
        "scenario": "baseline",
        "study_id": study_id,
        "clinical": clinical,
        "mutated_genes": mutated_genes,
        "cna_genes": cna_genes,
        "sv_genes": sv_genes,
    }


def capture_cancer_type_filter(page: Page, study_id: str, top_cancer_type: str,
                                baseline_top_n_samples: int) -> dict:
    study_url = f"{BASE_URL}/study/summary?id={study_id}"
    print(f"  Cancer-type filter: '{top_cancer_type}'")

    # Click the filter by interacting with the Cancer Type chart
    # Navigate fresh to avoid leftover state
    page.goto(study_url, timeout=LOAD_TIMEOUT_MS)
    try:
        page.wait_for_load_state("networkidle", timeout=LOAD_TIMEOUT_MS)
    except Exception:
        pass
    page.wait_for_timeout(15_000)

    chart_sel = "[data-test='chart-container-CANCER_TYPE']"
    try:
        page.wait_for_selector(f"{chart_sel}", timeout=30_000)
        row = page.locator(
            f"{chart_sel} [role='row'], {chart_sel} tr, {chart_sel} .react-bs-container-body tr",
            has_text=top_cancer_type
        ).first
        if row.count() == 0:
            row = page.locator(f"{chart_sel} >> text={top_cancer_type}").first
        checkbox = row.locator("[role='checkbox'], input[type='checkbox']").first
        if checkbox.count() > 0:
            checkbox.click()
        else:
            row.click()
        page.wait_for_timeout(8_000)
        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            pass
    except Exception as e:
        print(f"  WARNING: could not apply filter via DOM: {e}", file=sys.stderr)

    mutated_genes = _dom_scrape_mutated_genes(page)
    cna_genes = _dom_scrape_cna_genes(page)

    filter_applied = bool(
        mutated_genes
        and mutated_genes[0].get("n_samples", baseline_top_n_samples) != baseline_top_n_samples
    )

    return {
        "scenario": "cancer_type",
        "study_id": study_id,
        "filter_cancer_type": top_cancer_type,
        "filter_applied": filter_applied,
        "mutated_genes": mutated_genes,
        "cna_genes": cna_genes,
    }


def capture_tp53_filter(page: Page, study_id: str, baseline_top_n_samples: int) -> dict:
    study_url = f"{BASE_URL}/study/summary?id={study_id}"
    print("  TP53 mutation filter")

    page.goto(study_url, timeout=LOAD_TIMEOUT_MS)
    try:
        page.wait_for_load_state("networkidle", timeout=LOAD_TIMEOUT_MS)
    except Exception:
        pass
    page.wait_for_timeout(15_000)

    chart_sel = f"[data-test='chart-container-{study_id}_mutations']"
    try:
        page.wait_for_selector(f"{chart_sel} >> text=TP53", timeout=30_000)
        row = page.locator(
            f"{chart_sel} [role='row'], {chart_sel} tr", has_text="TP53"
        ).first
        checkbox = row.locator("[role='checkbox'], input[type='checkbox']").first
        if checkbox.count() > 0:
            checkbox.click(force=True)
        else:
            row.click(force=True)
        page.wait_for_timeout(10_000)
        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            pass
    except Exception as e:
        print(f"  WARNING: could not apply TP53 filter via DOM: {e}", file=sys.stderr)

    mutated_genes = _dom_scrape_mutated_genes(page)

    tp53_row = next((r for r in mutated_genes if r["gene"] == "TP53"), None)
    filter_applied = bool(tp53_row and tp53_row.get("freq", 0) >= 99.0)
    if not filter_applied and mutated_genes:
        filter_applied = mutated_genes[0].get("n_samples", baseline_top_n_samples) != baseline_top_n_samples

    return {
        "scenario": "tp53_filter",
        "study_id": study_id,
        "filter_applied": filter_applied,
        "mutated_genes": mutated_genes,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Capture golden fixtures from public cBioPortal")
    parser.add_argument("--study", default=STUDY_ID)
    parser.add_argument("--baseline-only", action="store_true")
    parser.add_argument("--headed", action="store_true", help="Show browser window (useful for debugging)")
    args = parser.parse_args()

    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.headed, slow_mo=50)
        context = browser.new_context(viewport={"width": 1600, "height": 900})
        page = context.new_page()

        # Baseline
        print(f"\n[Baseline] {args.study}")
        baseline = capture_baseline(page, args.study)
        out = FIXTURES_DIR / f"{args.study}_baseline.json"
        out.write_text(json.dumps(baseline, indent=2))
        print(f"  Saved → {out}")

        if not args.baseline_only:
            top_ct = (list(baseline["clinical"].get("CANCER_TYPE", [{}]))[:1] or [{}])[0].get(
                "value", "Non-Small Cell Lung Cancer"
            )
            baseline_top_n = (
                baseline["mutated_genes"][0]["n_samples"] if baseline["mutated_genes"] else 0
            )

            print(f"\n[Cancer type filter] top={top_ct!r}")
            ct_data = capture_cancer_type_filter(page, args.study, top_ct, baseline_top_n)
            out = FIXTURES_DIR / f"{args.study}_cancer_type.json"
            out.write_text(json.dumps(ct_data, indent=2))
            print(f"  Saved → {out}")

            print(f"\n[TP53 filter]")
            tp53_data = capture_tp53_filter(page, args.study, baseline_top_n)
            out = FIXTURES_DIR / f"{args.study}_tp53_filter.json"
            out.write_text(json.dumps(tp53_data, indent=2))
            print(f"  Saved → {out}")

        browser.close()

    print("\nDone.")
    for f in sorted(FIXTURES_DIR.glob("*.json")):
        d = json.loads(f.read_text())
        n_clin = len(d.get("clinical", {}))
        n_mut = len(d.get("mutated_genes", []))
        print(f"  {f.name}  ({n_clin} clinical attrs, {n_mut} mutated genes)")


if __name__ == "__main__":
    main()
