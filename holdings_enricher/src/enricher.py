"""
enricher.py
-----------
Takes parsed holdings records and enriches each row with:
  - ISIN
  - AMFI company name
  - Market Cap Category  (Large / Mid / Small Cap)
  - Market Cap ₹Cr       (numeric, from sector source files)
  - Industry
  - Sector
  - Match Method & Score  (for auditability)
"""

from typing import List, Dict, Optional

from src.matcher import Matcher


def enrich(
    records: List[Dict],
    isin_mapping: Dict,          # {isin -> {amfi_name, mktcap_category, mktcap_cr, industry, sector}}
    matcher: Matcher,
) -> List[Dict]:
    """
    Enrich each holding record with AMFI data via ISIN lookup.

    Parameters
    ----------
    records      : output of parser.parse_holdings_csv()
    isin_mapping : loaded from isin_mapping.json
    matcher      : Matcher instance (contains alias rules + fuzzy index)

    Returns
    -------
    List of enriched dicts, one per input row, with extra keys:
        isin, amfi_name, match_method, match_score,
        mktcap_category, mktcap_cr, industry, sector
    """
    enriched = []

    for rec in records:
        stock = rec["stock"]
        isin, method, score = matcher.match(stock)

        amfi_rec: Dict = isin_mapping.get(isin, {}) if isin else {}

        enriched.append(
            {
                # ── original fields ──────────────────────────────────────
                "Cap Category":  rec["cap_category"],
                "Fund":          rec["fund"],
                "Stock":         stock,
                "Holding %":     rec["pct"],
                "As of":         rec["as_of"],
                # ── matching metadata ────────────────────────────────────
                "AMFI Name":     amfi_rec.get("amfi_name", ""),
                "ISIN":          isin or "",
                "Match Method":  method,
                "Match Score":   score,
                # ── enriched data ────────────────────────────────────────
                "Mkt Cap Cat":   amfi_rec.get("mktcap_category", ""),
                "Mkt Cap ₹Cr":  amfi_rec.get("mktcap_cr"),
                "Industry":      amfi_rec.get("industry", ""),
                "Sector":        amfi_rec.get("sector", ""),
            }
        )

    return enriched


def match_summary(enriched: List[Dict]) -> Dict:
    """Return a dict with high-level match statistics."""
    total   = len(enriched)
    matched = sum(1 for r in enriched if r["ISIN"])
    by_method: Dict[str, int] = {}
    for r in enriched:
        m = r["Match Method"]
        by_method[m] = by_method.get(m, 0) + 1

    return {
        "total":      total,
        "matched":    matched,
        "unmatched":  total - matched,
        "match_pct":  round(matched / total * 100, 2) if total else 0,
        "by_method":  by_method,
    }
