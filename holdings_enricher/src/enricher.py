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

# Match-method labels specific to the API path (the name-based ones live in
# matcher.py). Kept distinct so the alias report shows at a glance how much of
# the enrichment came free from the API versus needed a name lookup.
METHOD_ISIN          = "isin-exact"      # API ISIN hit the AMFI mapping directly
METHOD_ISIN_FALLBACK = "isin-stale"      # API ISIN missed; recovered by name


def enrich(
    records: List[Dict],
    isin_mapping: Dict,          # {isin -> {amfi_name, mktcap_category, mktcap_cr, industry, sector}}
    matcher: Matcher,
) -> List[Dict]:
    """
    Enrich each holding record with AMFI data via ISIN lookup.

    Parameters
    ----------
    records      : output of api_source.to_records()
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

        # ── ISIN first, name second ──────────────────────────────────────
        # The API states an ISIN per holding, so prefer the direct lookup:
        # it is exact, and it sidesteps instrument strings the name matcher
        # cannot parse ("HDFC BANK LIMITED EQ NEW FV RE. 1/-").
        #
        # The fallback is not redundant. A company's ISIN changes on
        # corporate actions (face-value splits, mergers) while the AMFI
        # mapping still carries the older one — Kotak Bank arrives as
        # INE237A01028 but is mapped under INE237A01036. The name matcher
        # resolves exactly those, so dropping it would silently strip
        # market-cap category from a few percent of each fund's weight.
        api_isin = (rec.get("isin") or "").strip()
        if api_isin and api_isin in isin_mapping:
            isin, method, score = api_isin, METHOD_ISIN, 100.0
        else:
            isin, method, score = matcher.match(stock)
            if api_isin and isin:
                # Recovered a stale/revised ISIN via the name — worth marking
                # distinctly so the alias report shows how often this happens.
                method = METHOD_ISIN_FALLBACK

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
                # AMFI is authoritative for sector/industry: the MCP server
                # groups holdings by these strings, and AMFI's vocabulary
                # ("Banking and Finance") differs from the API's ("Financial
                # Services"). Mixing the two would split one real sector into
                # two keys. The API's labels are used ONLY when AMFI has no
                # record for the holding, where the choice is between the
                # API's label and nothing at all.
                "Mkt Cap Cat":   amfi_rec.get("mktcap_category", ""),
                "Mkt Cap ₹Cr":  amfi_rec.get("mktcap_cr"),
                "Industry":      amfi_rec.get("industry")
                                 or rec.get("api_industry", ""),
                "Sector":        amfi_rec.get("sector")
                                 or rec.get("raw_sector", ""),
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
