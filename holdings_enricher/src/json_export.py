"""
json_export.py
--------------
Exports the Fund Profile as hierarchical JSON for the parent MCP server.

The xlsx Fund Profile sheet is deliberately FLAT — one row per fund, one column
per sector — because that is what reads well in a spreadsheet. A tool answering
questions like "what is this fund's exposure to Financial Services?" wants the
opposite: a nested structure it can index into without knowing the column order.
This module produces that shape from the same enriched rows, so the two outputs
can never disagree.

Shape
-----
    {
      "generated_at": "2026-08-13T19:40:00",
      "fund_count": 3,
      "funds": {
        "<fund name>": {
          "fund": "<fund name>",
          "cap_category": "Large Cap",         # label passed via --cap
          "as_of": "31-Jul-2026",
          "equity_pct": 92.4,                  # total equity weight
          "market_cap": {
            "Large Cap": 70.1, "Mid Cap": 15.2,
            "Small Cap": 5.1, "Unclassified": 2.0
          },
          "sectors": {
            "Financial Services": {
              "weight_pct": 28.4,
              "industries": {"Banks": 20.1, "Finance": 8.3},
              "holdings": [
                {"stock": "HDFC Bank Ltd.", "amfi_name": "...", "isin": "INE...",
                 "weight_pct": 7.2, "market_cap_cat": "Large Cap",
                 "market_cap_cr": 1234567.0, "industry": "Banks"}
              ]
            }
          },
          "top_holdings": [ ...same holding dicts, descending by weight... ],
          "unmatched": [ {"stock": "...", "weight_pct": 0.6} ]
        }
      }
    }

Weights are percentages of the fund's total portfolio, exactly as they appear in
the source holdings file — they sum to the fund's EQUITY weight, not to 100
(cash, repo and derivatives are not equity holdings and are excluded here).
`unmatched` lists equity rows that could not be resolved to an AMFI record, so a
consumer can tell "no exposure" apart from "we could not classify it".
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List

UNCLASSIFIED = "Unclassified Sector"
CAP_ORDER = ["Large Cap", "Mid Cap", "Small Cap"]


def _holding_dict(row: Dict) -> Dict:
    """One enriched row -> the holding shape used in the JSON."""
    return {
        "stock": row.get("Stock"),
        "amfi_name": row.get("AMFI Name"),
        "isin": row.get("ISIN"),
        "weight_pct": row.get("Holding %"),
        "market_cap_cat": row.get("Mkt Cap Cat") or None,
        "market_cap_cr": row.get("Mkt Cap ₹Cr"),
        "industry": row.get("Industry") or None,
        "match_method": row.get("Match Method"),
    }


def build_fund_profile_json(enriched: List[Dict],
                            top_n: int = 10,
                            resolver=None) -> Dict:
    """
    Aggregate enriched rows into the nested structure documented above.

    Mirrors build_fund_profile() in writer.py — same inputs, same arithmetic,
    different shape. Rounding matches the xlsx (5 dp) so the two agree exactly.

    `resolver` is a NavNameResolver; when given, each fund also carries the
    exact NAV-parquet scheme name so the MCP server can join on it. Funds are
    then keyed by that NAV name rather than the holdings name.
    """
    funds: Dict[str, Dict] = {}

    for row in enriched:
        fund = (row.get("Fund") or "").strip()
        if not fund:
            continue

        f = funds.setdefault(fund, {
            "fund": fund,
            "cap_category": (row.get("Cap Category") or "").strip() or None,
            "as_of": (row.get("As of") or "").strip() or None,
            "equity_pct": 0.0,
            "market_cap": {c: 0.0 for c in CAP_ORDER + ["Unclassified"]},
            "sectors": {},
            "_holdings": [],
            "unmatched": [],
        })

        pct = row.get("Holding %") or 0.0
        f["equity_pct"] += pct

        # market-cap bucket — same rule as the xlsx Fund Profile
        cap = (row.get("Mkt Cap Cat") or "").strip()
        f["market_cap"][cap if cap in CAP_ORDER else "Unclassified"] += pct

        holding = _holding_dict(row)
        f["_holdings"].append(holding)

        # A row with no AMFI name never resolved; record it so a consumer can
        # distinguish "not held" from "held but unclassifiable".
        if not row.get("AMFI Name"):
            f["unmatched"].append({"stock": row.get("Stock"),
                                   "weight_pct": pct})

        sector = (row.get("Sector") or "").strip() or UNCLASSIFIED
        industry = (row.get("Industry") or "").strip() or UNCLASSIFIED

        s = f["sectors"].setdefault(sector, {
            "weight_pct": 0.0, "industries": {}, "holdings": [],
        })
        s["weight_pct"] += pct
        s["industries"][industry] = s["industries"].get(industry, 0.0) + pct
        s["holdings"].append(holding)

    # finalise: round, sort, derive top holdings
    out: Dict[str, Dict] = {}
    unresolved: List[Dict] = []
    for name in sorted(funds):
        f = funds[name]
        holdings = f.pop("_holdings")

        # Resolve to the NAV parquet's exact scheme_name. The two sources never
        # agree on the raw string, so without this the MCP server cannot join
        # holdings to NAV data at all.
        key = name
        if resolver is not None:
            r = resolver.resolve(name, f.get("cap_category") or "")
            f["nav_scheme_name"] = r["nav_scheme_name"]
            f["nav_name_match"] = {"method": r["method"], "score": r["score"]}
            if r["nav_scheme_name"]:
                key = r["nav_scheme_name"]
            else:
                unresolved.append({"fund": name,
                                   "cap_category": f.get("cap_category"),
                                   "best_score": r["score"]})

        f["equity_pct"] = round(f["equity_pct"], 5)
        f["market_cap"] = {k: round(v, 5) for k, v in f["market_cap"].items()}

        for sec in f["sectors"].values():
            sec["weight_pct"] = round(sec["weight_pct"], 5)
            sec["industries"] = {
                k: round(v, 5) for k, v in
                sorted(sec["industries"].items(), key=lambda kv: -kv[1])
            }
            sec["holdings"].sort(key=lambda h: -(h["weight_pct"] or 0))

        # sectors ordered by weight, heaviest first — the useful default
        f["sectors"] = dict(sorted(f["sectors"].items(),
                                   key=lambda kv: -kv[1]["weight_pct"]))

        f["top_holdings"] = sorted(
            holdings, key=lambda h: -(h["weight_pct"] or 0))[:top_n]
        f["holding_count"] = len(holdings)
        out[key] = f

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "fund_count": len(out),
        "keyed_by": "nav_scheme_name" if resolver is not None else "holdings_fund_name",
        "funds": out,
    }
    if unresolved:
        payload["unresolved_fund_names"] = unresolved
    return payload


def write_fund_profile_json(enriched: List[Dict], path: str | Path,
                            top_n: int = 10, resolve_nav_names: bool = True,
                            strict: bool = True) -> Path:
    """
    Build the nested profile and write it to `path`. Returns the path.

    resolve_nav_names: map each fund to its exact NAV-parquet scheme_name and
        key the output by it, so the MCP server can join without fuzzy matching.
    strict: raise if any fund cannot be resolved. On by default — a fund missing
        from the output is invisible to the MCP tool, and a silently dropped
        fund is far worse than a failed export.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    resolver = None
    if resolve_nav_names:
        from .nav_names import NavNameResolver
        resolver = NavNameResolver()

    payload = build_fund_profile_json(enriched, top_n=top_n, resolver=resolver)

    unresolved = payload.get("unresolved_fund_names") or []
    if unresolved:
        lines = "\n".join(
            f"    {u['fund']}  ({u['cap_category']}, best score {u['best_score']})"
            for u in unresolved)
        msg = (f"{len(unresolved)} fund(s) could not be matched to a NAV "
               f"scheme name:\n{lines}\n"
               "  Add them to data/nav_scheme_names.txt, or add an AMC "
               "abbreviation to AMC_ALIASES in src/nav_names.py.")
        if strict:
            raise ValueError(msg)
        print(f"[json_export] WARNING: {msg}")

    # ensure_ascii=False keeps the rupee sign and company names readable; the
    # file is always written UTF-8 regardless of the console code page.
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)

    methods: Dict[str, int] = {}
    for f in payload["funds"].values():
        m = (f.get("nav_name_match") or {}).get("method", "n/a")
        methods[m] = methods.get(m, 0) + 1
    print(f"[json_export] Fund profile JSON saved -> {path} "
          f"({payload['fund_count']} funds, keyed by {payload['keyed_by']})")
    if resolver is not None:
        print(f"[json_export] NAV name match: {methods}")
    return path
