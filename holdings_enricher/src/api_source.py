"""
api_source.py
-------------
Turns Advisorkhoj API rows into the record contract the enricher expects.

This is the API-era replacement for the old `parser.py`, which read holdings
CSVs exported from Value Research. It emits exactly the same record shape, so
everything downstream (enricher, validator, writer, json_export) is unchanged:

    equity:      cap_category, fund, stock, holding_type, as_of, pct,
                 raw_sector, isin, api_industry
    non-equity:  fund, holding_type, pct

Two fields are new versus the CSV path, and they are the reason the API is
worth switching to:

    isin          the API states it per holding, so the enricher can look up
                  AMFI data directly instead of fuzzy-matching a stock name
    api_industry  the API's own industry label, kept as a fallback for holdings
                  whose ISIN is absent from the AMFI mapping

What the API does NOT provide is market-cap category (Large/Mid/Small), which
is why `isin_mapping.json` is still required — see enricher.py.

Naming
------
The API distinguishes two names and so must we:

    scheme_amfi_common  "Axis Large Cap Fund"                    <- query key
    scheme_name         "Axis Large Cap Fund - Regular - IDCW"   <- actual scheme

`fund` is set from `scheme_name`, matching what the CSV path produced and what
`nav_names.py` expects to resolve against the NAV parquet. A portfolio request
returns exactly one scheme_name per fund (verified across several funds and
months), so holdings never double-count across plan variants.

Dates
-----
The API returns `portfolio_date` as DD-MM-YYYY; the CSV path produced
DD-Mon-YYYY ("31-Jul-2026") and that is what `as_of` carries through to the
final JSON. `_to_as_of()` converts, so the output format does not change.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from src.api_client import AdvisorkhojClient, AdvisorkhojError

EQUITY_ASSET_CLASS = "Equity"

# Pause between per-fund portfolio calls. A category pull is dozens of requests
# against someone else's API; this keeps a monthly refresh from looking like a
# burst of abuse. Tune with --api-delay if the provider is comfortable.
DEFAULT_DELAY = 0.3


def _to_as_of(portfolio_date: str) -> str:
    """'31-12-2025' -> '31-Dec-2025'. Returns the input unchanged if unparseable."""
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d-%b-%Y"):
        try:
            return datetime.strptime(portfolio_date.strip(), fmt).strftime("%d-%b-%Y")
        except (ValueError, AttributeError):
            continue
    return (portfolio_date or "").strip()


def to_records(api_rows: List[Dict], cap_category: str = "") -> Tuple[List[Dict], List[Dict]]:
    """
    Convert one fund's API rows into (equity_records, non_equity_records).

    Mirrors `parse_holdings_csv(..., return_non_equity=True)`. Non-equity rows
    (cash, TREPS, repo, receivables) are not enriched but their weight is kept
    so the validator can reconcile each fund to a true 100%.
    """
    equity: List[Dict] = []
    non_equity: List[Dict] = []

    for row in api_rows:
        asset_class = (row.get("asset_class") or "").strip()
        fund = (row.get("scheme_name") or "").strip()

        pct = row.get("holdings")
        try:
            pct = float(pct) if pct is not None else None
        except (TypeError, ValueError):
            pct = None

        if asset_class != EQUITY_ASSET_CLASS:
            if pct is not None:
                non_equity.append({
                    "fund": fund,
                    "holding_type": asset_class,
                    "pct": pct,
                })
            continue

        equity.append({
            "cap_category": cap_category,
            "fund":         fund,
            "stock":        (row.get("instrument") or "").strip(),
            "holding_type": asset_class,
            "as_of":        _to_as_of(row.get("portfolio_date") or ""),
            "pct":          pct,
            "raw_sector":   (row.get("sector") or "").strip(),
            # New in the API path — see the module docstring.
            "isin":         (row.get("isin") or "").strip(),
            "api_industry": (row.get("industry") or "").strip(),
        })

    return equity, non_equity


def fetch_category(
    client: AdvisorkhojClient,
    category: str,
    year: int,
    month: str,
    cap_category: str = "",
    funds: Optional[List[str]] = None,
    delay: float = DEFAULT_DELAY,
    verbose: bool = True,
) -> Tuple[List[Dict], List[Dict], Dict]:
    """
    Pull every fund in one category for one month.

    Returns (equity_records, non_equity_records, report) where `report` records
    what happened per fund — funds with no published portfolio for the month are
    NOT an error (a month is published progressively), but they must be visible
    rather than silently absent, or a half-empty run looks like a complete one.

    `funds` overrides the category listing when you want specific funds only.
    `cap_category` is the label stamped on every row; it scopes NAV name
    resolution later, so it must match a category in nav_scheme_names.txt.
    """
    names = funds if funds else client.get_schemes_in_category(category)
    if verbose:
        print(f"[api] {category}: {len(names)} fund(s) for {month} {year}")

    all_equity: List[Dict] = []
    all_non_equity: List[Dict] = []
    empty: List[str] = []
    failed: List[Dict] = []

    for i, name in enumerate(names, 1):
        try:
            rows = client.get_portfolio(name, year, month)
        except AdvisorkhojError as e:
            # One bad fund must not abandon the other 40. Collect and continue;
            # the caller decides whether a partial pull is acceptable.
            failed.append({"fund": name, "error": str(e).split("\n")[0]})
            if verbose:
                print(f"  [{i}/{len(names)}] {name} — FAILED: "
                      f"{str(e).split(chr(10))[0]}")
            continue

        if not rows:
            empty.append(name)
            if verbose:
                print(f"  [{i}/{len(names)}] {name} — no portfolio published")
        else:
            eq, non_eq = to_records(rows, cap_category=cap_category)
            all_equity.extend(eq)
            all_non_equity.extend(non_eq)
            if verbose:
                as_of = eq[0]["as_of"] if eq else "?"
                print(f"  [{i}/{len(names)}] {name} — {len(eq)} equity "
                      f"({as_of})")

        if delay and i < len(names):
            time.sleep(delay)

    report = {
        "category": category,
        "cap_category": cap_category,
        "year": year,
        "month": month,
        "requested": len(names),
        "with_holdings": len(names) - len(empty) - len(failed),
        "empty": empty,
        "failed": failed,
    }
    return all_equity, all_non_equity, report
