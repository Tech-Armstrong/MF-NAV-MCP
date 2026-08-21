"""
validator.py
------------
Checks that each fund's holdings reconcile to ~100%.

A fund's equity weight alone never reaches 100% — the balance sits in cash,
reverse repo, derivatives and debt instruments. Validating equity against 100
therefore flags every fund holding more than `tolerance` in cash, which is
normal portfolio management rather than a data problem. (On the Jul-2026 files
that produced 75 false alarms out of 103 funds.)

So the check reconciles EQUITY + NON-EQUITY against 100. That total is the sum
of the source CSV's own Percentage column, so a deviation now means real
missing or malformed data.
"""

from collections import defaultdict
from typing import List, Dict, Tuple, Optional

DEFAULT_TOLERANCE = 2.0   # ± percentage points from 100


def validate_weights(
    enriched: List[Dict],
    tolerance: float = DEFAULT_TOLERANCE,
    non_equity: Optional[List[Dict]] = None,
) -> Tuple[Dict[str, float], List[str], Dict[str, float]]:
    """
    Parameters
    ----------
    enriched   : list of enriched holding dicts (output of enricher.enrich)
    tolerance  : allowed deviation from 100 (default ±2 pp)
    non_equity : non-equity rows from the API source (cash / repo / derivatives /
                 debt). Required to reconcile against a true 100%; if omitted,
                 the check falls back to equity-only and will over-flag.

    Returns
    -------
    fund_totals  : {fund_name -> equity + non-equity total pct}
    flagged      : funds whose total is outside [100-tol, 100+tol]
    equity_totals: {fund_name -> equity-only pct}  (for reporting)
    """
    equity_totals: Dict[str, float] = defaultdict(float)
    other_totals:  Dict[str, float] = defaultdict(float)

    for row in enriched:
        pct = row.get("Holding %")
        if pct is not None:
            try:
                equity_totals[row["Fund"]] += float(pct)
            except (ValueError, TypeError):
                pass

    for row in non_equity or []:
        pct = row.get("pct")
        if pct is not None:
            try:
                other_totals[row["fund"]] += float(pct)
            except (ValueError, TypeError):
                pass

    fund_totals = {
        fund: equity_totals.get(fund, 0.0) + other_totals.get(fund, 0.0)
        for fund in set(equity_totals) | set(other_totals)
    }

    flagged = [
        fund
        for fund, total in fund_totals.items()
        if not (100 - tolerance <= total <= 100 + tolerance)
    ]

    return fund_totals, flagged, dict(equity_totals)


def validation_rows(
    fund_totals: Dict[str, float],
    flagged: List[str],
    tolerance: float = DEFAULT_TOLERANCE,
    equity_totals: Optional[Dict[str, float]] = None,
) -> List[Dict]:
    """Build a list of dicts ready for writing to the Validation sheet."""
    equity_totals = equity_totals or {}
    rows = []
    for fund, total in sorted(fund_totals.items()):
        deviation = total - 100
        equity = equity_totals.get(fund, 0.0)
        status = "OK" if fund not in flagged else (
            "INCOMPLETE" if total < 100 - tolerance else "OVER 100"
        )
        rows.append(
            {
                "Fund":             fund,
                "Equity %":         round(equity, 4),
                "Non-Equity %":     round(total - equity, 4),
                "Sum of Holding %": round(total, 4),
                "Deviation":        round(deviation, 4),
                "Status":           status,
                "Tolerance (±%)":   tolerance,
            }
        )
    return rows
