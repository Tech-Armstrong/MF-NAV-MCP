"""
parser.py
---------
Reads the raw holdings CSV (as downloaded from Value Research / MFI Explorer).

Expected CSV layout
-------------------
Row 1 : title row  ("Mutliple Fund Holdings Download")
Row 2 : column headers (comma-separated)
Row 3+: data rows

Columns used:
  Fund, Holding, Holding type, As of, Percentage, Sector, Rating

Supports any cap-category label supplied by the caller
(e.g. "Large Cap", "Mid Cap", "Small Cap").
"""

from pathlib import Path
from typing import List, Dict

EQUITY_HOLDING_TYPE = "Equity"


def parse_holdings_csv(
    filepath: str | Path,
    cap_category: str = "",
    return_non_equity: bool = False,
):
    """
    Parse a holdings CSV and return a list of dicts for equity rows only.

    Parameters
    ----------
    filepath          : path to the CSV file
    cap_category      : label to stamp on every row (e.g. "Large Cap")
    return_non_equity : if True, return (equity_records, non_equity_records)
                        instead of just equity_records. Non-equity rows carry
                        only fund / holding_type / pct — enough for the
                        validator to reconcile a fund to 100%.

    Returns
    -------
    List of dicts with keys:
        cap_category, fund, stock, holding_type, as_of, pct, raw_sector
    (or a 2-tuple, when return_non_equity is set)
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Holdings file not found: {filepath}")

    with open(filepath, encoding="utf-8-sig") as f:
        raw = f.read()

    lines = [ln.rstrip("\r") for ln in raw.strip().split("\n")]

    # Find header row — the first line that contains "Fund" and "Holding"
    header_idx = None
    for i, line in enumerate(lines):
        parts = [p.strip() for p in line.split(",")]
        if "Fund" in parts and "Holding" in parts:
            header_idx = i
            break

    if header_idx is None:
        raise ValueError(f"Cannot find header row in {filepath.name}")

    headers = [h.strip() for h in lines[header_idx].split(",")]

    # Locate required columns
    def _col(name):
        try:
            return headers.index(name)
        except ValueError:
            return None

    c_fund    = _col("Fund")
    c_holding = _col("Holding")
    c_type    = _col("Holding type")
    c_as_of   = _col("As of")
    c_pct     = _col("Percentage")
    c_sector  = _col("Sector")

    records = []
    non_equity: List[Dict] = []
    for line in lines[header_idx + 1 :]:
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < len(headers):
            continue

        holding_type = parts[c_type].strip() if c_type is not None else ""

        try:
            pct = float(parts[c_pct]) if c_pct is not None else None
        except ValueError:
            pct = None

        if holding_type != EQUITY_HOLDING_TYPE:
            # Not an equity holding (cash, repo, derivatives, debt, …). These are
            # excluded from enrichment, but their weight is retained so the
            # validator can reconcile each fund against a true 100%.
            if pct is not None:
                non_equity.append(
                    {
                        "fund":         parts[c_fund].strip() if c_fund is not None else "",
                        "holding_type": holding_type,
                        "pct":          pct,
                    }
                )
            continue

        records.append(
            {
                "cap_category": cap_category,
                "fund":         parts[c_fund].strip()    if c_fund    is not None else "",
                "stock":        parts[c_holding].strip() if c_holding is not None else "",
                "holding_type": holding_type,
                "as_of":        parts[c_as_of].strip()   if c_as_of   is not None else "",
                "pct":          pct,
                "raw_sector":   parts[c_sector].strip()  if c_sector  is not None else "",
            }
        )

    if return_non_equity:
        return records, non_equity
    return records
