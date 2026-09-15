"""
ticker.py — derive an index ticker from its NSE display name.

Copied verbatim from Index/parse_index_xlsx.py's make_ticker(), NOT imported,
so this script has no dependency on the rest of the repo's layout and can run
standalone in CI. If make_ticker() ever changes in Index/parse_index_xlsx.py,
mirror the change here too — otherwise tickers computed by this refresh will
stop matching the ones already in index_master.parquet and every row will be
silently skipped as "unknown".
"""

import re


def make_ticker(name: str) -> str:
    """'Nifty Midcap 150' -> 'NIFTY_MIDCAP_150'."""
    t = name.strip().upper()
    t = t.replace("&", " AND ")
    t = re.sub(r"[^A-Z0-9]+", "_", t)
    return re.sub(r"_+", "_", t).strip("_")
