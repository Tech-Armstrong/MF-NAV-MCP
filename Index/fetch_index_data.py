"""
Index/fetch_index_data.py  —  download Indian index history from Yahoo Finance
==============================================================================

Writes two parquet files that server.py reads exactly like the fund data:

    ./data/index_history.parquet   ticker, nav_date, close
    ./data/index_master.parquet    ticker, index_name

NOTE: those paths are relative to the CURRENT DIRECTORY, so run this from the
project root (the directory containing server.py), not from Index/.

The schema deliberately mirrors nav_history / scheme_master (a code column, a
date column, a level column) so the index tools reuse _compute_returns rather
than duplicating the returns math.

Unlike the NAV parquet, these two files ARE committed to the repo: the data is
small (5 indices x 20 years is a few hundred KB), rarely changes shape, and
committing it means the server has no runtime dependency on Yahoo — no rate
limits, no outages, no yfinance in the deployed image.

Re-run this to refresh (the series only extend forward, so it is safe to re-run
any time) and commit the result — from the project root:

    python Index/fetch_index_data.py
    git add data/index_history.parquet data/index_master.parquet

Requires yfinance, which is a DEV dependency only — see requirements-dev.txt.
The server itself never imports it.
"""

from __future__ import annotations

import os
import sys
from datetime import date

import duckdb

# Curated set: broad market indices only.
#
# Yahoo's coverage of Indian indices is uneven. These five all carry ~20 years of
# daily closes with ~245 trading days a year and no NaNs. The SECTOR indices
# (^CNXAUTO, ^CNXFMCG, ^CNXFIN, ^CNXMETAL, ^CNXREALTY, ^CNXENERGY, ^CNXIT,
# ^NSEBANK, ...) are deliberately excluded: several return only a handful of rows
# even for a one-month window, so returns computed from them would be silently
# wrong rather than merely absent.
INDICES = [
    ("^NSEI",    "Nifty 50"),
    ("^BSESN",   "BSE Sensex"),
    ("^CNX100",  "Nifty 100"),
    ("^CRSLDX",  "Nifty 500"),
    ("^NSMIDCP", "Nifty Midcap 50"),
]

# A ticker with fewer rows than this is treated as broken rather than written —
# ~20 years of daily closes should be several thousand rows, so anything this
# small means Yahoo returned a truncated series.
MIN_ROWS = 500


def main() -> None:
    try:
        import yfinance as yf
    except ImportError:
        sys.exit(
            "yfinance is not installed. It is a dev-only dependency:\n"
            "    pip install -r requirements-dev.txt"
        )

    os.makedirs("data", exist_ok=True)

    master_rows: list[tuple[str, str]] = []
    hist_rows: list[tuple[str, date, float]] = []
    failed: list[tuple[str, str]] = []

    for ticker, name in INDICES:
        print(f"  {ticker:<10} {name:<18} ", end="", flush=True)
        try:
            # auto_adjust is a no-op for indices (they pay no dividends) but is
            # passed explicitly so the column set is stable across yfinance
            # versions. These are PRICE-return series — see the note below.
            hist = yf.Ticker(ticker).history(period="max", auto_adjust=True)
        except Exception as exc:                      # network / parse failure
            failed.append((ticker, str(exc)[:60]))
            print(f"FAILED ({str(exc)[:40]})")
            continue

        if hist.empty:
            failed.append((ticker, "empty response"))
            print("FAILED (empty)")
            continue

        closes = hist["Close"].dropna()
        closes = closes[closes > 0]                   # guard against bad ticks
        if len(closes) < MIN_ROWS:
            failed.append((ticker, f"only {len(closes)} rows"))
            print(f"FAILED (only {len(closes)} rows, expected >{MIN_ROWS})")
            continue

        # The DatetimeIndex is tz-aware (Asia/Kolkata); .date() drops the tz and
        # gives the plain trading date, which is what DuckDB stores.
        for ts, close in closes.items():
            hist_rows.append((ticker, ts.date(), float(close)))

        master_rows.append((ticker, name))
        print(f"OK  {len(closes):>5} rows  "
              f"{closes.index[0].date()} -> {closes.index[-1].date()}")

    if failed:
        print("\nFailed tickers:")
        for ticker, why in failed:
            print(f"  {ticker}: {why}")
    if not master_rows:
        sys.exit("\nNo index data downloaded — refusing to write empty parquet.")

    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE index_master(ticker VARCHAR, index_name VARCHAR);")
    con.executemany("INSERT INTO index_master VALUES (?, ?);", master_rows)
    con.execute("CREATE TABLE index_history("
                "ticker VARCHAR, nav_date DATE, close DOUBLE);")
    con.executemany("INSERT INTO index_history VALUES (?, ?, ?);", hist_rows)

    con.execute("COPY index_master  TO './data/index_master.parquet'  (FORMAT PARQUET);")
    con.execute("COPY index_history TO './data/index_history.parquet' (FORMAT PARQUET);")

    print(f"\nWrote ./data/index_master.parquet   ({len(master_rows)} indices)")
    print(f"Wrote ./data/index_history.parquet  ({len(hist_rows)} rows)")
    print("\nNOTE: Yahoo index series are PRICE return — they exclude dividends.")
    print("Fund NAVs are TOTAL return. Comparing them directly understates the")
    print("index by roughly 1-1.5%/yr for Indian equity, so the server labels")
    print("index results return_type='price'.")
    print("\nCommit the refreshed data (data/ is gitignored except these files):")
    print("    git add data/index_history.parquet data/index_master.parquet")


if __name__ == "__main__":
    main()
