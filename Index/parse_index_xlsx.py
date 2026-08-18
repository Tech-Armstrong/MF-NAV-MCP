"""
Index/parse_index_xlsx.py  —  build the index parquet from the Weekly Market
                              Pulse Tracker workbook
============================================================================

Writes the two parquet files the server reads:

    ./data/index_history.parquet   ticker, nav_date, close
    ./data/index_master.parquet    ticker, index_name

Run from the PROJECT ROOT (the directory containing server.py), because the
output paths are relative to the current directory:

    python Index/parse_index_xlsx.py
    python Index/parse_index_xlsx.py --xlsx "Index/Weekly Market Pulse Tracker - 17 August 2026.xlsx"
    git add data/index_history.parquet data/index_master.parquet

Why this replaced the Yahoo fetcher
-----------------------------------
Index/fetch_index_data.py pulled 5 broad indices from Yahoo. This workbook has
111, going back to 1990 instead of 2007 — and critically it covers the SECTOR
and FACTOR indices that Yahoo serves too sparsely to be trusted (Yahoo returned
6 rows for ^CNXAUTO over a one-month window).

The two agree where they overlap: of 4,639 shared Nifty 50 dates, 4,638 match
to the cent. So this is a strict superset, not a different opinion.

Source layout (sheet "Data")
----------------------------
    row  1-10  summary block (52W high/low, latest close, ...) — ignored
    row  11    header:  Date | <index name> | <index name> | ...
    row  12+   one row per trading day, DESCENDING by date

A blank cell means the index did not exist yet (or did not trade); those are
skipped rather than forward-filled, so the server's own "latest close on/before
the target" logic decides what a missing day means.

Ticker convention
-----------------
The workbook has no tickers, so one is derived from the index name:
`Nifty Midcap 150` -> `NIFTY_MIDCAP_150`. Names are messy (mixed case, `&`,
`:`, `Ex-`), so derivation is deterministic and collisions are a hard error
rather than a silent overwrite. The friendly aliases the server already
exposes (NIFTY50, SENSEX, ...) are unaffected — they resolve via _INDEX_ALIASES
in server.py, which is keyed on the ticker.
"""

from __future__ import annotations

import argparse
import glob
import os
import re
import sys
from datetime import date, datetime

import duckdb

SHEET = "Data"
HEADER_ROW = 11          # 1-based row holding "Date | <index> | ..."

# An index with fewer points than this cannot support the periods the server
# offers (5Y needs ~1250 trading days). Keeping a 300-point series would let
# get_index_returns return a confidently wrong 5Y number, so they are dropped.
MIN_ROWS = 500


def make_ticker(name: str) -> str:
    """'Nifty Midcap 150' -> 'NIFTY_MIDCAP_150'. Deterministic and reversible
    enough to eyeball; collisions are caught by the caller."""
    t = name.strip().upper()
    t = t.replace("&", " AND ")
    t = re.sub(r"[^A-Z0-9]+", "_", t)
    return re.sub(r"_+", "_", t).strip("_")


def find_default_xlsx() -> str | None:
    """Newest .xlsx sitting in Index/ — usually the latest weekly tracker."""
    here = os.path.dirname(os.path.abspath(__file__))
    files = [f for f in glob.glob(os.path.join(here, "*.xlsx"))
             if not os.path.basename(f).startswith("~$")]
    return max(files, key=os.path.getmtime) if files else None


def parse(xlsx_path: str, min_rows: int = MIN_ROWS):
    try:
        import openpyxl
    except ImportError:
        sys.exit("openpyxl is not installed:  pip install -r requirements-dev.txt")

    print(f"Reading {xlsx_path}")
    wb = openpyxl.load_workbook(xlsx_path, data_only=True, read_only=True)
    if SHEET not in wb.sheetnames:
        sys.exit(f"Sheet '{SHEET}' not found. Sheets: {wb.sheetnames}")

    rows = list(wb[SHEET].iter_rows(min_row=HEADER_ROW, values_only=True))
    if not rows:
        sys.exit(f"Sheet '{SHEET}' has no rows at/after row {HEADER_ROW}.")

    header = rows[0]
    if not header or str(header[0]).strip().lower() != "date":
        sys.exit(f"Expected 'Date' in the first cell of row {HEADER_ROW}, "
                 f"got {header[0]!r}. Has the workbook layout changed?")

    # Column 0 is Date; a trailing duplicate "Date" column exists in the source
    # and is skipped along with any blank headers.
    columns = [(i, str(h).strip()) for i, h in enumerate(header)
               if i > 0 and h not in (None, "") and str(h).strip().lower() != "date"]
    print(f"  {len(columns)} index columns found in the header")

    series: dict[str, list[tuple[date, float]]] = {}
    for row in rows[1:]:
        d = row[0]
        if not isinstance(d, datetime):
            continue
        day = d.date()
        for i, name in columns:
            if i >= len(row):
                continue
            v = row[i]
            # Blank = index did not exist / did not trade. Skip, never fill.
            if isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0:
                series.setdefault(name, []).append((day, float(v)))

    kept, dropped = {}, []
    for name, points in series.items():
        if len(points) < min_rows:
            dropped.append((name, len(points)))
        else:
            kept[name] = points

    # Ticker collisions would silently merge two different indices.
    tickers: dict[str, str] = {}
    for name in kept:
        t = make_ticker(name)
        if t in tickers:
            sys.exit(f"Ticker collision: '{name}' and '{tickers[t]}' both map "
                     f"to '{t}'. Adjust make_ticker().")
        tickers[t] = name

    return kept, tickers, dropped


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Build data/index_*.parquet from the Weekly Market Pulse "
                    "Tracker workbook.")
    ap.add_argument("--xlsx", default=None,
                    help="Path to the workbook (default: newest .xlsx in Index/).")
    ap.add_argument("--min-rows", type=int, default=MIN_ROWS,
                    help=f"Drop indices with fewer points (default {MIN_ROWS}).")
    ap.add_argument("--out-dir", default="./data",
                    help="Where to write the parquet (default ./data).")
    args = ap.parse_args()

    xlsx_path = args.xlsx or find_default_xlsx()
    if not xlsx_path or not os.path.exists(xlsx_path):
        sys.exit("No workbook found. Put the weekly tracker .xlsx in Index/ "
                 "or pass --xlsx PATH.")

    kept, tickers, dropped = parse(xlsx_path, args.min_rows)
    if not kept:
        sys.exit("No index series met the minimum row count — nothing written.")

    hist_rows, master_rows = [], []
    for ticker, name in sorted(tickers.items()):
        master_rows.append((ticker, name))
        for day, close in sorted(kept[name]):
            hist_rows.append((ticker, day, close))

    os.makedirs(args.out_dir, exist_ok=True)
    hist_path = os.path.join(args.out_dir, "index_history.parquet")
    master_path = os.path.join(args.out_dir, "index_master.parquet")

    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE index_master(ticker VARCHAR, index_name VARCHAR);")
    con.executemany("INSERT INTO index_master VALUES (?, ?);", master_rows)
    con.execute("CREATE TABLE index_history("
                "ticker VARCHAR, nav_date DATE, close DOUBLE);")
    con.executemany("INSERT INTO index_history VALUES (?, ?, ?);", hist_rows)
    con.execute(f"COPY index_master  TO '{master_path}'  (FORMAT PARQUET);")
    con.execute(f"COPY index_history TO '{hist_path}' (FORMAT PARQUET);")

    all_days = [d for pts in kept.values() for d, _ in pts]
    print(f"\nWrote {master_path}   ({len(master_rows)} indices)")
    print(f"Wrote {hist_path}  ({len(hist_rows)} rows, "
          f"{min(all_days)} -> {max(all_days)})")

    if dropped:
        print(f"\nDropped {len(dropped)} index/indices with < {args.min_rows} "
              f"points (too short to compute the server's periods safely):")
        for name, n in sorted(dropped, key=lambda x: -x[1]):
            print(f"  {n:>6}  {name}")

    print("\nThese index levels are PRICE return — they exclude dividends, while")
    print("fund NAVs are TOTAL return. The server labels them return_type='price'.")
    print("\nCommit the refreshed data:")
    print("    git add data/index_history.parquet data/index_master.parquet")


if __name__ == "__main__":
    main()
