"""
Index/append_daily_close.py  —  append a daily NSE close CSV to the index parquet
=================================================================================

The Weekly Market Pulse Tracker workbook (see Index/parse_index_xlsx.py) builds
the historical series; this script keeps it current from the daily NSE
"Index_close_<date>.csv" export.

Run from the PROJECT ROOT (paths are relative to the current directory):

    python Index/append_daily_close.py                         # newest CSV in Index/
    python Index/append_daily_close.py --csv Index/Index_close_17-aug.csv
    python Index/append_daily_close.py --dry-run               # report, write nothing
    git add data/index_history.parquet

Behaviour
---------
Only indices ALREADY in index_master.parquet are updated. The daily file
carries ~164 indices while the historical workbook has 109; the extras (G-Sec,
Bharat Bond, leveraged/inverse variants, newer sector indices) are reported and
skipped so the index set stays aligned with the history that backs it. To adopt
one, add it to the workbook and re-run parse_index_xlsx.py.

Matching is by TICKER, derived from the index name with the same make_ticker()
the workbook parser uses — verified to line up for all 109.

Re-running with the same file is safe: an existing (ticker, date) row is left
alone unless --overwrite is passed, so a corrected re-export can be applied
deliberately rather than by accident.

CSV layout (NSE daily export)
-----------------------------
    Index Name,Index Date,Open,High,Low,Closing Index Value,Points Change,...
    Nifty 50,17-08-2026,24343.45,...,24287.65,-78.35,...

Only Index Name, Index Date and Closing Index Value are used — the server
stores closes only. Rows whose close is "-" (not traded/computed that day) are
skipped rather than written as zero.
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
from datetime import date, datetime

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from parse_index_xlsx import make_ticker          # noqa: E402  (same derivation)

NAME_COL = "Index Name"
DATE_COL = "Index Date"
CLOSE_COL = "Closing Index Value"

# NSE writes dates as DD-MM-YYYY; a couple of other shapes show up in exports.
_DATE_FORMATS = ("%d-%m-%Y", "%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y")


def parse_date(raw: str) -> date | None:
    raw = (raw or "").strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def parse_close(raw: str) -> float | None:
    """'24287.65' -> 24287.65;  '-' / '' -> None (not traded that day)."""
    raw = (raw or "").strip().replace(",", "")
    if not raw or raw == "-":
        return None
    try:
        v = float(raw)
    except ValueError:
        return None
    return v if v > 0 else None


def find_default_csv() -> str | None:
    here = os.path.dirname(os.path.abspath(__file__))
    files = [f for f in glob.glob(os.path.join(here, "*.csv"))
             if not os.path.basename(f).startswith("~$")]
    return max(files, key=os.path.getmtime) if files else None


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Append a daily NSE index close CSV to data/index_history.parquet.")
    ap.add_argument("--csv", default=None,
                    help="Path to the daily CSV (default: newest .csv in Index/).")
    ap.add_argument("--data-dir", default="./data",
                    help="Directory holding the parquet (default ./data).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Report what would change without writing.")
    ap.add_argument("--overwrite", action="store_true",
                    help="Replace rows that already exist for the same "
                         "(ticker, date) instead of leaving them alone.")
    args = ap.parse_args()

    csv_path = args.csv or find_default_csv()
    if not csv_path or not os.path.exists(csv_path):
        sys.exit("No CSV found. Put the daily NSE export in Index/ or pass --csv PATH.")

    hist_path = os.path.join(args.data_dir, "index_history.parquet")
    master_path = os.path.join(args.data_dir, "index_master.parquet")
    for p in (hist_path, master_path):
        if not os.path.exists(p):
            sys.exit(f"{p} not found. Build it first:  python Index/parse_index_xlsx.py")

    con = duckdb.connect(":memory:")
    known = {r[0] for r in con.execute(
        f"SELECT ticker FROM read_parquet('{master_path}')").fetchall()}
    print(f"Reading {csv_path}")
    print(f"  parquet tracks {len(known)} indices")

    new_rows: list[tuple[str, date, float]] = []
    skipped_unknown, skipped_noclose, bad_date = [], [], 0
    seen_dates: set[date] = set()

    with open(csv_path, encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        if NAME_COL not in (reader.fieldnames or []):
            sys.exit(f"'{NAME_COL}' column not found. Header: {reader.fieldnames}")
        for row in reader:
            name = (row.get(NAME_COL) or "").strip()
            if not name:
                continue
            d = parse_date(row.get(DATE_COL, ""))
            if d is None:
                bad_date += 1
                continue
            ticker = make_ticker(name)
            if ticker not in known:
                skipped_unknown.append(name)
                continue
            close = parse_close(row.get(CLOSE_COL, ""))
            if close is None:
                skipped_noclose.append(name)
                continue
            new_rows.append((ticker, d, close))
            seen_dates.add(d)

    if not new_rows:
        sys.exit("No usable rows found — nothing to append.")

    print(f"  {len(new_rows)} rows for {len(seen_dates)} date(s): "
          f"{', '.join(str(d) for d in sorted(seen_dates))}")
    if skipped_unknown:
        print(f"  skipped {len(skipped_unknown)} index/indices not in the parquet "
              f"(add them to the workbook and re-run parse_index_xlsx.py):")
        for n in skipped_unknown[:8]:
            print(f"      {n}")
        if len(skipped_unknown) > 8:
            print(f"      ... and {len(skipped_unknown) - 8} more")
    if skipped_noclose:
        print(f"  skipped {len(skipped_noclose)} row(s) with no close value")
    if bad_date:
        print(f"  skipped {bad_date} row(s) with an unparseable date")

    # Stage the incoming rows, then decide what is genuinely new.
    con.execute("CREATE TABLE incoming(ticker VARCHAR, nav_date DATE, close DOUBLE);")
    con.executemany("INSERT INTO incoming VALUES (?, ?, ?);", new_rows)
    con.execute(f"CREATE VIEW existing AS SELECT * FROM read_parquet('{hist_path}');")

    dup, = con.execute("""
        SELECT COUNT(*) FROM incoming i
        JOIN existing e ON e.ticker = i.ticker AND e.nav_date = i.nav_date
    """).fetchone()
    before, = con.execute("SELECT COUNT(*) FROM existing").fetchone()

    if dup:
        if args.overwrite:
            print(f"  {dup} row(s) already present — will be REPLACED (--overwrite)")
        else:
            print(f"  {dup} row(s) already present — left unchanged "
                  f"(pass --overwrite to replace)")

    if args.dry_run:
        added = len(new_rows) - (0 if args.overwrite else dup)
        print(f"\nDRY RUN — would take history from {before:,} to "
              f"{before + added:,} rows. Nothing written.")
        return

    # Existing rows win unless --overwrite, in which case incoming wins.
    keep_existing = ("NOT EXISTS (SELECT 1 FROM incoming i WHERE i.ticker = e.ticker "
                     "AND i.nav_date = e.nav_date)") if args.overwrite else "TRUE"
    take_incoming = "TRUE" if args.overwrite else (
        "NOT EXISTS (SELECT 1 FROM existing e WHERE e.ticker = i.ticker "
        "AND e.nav_date = i.nav_date)")

    con.execute(f"""
        CREATE TABLE merged AS
        SELECT ticker, nav_date, close FROM existing e WHERE {keep_existing}
        UNION ALL
        SELECT ticker, nav_date, close FROM incoming i WHERE {take_incoming}
    """)
    after, = con.execute("SELECT COUNT(*) FROM merged").fetchone()

    # Write via a temp file so a failure cannot leave a truncated parquet.
    tmp = hist_path + ".tmp"
    con.execute(f"COPY (SELECT * FROM merged ORDER BY ticker, nav_date) "
                f"TO '{tmp}' (FORMAT PARQUET);")
    os.replace(tmp, hist_path)

    lo, hi = con.execute("SELECT MIN(nav_date), MAX(nav_date) FROM merged").fetchone()
    print(f"\nWrote {hist_path}")
    print(f"  {before:,} -> {after:,} rows  (+{after - before:,})")
    print(f"  date range {lo} -> {hi}")
    print("\nCommit the refreshed data:")
    print("    git add data/index_history.parquet")


if __name__ == "__main__":
    main()
