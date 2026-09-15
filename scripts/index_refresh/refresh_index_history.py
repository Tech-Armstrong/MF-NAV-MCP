"""
scripts/index_refresh/refresh_index_history.py — daily NiftyIndices snapshot
refresh, run by .github/workflows/index-refresh.yml on a schedule.

This is the same pipeline originally built as an Azure Function
(index_refresh_function/), rewritten as a plain script because GitHub Actions
runs it directly with `python`, no Azure deploy/build step involved:

  1. Fetch the latest available NiftyIndices daily snapshot CSV (requests).
  2. Download the current index_history.parquet / index_master.parquet from
     Blob (az://mfnavdata/processed/index_history/, .../index_master/).
  3. Merge in any (ticker, date) rows not already present — only for tickers
     already known in index_master (same policy as Index/append_daily_close.py:
     unknown indices are reported and skipped, never added here — adding one
     requires updating the source workbook and re-running parse_index_xlsx.py
     by hand, then re-uploading index_master.parquet).
  4. Upload the merged parquet back to Blob.

Auth: AZURE_STORAGE_CONNECTION_STRING is read from the environment, which the
GitHub Actions workflow populates from a repo secret — never hardcoded here
and never logged.

Run manually for a local test:
    AZURE_STORAGE_CONNECTION_STRING="..." python scripts/index_refresh/refresh_index_history.py
    AZURE_STORAGE_CONNECTION_STRING="..." python scripts/index_refresh/refresh_index_history.py --dry-run
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
import time
from datetime import datetime, timedelta, date as date_cls

import duckdb
import pandas as pd
import requests
from azure.storage.blob import ContainerClient

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ticker import make_ticker          # noqa: E402  (see ticker.py docstring)

# ── NiftyIndices fetch config ──────────────────────────────────────────────

BASE_URL = "https://www.niftyindices.com/Daily_Snapshot/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.niftyindices.com/reports/daily-reports",
    "Accept": "text/csv,application/octet-stream,*/*",
}

MAX_RETRIES = 3
MAX_DAYS_BACK = 10
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 60

EXPECTED_COLUMNS = [
    "Index Name",
    "Index Date",
    "Open Index Value",
    "High Index Value",
    "Low Index Value",
    "Closing Index Value",
    "Points Change",
    "Change(%)",
    "Volume",
    "Turnover (Rs. Cr.)",
    "P/E",
    "P/B",
    "Div Yield",
]

NAME_COL = "Index Name"
DATE_COL = "Index Date"
CLOSE_COL = "Closing Index Value"

# ── Blob config ─────────────────────────────────────────────────────────────

CONTAINER = "mfnavdata"
HIST_BLOB = "processed/index_history/index_history.parquet"
MASTER_BLOB = "processed/index_master/index_master.parquet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# The azure-storage-blob SDK logs full HTTP request/response headers at INFO,
# which drowns the actual refresh log in noise (values are REDACTED, so this
# is a verbosity problem, not a leak) — quiet it to WARNING.
logging.getLogger("azure").setLevel(logging.WARNING)


# ══════════════════════════════════════════════════════════════════════════
# 1. Fetch
# ══════════════════════════════════════════════════════════════════════════

def get_report_url(d: date_cls) -> str:
    return BASE_URL + f"ind_close_all_{d.strftime('%d%m%Y')}.csv"


def download_report(url: str) -> requests.Response | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Download attempt %d/%d: %s", attempt, MAX_RETRIES, url)
            response = requests.get(
                url, headers=HEADERS, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
            )
            if response.status_code == 200:
                logger.info("Download successful | Size: %s bytes", f"{len(response.content):,}")
                return response
            elif response.status_code == 404:
                logger.info("Report not available for this date (404)")
                return None
            else:
                logger.warning("HTTP %s", response.status_code)
        except requests.exceptions.Timeout:
            logger.warning("Request timed out on attempt %d", attempt)
        except requests.exceptions.RequestException as e:
            logger.warning("Request failed on attempt %d: %s", attempt, e)

        if attempt < MAX_RETRIES:
            wait_time = attempt * 2
            logger.info("Retrying in %d seconds...", wait_time)
            time.sleep(wait_time)

    logger.error("All download attempts failed")
    return None


def validate_and_read_csv(response: requests.Response) -> pd.DataFrame | None:
    try:
        if response.status_code != 200:
            logger.error("Invalid HTTP status: %s", response.status_code)
            return None
        if not response.content:
            logger.error("Downloaded file is empty")
            return None

        df = pd.read_csv(io.BytesIO(response.content))
        logger.info("CSV loaded | Rows: %d | Columns: %d", len(df), len(df.columns))

        missing_columns = [c for c in EXPECTED_COLUMNS if c not in list(df.columns)]
        if missing_columns:
            logger.error("CSV schema validation FAILED | Missing: %s", missing_columns)
            return None

        logger.info("CSV schema validation PASSED")
        return df

    except pd.errors.ParserError as e:
        logger.error("CSV parsing failed: %s", e)
        return None
    except Exception as e:
        logger.error("Unexpected error while reading CSV: %s", e)
        return None


def find_latest_report() -> tuple[date_cls | None, str | None, pd.DataFrame | None]:
    current_date = datetime.today().date() - timedelta(days=1)

    for days_back in range(MAX_DAYS_BACK + 1):
        check_date = current_date - timedelta(days=days_back)
        logger.info("Checking report for %s", check_date)

        url = get_report_url(check_date)
        response = download_report(url)
        if response is None:
            continue

        df = validate_and_read_csv(response)
        if df is None:
            logger.error("Report found but validation failed for %s", check_date)
            continue

        logger.info("Latest valid report found: %s", check_date)
        return check_date, url, df

    logger.error("No valid NIFTY report found in the last %d days.", MAX_DAYS_BACK)
    return None, None, None


# ══════════════════════════════════════════════════════════════════════════
# 2. Parse + merge (mirrors Index/append_daily_close.py's row handling)
# ══════════════════════════════════════════════════════════════════════════

def parse_close(raw) -> float | None:
    """'24287.65' -> 24287.65;  '-' / '' / NaN -> None (not traded that day)."""
    if raw is None:
        return None
    raw = str(raw).strip().replace(",", "")
    if not raw or raw == "-" or raw.lower() == "nan":
        return None
    try:
        v = float(raw)
    except ValueError:
        return None
    return v if v > 0 else None


def rows_from_daily_df(df: pd.DataFrame, report_date: date_cls, known_tickers: set[str]):
    """CSV rows -> (ticker, date, close) tuples, skipping unknown/blank closes."""
    new_rows: list[tuple[str, date_cls, float]] = []
    skipped_unknown: list[str] = []
    skipped_noclose: list[str] = []

    for _, row in df.iterrows():
        name = str(row.get(NAME_COL) or "").strip()
        if not name:
            continue
        ticker = make_ticker(name)
        if ticker not in known_tickers:
            skipped_unknown.append(name)
            continue
        close = parse_close(row.get(CLOSE_COL))
        if close is None:
            skipped_noclose.append(name)
            continue
        new_rows.append((ticker, report_date, close))

    return new_rows, skipped_unknown, skipped_noclose


def merge_history(existing_path: str, new_rows: list[tuple[str, date_cls, float]], out_path: str) -> tuple[int, int]:
    """Union existing history with new_rows, existing wins on (ticker, date)
    duplicates (same policy as append_daily_close.py without --overwrite).
    Writes the merged table to out_path. Returns (before, after) row counts."""
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE VIEW existing AS SELECT * FROM read_parquet('{existing_path}');")
    con.execute("CREATE TABLE incoming(ticker VARCHAR, nav_date DATE, close DOUBLE);")
    con.executemany("INSERT INTO incoming VALUES (?, ?, ?);", new_rows)

    con.execute("""
        CREATE TABLE merged AS
        SELECT ticker, nav_date, close FROM existing
        UNION ALL
        SELECT ticker, nav_date, close FROM incoming i
        WHERE NOT EXISTS (
            SELECT 1 FROM existing e
            WHERE e.ticker = i.ticker AND e.nav_date = i.nav_date
        )
    """)

    before, = con.execute("SELECT COUNT(*) FROM existing").fetchone()
    after, = con.execute("SELECT COUNT(*) FROM merged").fetchone()

    con.execute(
        f"COPY (SELECT * FROM merged ORDER BY ticker, nav_date) "
        f"TO '{out_path}' (FORMAT PARQUET);"
    )
    return before, after


# ══════════════════════════════════════════════════════════════════════════
# 3. Main
# ══════════════════════════════════════════════════════════════════════════

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                     help="Fetch and compute the merge, but do not upload to Blob.")
    args = ap.parse_args()

    # Local/dev convenience only: if a .env file sits next to the repo root,
    # load it so AZURE_STORAGE_CONNECTION_STRING doesn't need exporting by
    # hand. In CI (GitHub Actions) no .env exists, so this is a silent no-op
    # and the workflow's `env:` block (populated from a repo secret) is what
    # actually supplies the variable there.
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".env"))
    except ImportError:
        pass

    logger.info("=" * 60)
    logger.info("INDEX DAILY REFRESH STARTED")
    logger.info("=" * 60)

    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        sys.exit("AZURE_STORAGE_CONNECTION_STRING is not set.")

    report_date, url, df = find_latest_report()
    if df is None:
        logger.error("No report available in the lookback window — nothing to do.")
        sys.exit(1)

    cc = ContainerClient.from_connection_string(conn_str, container_name=CONTAINER)

    tmp_master = "/tmp/index_master.parquet"
    with open(tmp_master, "wb") as f:
        f.write(cc.download_blob(MASTER_BLOB).readall())

    con = duckdb.connect(":memory:")
    known_tickers = {
        r[0] for r in con.execute(
            f"SELECT ticker FROM read_parquet('{tmp_master}')"
        ).fetchall()
    }
    logger.info("index_master tracks %d tickers", len(known_tickers))

    new_rows, skipped_unknown, skipped_noclose = rows_from_daily_df(
        df, report_date, known_tickers
    )

    if not new_rows:
        logger.warning(
            "No usable rows for %s (unknown=%d, no-close=%d) — nothing to append.",
            report_date, len(skipped_unknown), len(skipped_noclose),
        )
        return

    logger.info(
        "%d new rows for %s | skipped unknown=%d, no-close=%d",
        len(new_rows), report_date, len(skipped_unknown), len(skipped_noclose),
    )
    if skipped_unknown:
        sample = ", ".join(skipped_unknown[:8])
        logger.info("Sample unknown indices (not in index_master): %s", sample)

    tmp_existing = "/tmp/index_history_existing.parquet"
    with open(tmp_existing, "wb") as f:
        f.write(cc.download_blob(HIST_BLOB).readall())

    tmp_merged = "/tmp/index_history_merged.parquet"
    before, after = merge_history(tmp_existing, new_rows, tmp_merged)
    logger.info("Merged history: %d -> %d rows (+%d)", before, after, after - before)

    if args.dry_run:
        logger.info("DRY RUN — not uploading. Nothing written to Blob.")
        return

    with open(tmp_merged, "rb") as f:
        cc.upload_blob(name=HIST_BLOB, data=f.read(), overwrite=True)
    logger.info("Uploaded merged index_history.parquet to %s/%s", CONTAINER, HIST_BLOB)

    logger.info("=" * 60)
    logger.info("INDEX DAILY REFRESH COMPLETE — report date %s", report_date)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
