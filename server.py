"""
NAV Analytics MCP Server (standalone)
=====================================

A self-contained MCP server that exposes Indian mutual fund NAV analytics as
tools, querying parquet data through DuckDB. Point it at local parquet files or
at Azure Blob parquet — the server only ever runs read-only SELECTs and never
fabricates data.

Tools exposed
-------------
    search_funds(query, limit)          fuzzy fund-name -> scheme_code resolver
    get_fund_returns(scheme_codes, period)
                                        point-to-point + CAGR for 1..N funds
    get_fund_returns_between(scheme_codes, start_date, end_date)
                                        same, over an explicit ISO date range
    get_category_returns(category, period, sort_by, ascending, staleness_days)
                                        returns for every fund in a category
    list_categories()                   distinct category names
    list_funds_in_category(category)    schemes in a category

Supported period strings
------------------------
    1W, 2W                weeks
    1M, 3M, 6M, 9M        months
    1Y, 2Y, 3Y, 5Y        years
    YTD                   Jan 1 (of the fund's last-NAV year) -> last NAV
    MTD                   1st of month -> last NAV
    SI                    since inception (earliest NAV -> last NAV)

Every window ENDS at the fund's own last available NAV (its "anchor"), not the
calendar today. This matches how published sources (Value Research / ET Money)
report: the latest NAV may be a day or two behind, and non-trading days have no
NAV.

Expected parquet schema
-----------------------
    nav_history     scheme_code VARCHAR, nav_date DATE, nav DOUBLE
    scheme_master   scheme_code VARCHAR, scheme_name VARCHAR,
                    fund_house VARCHAR, category VARCHAR

Configuration (environment variables)
-------------------------------------
    NAV_HISTORY_PATH     parquet path/glob for NAV history
                         (default: ./data/nav_history.parquet)
    SCHEME_MASTER_PATH   parquet path/glob for scheme master
                         (default: ./data/scheme_master.parquet)
    AZURE_STORAGE_CONNECTION_STRING
                         if set, DuckDB's azure extension is loaded and a secret
                         registered, so the paths above may be az:// URLs, e.g.
                         az://mycontainer/nav/*.parquet

    MCP_TRANSPORT        "stdio" for local Cursor/Claude Desktop use; anything
                         else (default) serves streamable HTTP at /mcp.
    PORT                 HTTP listen port (default 8000; App Service injects this).

    PUBLIC_BASE_URL      Enables OAuth for the claude.ai connector when set to the
                         externally reachable https base of the deployed server,
                         e.g. https://<app>.azurewebsites.net. A self-contained
                         OAuth server (InMemoryOAuthProvider) handles the connector
                         handshake — no external identity provider or app
                         registration needed. If unset, the HTTP server runs
                         UNAUTHENTICATED — fine for local testing, not for public
                         hosting.

Run
---
    python server.py                 # streamable HTTP (for the claude.ai connector)
    MCP_TRANSPORT=stdio python server.py   # stdio (for Cursor / Claude Desktop)

No data yet? Generate a synthetic fixture to smoke-test the server:
    python make_sample_data.py
"""

from __future__ import annotations

import os
import sys
import re
import time
import threading
import statistics
import datetime as _dt
from datetime import date, timedelta
from typing import Optional, Union

import duckdb
from dateutil.relativedelta import relativedelta
from fastmcp import FastMCP

# ── configuration ─────────────────────────────────────────────────────────────

NAV_HISTORY_PATH = os.environ.get("NAV_HISTORY_PATH", "./data/nav_history.parquet")
SCHEME_MASTER_PATH = os.environ.get("SCHEME_MASTER_PATH", "./data/scheme_master.parquet")
AZURE_CONN = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

# Index benchmark parquet. Unlike the NAV paths these default to files COMMITTED
# to the repo (see Index/fetch_index_data.py), so they resolve relative to this file
# rather than the process working directory — MCP clients spawn the server from
# arbitrary directories. Never read from Azure: the data is small and versioned
# with the code.
_HERE = os.path.dirname(os.path.abspath(__file__))
INDEX_HISTORY_PATH = os.environ.get(
    "INDEX_HISTORY_PATH", os.path.join(_HERE, "data", "index_history.parquet"))
INDEX_MASTER_PATH = os.environ.get(
    "INDEX_MASTER_PATH", os.path.join(_HERE, "data", "index_master.parquet"))

# Fund holdings profile: market-cap split, sector/industry breakdown and top
# holdings per fund, produced monthly by holdings_enricher/ and keyed by the NAV
# parquet's exact scheme_name so it joins cleanly with the returns tools.
# Committed alongside the code for now; when more cap categories are added this
# is expected to move to Azure Blob as parquet, like the NAV data.
FUND_HOLDINGS_PATH = os.environ.get(
    "FUND_HOLDINGS_PATH", os.path.join(_HERE, "data", "fund_holdings.json"))

# Month-end risk-free rates for the Sharpe denominator. Committed data rather
# than a constant: the rate is itself as-of a date, so it must pair with the
# NAV window or the result silently mixes two different moments.
RISK_FREE_RATES_PATH = os.environ.get(
    "RISK_FREE_RATES_PATH", os.path.join(_HERE, "data", "risk_free_rates.json"))

# Friendly names -> tickers, so callers can say "NIFTY50" instead of
# "NIFTY_50". Resolution is case-insensitive and strips spaces/underscores.
#
# Tickers are derived from the index names in the source workbook (see
# Index/parse_index_xlsx.py), so they are all NIFTY_*-shaped. Only shorthands
# that a caller would plausibly type need an entry here — list_indices()
# advertises the full set, and any exact ticker works without an alias.
#
# NOTE: the workbook is NSE-only, so there is no BSE Sensex series. SENSEX is
# deliberately absent rather than aliased to a Nifty index, which would answer
# a different question than the one asked.
_INDEX_ALIASES = {
    "NIFTY": "NIFTY_50",
    "NIFTY50": "NIFTY_50",
    "NIFTY100": "NIFTY_100",
    "NIFTY200": "NIFTY_200",
    "NIFTY500": "NIFTY_500",
    "NIFTYNEXT50": "NIFTY_NEXT_50",
    "NEXT50": "NIFTY_NEXT_50",
    "NIFTYMIDCAP50": "NIFTY_MIDCAP_50",
    "MIDCAP50": "NIFTY_MIDCAP_50",
    "NIFTYMIDCAP100": "NIFTY_MIDCAP_100",
    "MIDCAP100": "NIFTY_MIDCAP_100",
    "NIFTYMIDCAP150": "NIFTY_MIDCAP_150",
    "MIDCAP150": "NIFTY_MIDCAP_150",
    "NIFTYSMALLCAP100": "NIFTY_SMALLCAP_100",
    "SMALLCAP100": "NIFTY_SMALLCAP_100",
    "NIFTYSMALLCAP250": "NIFTY_SMALLCAP_250",
    "SMALLCAP250": "NIFTY_SMALLCAP_250",
    "BANKNIFTY": "NIFTY_BANK",
    "NIFTYBANK": "NIFTY_BANK",
    "NIFTYIT": "NIFTY_IT",
    "NIFTYPHARMA": "NIFTY_PHARMA",
    "NIFTYAUTO": "NIFTY_AUTO",
    "NIFTYFMCG": "NIFTY_FMCG",
    "VIX": "INDIA_VIX",
    "INDIAVIX": "INDIA_VIX",
}

# scheme_master.category -> default Beta benchmark ticker, used by
# fund_risk_data when no explicit benchmark_ticker is passed. Keys are
# UPPERCASE to match category values as stored (see list_categories()).
#
# Deliberately starts with just the three unambiguous cap-based categories —
# each has one standard NIFTY-family benchmark that means the same thing
# across every fund in it. Every other category (Large & Mid Cap, Multi Cap,
# Flexi Cap, hybrids, debt, sectoral/thematic, FOF, ...) is left out on
# purpose: guessing a benchmark for a category without one obvious answer is
# worse than surfacing "no default benchmark for this category" and requiring
# an explicit benchmark_ticker. Extend this map as more categories get a
# confirmed standard benchmark.
_CATEGORY_BENCHMARKS = {
    "LARGE CAP": "NIFTY_100",
    "MID CAP": "NIFTY_MIDCAP_150",
    "SMALL CAP": "NIFTY_SMALLCAP_250",
}

_VALID_PERIODS = {
    "1W", "2W",
    "1M", "3M", "6M", "9M",
    "1Y", "2Y", "3Y", "5Y",
    "YTD", "MTD", "SI",
}
# Periods long enough that an annualized CAGR is the meaningful comparison metric.
_LONG_PERIODS = {"2Y", "3Y", "5Y"}


# ── Azure Blob → local parquet (via the azure-storage-blob SDK) ────────────────
#
# We deliberately do NOT use DuckDB's `azure` extension to read az:// URLs. On
# locked-down hosts (Azure App Service's Linux container) that extension's bundled
# libcurl can't locate the CA bundle and fails with "Problem with the SSL CA cert".
# Instead we download the parquet with the azure-storage-blob SDK (which verifies
# TLS against certifi and works everywhere), cache it under a local dir, and hand
# DuckDB plain local files. Robust and cert-issue-free.

_LOCAL_CACHE_DIR = os.environ.get("BLOB_CACHE_DIR", "/tmp/navdata")


def _parse_az_uri(uri: str) -> tuple[str, str]:
    """'az://container/path/to/blob' -> ('container', 'path/to/blob')."""
    rest = re.sub(r"^(az|azure)://", "", uri)
    container, _, blob_path = rest.partition("/")
    return container, blob_path


def _download_az_to_local(uri: str) -> str:
    """
    Download the blob(s) named by an az:// URI to the local cache and return a
    LOCAL path (or glob) DuckDB can read. Handles two shapes:
      - a single blob:  az://c/processed/scheme_master.parquet
      - a glob:         az://c/processed/nav_history/year=*/*.parquet
    For a glob we list blobs under the fixed prefix (everything before the first
    wildcard), download each *.parquet, and return a local recursive glob.
    """
    from azure.storage.blob import ContainerClient

    container, blob_path = _parse_az_uri(uri)
    cc = ContainerClient.from_connection_string(AZURE_CONN, container_name=container)

    has_glob = "*" in blob_path
    # Fixed prefix = the path up to the first path-segment containing a wildcard.
    prefix = blob_path
    if has_glob:
        segments = blob_path.split("/")
        keep = []
        for seg in segments:
            if "*" in seg:
                break
            keep.append(seg)
        prefix = "/".join(keep)
        if prefix and not prefix.endswith("/"):
            prefix += "/"

    dest_root = os.path.join(_LOCAL_CACHE_DIR, container)
    downloaded = 0
    for blob in cc.list_blobs(name_starts_with=prefix):
        if not blob.name.endswith(".parquet"):
            continue
        local_path = os.path.join(dest_root, blob.name.replace("/", os.sep))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(cc.download_blob(blob.name).readall())
        downloaded += 1

    if downloaded == 0:
        raise RuntimeError(
            f"No .parquet blobs found under az://{container}/{prefix} "
            f"(from URI {uri}). Check the container name and path."
        )

    if has_glob:
        # Root the recursive glob at the fixed PREFIX subdir (e.g. nav_history/),
        # not the container root — otherwise it would also match sibling datasets
        # like scheme_master.parquet, and DuckDB would try to union mismatched
        # schemas ("column nav_date ... could not be found"). The glob covers the
        # year=*/*.parquet layout regardless of depth.
        prefix_local = os.path.join(dest_root, prefix.rstrip("/").replace("/", os.sep))
        return os.path.join(prefix_local, "**", "*.parquet")
    return os.path.join(dest_root, blob_path.replace("/", os.sep))


# ── connection (single, long-lived, read-only) ─────────────────────────────────

def _build_connection() -> duckdb.DuckDBPyConnection:
    """
    Open ONE in-memory DuckDB connection for the life of the server and expose
    the two parquet sources as read-only views. A stdio MCP server is a
    long-lived process handling many calls, so the connection is created once at
    import time and never closed per call — the failure mode of closing a shared
    handle mid-session simply cannot occur here.
    """
    con = duckdb.connect(database=":memory:")

    # DuckDB can't bind prepared parameters inside CREATE VIEW, so paths are
    # inlined as string literals with single quotes doubled to stay safe.
    def _lit(s: str) -> str:
        return "'" + s.replace("'", "''") + "'"

    if AZURE_CONN:
        # Download from Blob via the SDK, then point DuckDB at local files.
        nav_path = _download_az_to_local(NAV_HISTORY_PATH)
        scheme_path = _download_az_to_local(SCHEME_MASTER_PATH)
    else:
        # Local mode: fail early and clearly if the parquet is missing.
        nav_path, scheme_path = NAV_HISTORY_PATH, SCHEME_MASTER_PATH
        missing = [p for p in (nav_path, scheme_path) if not os.path.exists(p)]
        if missing:
            sys.stderr.write(
                "NAV MCP: parquet not found: " + ", ".join(missing) + "\n"
                "Set NAV_HISTORY_PATH / SCHEME_MASTER_PATH, provide "
                "AZURE_STORAGE_CONNECTION_STRING for az:// paths, or run "
                "`python make_sample_data.py` to create a local fixture.\n"
            )

    con.execute(
        f"CREATE OR REPLACE VIEW nav_history AS "
        f"SELECT * FROM read_parquet({_lit(nav_path)});"
    )
    con.execute(
        f"CREATE OR REPLACE VIEW scheme_master AS "
        f"SELECT * FROM read_parquet({_lit(scheme_path)});"
    )

    # Index benchmarks: committed local parquet, never Azure. Optional — if the
    # files are absent the fund tools must still work, so the index views are
    # simply not created and the index tools report the situation rather than
    # the whole server failing to import.
    if os.path.exists(INDEX_HISTORY_PATH) and os.path.exists(INDEX_MASTER_PATH):
        con.execute(
            f"CREATE OR REPLACE VIEW index_history AS "
            f"SELECT * FROM read_parquet({_lit(INDEX_HISTORY_PATH)});"
        )
        con.execute(
            f"CREATE OR REPLACE VIEW index_master AS "
            f"SELECT * FROM read_parquet({_lit(INDEX_MASTER_PATH)});"
        )
    else:
        sys.stderr.write(
            "NAV MCP: index parquet not found at "
            f"{INDEX_HISTORY_PATH} / {INDEX_MASTER_PATH}; index tools will be "
            "unavailable. Run `python Index/fetch_index_data.py` to create them.\n"
        )
    return con


# ── connection handle + periodic blob refresh ─────────────────────────────────
#
# The parquet is DOWNLOADED to local disk at startup (see _download_az_to_local),
# so without a refresh the process serves whatever the blob held at import time
# for its entire life — new NAVs uploaded afterwards stay invisible until someone
# restarts the app. A background thread therefore rebuilds the connection every
# BLOB_REFRESH_SECONDS and swaps it in atomically.
#
# Why a cursor per call: swapping _CON mid-call would break the tools that create
# a TEMP TABLE and then query it, and DuckDB temp tables are per-connection, so a
# single shared handle also lets concurrent calls clobber each other's _targets.
# con.cursor() gives each call its own temp namespace over the same database, and
# an in-flight cursor keeps its parent alive across a swap.

_CON_LOCK = threading.Lock()
_CON = _build_connection()


def _db() -> duckdb.DuckDBPyConnection:
    """A private cursor over the current connection. Use once per tool call."""
    with _CON_LOCK:
        return _CON.cursor()


def _blob_signature() -> Optional[tuple]:
    """
    Cheap change-detector: (name, last_modified, size) for every NAV parquet.
    Returns None if the signature can't be read, which the caller treats as
    "don't know" and skips the refresh rather than risking a pointless download.
    """
    if not AZURE_CONN:
        return None
    try:
        from azure.storage.blob import ContainerClient
        sig = []
        for uri in (NAV_HISTORY_PATH, SCHEME_MASTER_PATH):
            if not re.match(r"^(az|azure)://", uri):
                continue
            container, blob_path = _parse_az_uri(uri)
            prefix = blob_path.split("*")[0].rsplit("/", 1)[0] if "*" in blob_path else blob_path
            cc = ContainerClient.from_connection_string(AZURE_CONN, container_name=container)
            for b in cc.list_blobs(name_starts_with=prefix):
                if b.name.endswith(".parquet"):
                    sig.append((b.name, str(b.last_modified), b.size))
        return tuple(sorted(sig))
    except Exception as exc:
        sys.stderr.write(f"NAV MCP: blob signature check failed: {exc}\n")
        return None


# Seed the signature from the blob just loaded, so the first scheduled check is
# a no-op unless the blob actually moved in the meantime.
_LAST_SIGNATURE = _blob_signature()
_LAST_RELOAD = _dt.datetime.now(_dt.timezone.utc)


def _refresh_once() -> bool:
    """Rebuild and swap the connection if the blob changed. True if swapped."""
    global _CON, _LAST_SIGNATURE, _LAST_RELOAD

    sig = _blob_signature()
    if sig is None or sig == _LAST_SIGNATURE:
        return False

    # Built OUTSIDE the lock: the download is slow and touches no shared state,
    # so in-flight calls keep serving the old data while it runs.
    new_con = _build_connection()
    with _CON_LOCK:
        old, _CON = _CON, new_con
        _LAST_SIGNATURE = sig
        _LAST_RELOAD = _dt.datetime.now(_dt.timezone.utc)

    # Deliberately NOT old.close(): a cursor handed out just before the swap may
    # still be mid-query. Dropping the reference lets the GC reclaim it once the
    # last cursor releases it.
    del old
    sys.stderr.write(f"NAV MCP: blob changed, data reloaded at {_LAST_RELOAD:%Y-%m-%d %H:%M}Z\n")
    return True


def _refresh_loop(interval_s: int) -> None:
    while True:
        time.sleep(interval_s)
        try:
            _refresh_once()
        except Exception as exc:
            # A failed refresh must never take the server down or drop the data
            # it is already serving — stale beats dead.
            sys.stderr.write(f"NAV MCP: refresh failed, keeping existing data: {exc}\n")


BLOB_REFRESH_SECONDS = int(os.environ.get("BLOB_REFRESH_SECONDS", "14400"))  # 4h
if BLOB_REFRESH_SECONDS > 0 and AZURE_CONN:
    threading.Thread(
        target=_refresh_loop, args=(BLOB_REFRESH_SECONDS,),
        daemon=True, name="blob-refresh",
    ).start()


# ── period resolution ──────────────────────────────────────────────────────────

def _resolve_window(period: str, anchor: date, inception: date) -> tuple[date, date, str]:
    """
    Return (start_target, end_target, start_snap) for a period anchored to the
    fund's last-NAV date.

    start_snap tells the caller which trading day to snap the start to:
        "before" -> latest NAV on/before start_target (walk back over holidays);
                    used for trailing windows so we capture a FULL period.
        "after"  -> earliest NAV on/after start_target (walk forward); used for
                    calendar-anchored starts (YTD/MTD) and inception (SI).

    end_target is always the anchor; the end NAV is the latest NAV on/before it.
    Raises ValueError for unrecognised periods.
    """
    p = period.upper().strip()

    if p == "YTD":
        return date(anchor.year, 1, 1), anchor, "after"
    if p == "MTD":
        return date(anchor.year, anchor.month, 1), anchor, "after"
    if p == "SI":
        return inception, anchor, "after"

    if p.endswith("W") and p[:-1].isdigit():
        return anchor - timedelta(weeks=int(p[:-1])), anchor, "before"
    if p.endswith("M") and p[:-1].isdigit():
        return anchor - relativedelta(months=int(p[:-1])), anchor, "before"
    if p.endswith("Y") and p[:-1].isdigit():
        return anchor - relativedelta(years=int(p[:-1])), anchor, "before"

    raise ValueError(
        f"Unrecognised period '{period}'. Valid: {', '.join(sorted(_VALID_PERIODS))}"
    )


# ── returns math ────────────────────────────────────────────────────────────────

def _absolute_return(nav_start: float, nav_end: float) -> Optional[float]:
    if not nav_start:
        return None
    return round((nav_end - nav_start) / nav_start * 100, 4)


def _cagr(nav_start: float, nav_end: float,
          d_start: date, d_end: date, period: str) -> Optional[float]:
    """
    Annualized CAGR (%) over the REALIZED window, using actual day-count.
    Returned only when the window represents more than a year — for <=1Y periods
    an annualized figure is misleading, so it's None.

    For a CUSTOM date range (period == "CUSTOM") there is no named window to key
    off, so the test is purely the realized duration — the same rule SI/YTD use.
    """
    years = (d_end - d_start).days / 365.25
    long_enough = (
        period in _LONG_PERIODS
        or (period in {"SI", "YTD", "CUSTOM"} and years > 1.0)
    )
    if not long_enough or not nav_start or years <= 0:
        return None
    return round(((nav_end / nav_start) ** (1 / years) - 1) * 100, 4)


# ── core: point-to-point returns for many funds (set-based) ─────────────────────

def _compute_returns(scheme_codes: list[str], period: str,
                     window: Optional[tuple[date, date]] = None) -> list[dict]:
    """
    Set-based returns for a list of scheme_codes. Regardless of list length this
    issues a constant handful of queries (metadata, anchors, targets, start
    NAVs, end NAVs) rather than looping per fund.

    `window` overrides the named period with an explicit (start_date, end_date)
    applied to every fund — the caller passes period="CUSTOM". Named periods
    derive their window per fund from that fund's own anchor; a custom window is
    the same absolute pair for all of them, which is the whole point of asking
    for one.
    """
    p = period.upper().strip()
    if window is None and p not in _VALID_PERIODS:
        raise ValueError(
            f"Unrecognised period '{period}'. Valid: {', '.join(sorted(_VALID_PERIODS))}"
        )

    codes = list(dict.fromkeys(scheme_codes))  # de-dupe, preserve order
    if not codes:
        return []

    ph = ", ".join(["?"] * len(codes))

    # 1) metadata for all requested codes
    con = _db()  # one cursor per call: private TEMP namespace
    meta_rows = con.execute(
        f"""SELECT scheme_code, scheme_name, fund_house, category
            FROM scheme_master WHERE scheme_code IN ({ph})""",
        codes,
    ).fetchall()
    meta = {r[0]: {"scheme_name": r[1], "fund_house": r[2], "category": r[3]}
            for r in meta_rows}

    # 2) per-fund anchor (last NAV) and inception (first NAV), one grouped query
    anchor_rows = con.execute(
        f"""SELECT scheme_code, MAX(nav_date), MIN(nav_date)
            FROM nav_history WHERE scheme_code IN ({ph})
            GROUP BY scheme_code""",
        codes,
    ).fetchall()
    anchors = {r[0]: (r[1], r[2]) for r in anchor_rows}  # code -> (anchor, inception)

    def _row(code, **extra):
        m = meta.get(code, {})
        base = {
            "scheme_code": code,
            "scheme_name": m.get("scheme_name"),
            "fund_house": m.get("fund_house"),
            "category": m.get("category"),
            "start_nav_date": None, "start_nav": None,
            "end_nav_date": None, "end_nav": None,
            "return_pct": None, "cagr_pct": None, "error": None,
        }
        base.update(extra)
        return base

    # 3) build a targets table; funds missing metadata or NAVs get an error row
    targets = []            # (code, start_target, end_target)
    results = {}            # code -> row dict (order restored at the end)
    start_snap = None
    for code in codes:
        if code not in meta:
            results[code] = _row(code, error=f"scheme_code '{code}' not found in scheme_master")
            continue
        if code not in anchors or anchors[code][0] is None:
            results[code] = _row(code, error="no NAV data for this scheme")
            continue
        if window is not None:
            # Explicit range: same absolute dates for every fund. "before" snaps
            # the start back over holidays so the full window is captured, and
            # the end resolves to the latest NAV on/before end_date — matching
            # the trailing-window convention.
            s_target, e_target, snap = window[0], window[1], "before"
        else:
            anchor, inception = anchors[code]
            s_target, e_target, snap = _resolve_window(p, anchor, inception)
        start_snap = snap  # same for every fund in a single call
        targets.append((code, s_target, e_target))

    start_navs, end_navs = {}, {}
    if targets:
        con.execute("CREATE OR REPLACE TEMP TABLE _targets("
                    "scheme_code VARCHAR, start_target DATE, end_target DATE);")
        con.executemany("INSERT INTO _targets VALUES (?, ?, ?);", targets)

        # 4) start NAV per fund — direction depends on the window type
        if start_snap == "before":
            start_sql = """
                SELECT t.scheme_code, n.nav_date, n.nav
                FROM _targets t
                JOIN nav_history n
                  ON n.scheme_code = t.scheme_code AND n.nav_date <= t.start_target
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY t.scheme_code ORDER BY n.nav_date DESC) = 1
            """
        else:  # "after"
            start_sql = """
                SELECT t.scheme_code, n.nav_date, n.nav
                FROM _targets t
                JOIN nav_history n
                  ON n.scheme_code = t.scheme_code AND n.nav_date >= t.start_target
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY t.scheme_code ORDER BY n.nav_date ASC) = 1
            """
        for c, d, v in con.execute(start_sql).fetchall():
            start_navs[c] = (d, v)

        # 5) end NAV per fund — latest NAV on/before the anchor
        end_sql = """
            SELECT t.scheme_code, n.nav_date, n.nav
            FROM _targets t
            JOIN nav_history n
              ON n.scheme_code = t.scheme_code AND n.nav_date <= t.end_target
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY t.scheme_code ORDER BY n.nav_date DESC) = 1
        """
        for c, d, v in con.execute(end_sql).fetchall():
            end_navs[c] = (d, v)

    # assemble
    for code, s_target, _e in targets:
        s = start_navs.get(code)
        e = end_navs.get(code)
        if not s:
            results[code] = _row(code,
                                 end_nav_date=e[0] if e else None,
                                 end_nav=e[1] if e else None,
                                 error=f"no NAV on/around start target {s_target}")
            continue
        if not e:
            results[code] = _row(code, start_nav_date=s[0], start_nav=s[1],
                                 error="no NAV on/before anchor")
            continue
        results[code] = _row(
            code,
            start_nav_date=s[0], start_nav=s[1],
            end_nav_date=e[0], end_nav=e[1],
            return_pct=_absolute_return(s[1], e[1]),
            cagr_pct=_cagr(s[1], e[1], s[0], e[0], p),
        )

    return [results[c] for c in codes]


# ── core: point-to-point returns for indices ───────────────────────────────────

def _resolve_ticker(t: str) -> str:
    """'nifty 50' / 'NIFTY50' / 'Nifty_50' -> 'NIFTY_50'. Unknown values pass
    through unchanged so an exact ticker works even if it is not aliased."""
    key = re.sub(r"[\s_-]+", "", t.strip().upper())
    return _INDEX_ALIASES.get(key, t.strip())


def _index_available() -> bool:
    try:
        _db().execute("SELECT 1 FROM index_master LIMIT 1;")
        return True
    except Exception:
        return False


def _compute_index_returns(tickers: list[str], period: str,
                           window: Optional[tuple[date, date]] = None) -> list[dict]:
    """
    Index counterpart of _compute_returns, sharing the same window resolution
    (_resolve_window) and the same math (_absolute_return, _cagr) so index and
    fund returns are computed identically and stay comparable.

    It is a separate function rather than a parameterized version of
    _compute_returns because the two read different tables with different column
    names and return differently-shaped rows; sharing the maths while keeping the
    queries explicit is clearer than one function templating both schemas.
    """
    p = period.upper().strip()
    if window is None and p not in _VALID_PERIODS:
        raise ValueError(
            f"Unrecognised period '{period}'. Valid: {', '.join(sorted(_VALID_PERIODS))}"
        )

    resolved = list(dict.fromkeys(_resolve_ticker(t) for t in tickers))
    if not resolved:
        return []

    ph = ", ".join(["?"] * len(resolved))

    con = _db()  # one cursor per call: private TEMP namespace
    meta_rows = con.execute(
        f"SELECT ticker, index_name FROM index_master WHERE ticker IN ({ph})",
        resolved,
    ).fetchall()
    meta = {r[0]: r[1] for r in meta_rows}

    anchor_rows = con.execute(
        f"""SELECT ticker, MAX(nav_date), MIN(nav_date)
            FROM index_history WHERE ticker IN ({ph})
            GROUP BY ticker""",
        resolved,
    ).fetchall()
    anchors = {r[0]: (r[1], r[2]) for r in anchor_rows}

    def _row(ticker, **extra):
        base = {
            "ticker": ticker,
            "index_name": meta.get(ticker),
            "start_date": None, "start_close": None,
            "end_date": None, "end_close": None,
            "return_pct": None, "cagr_pct": None, "error": None,
        }
        base.update(extra)
        return base

    targets, results, start_snap = [], {}, None
    for ticker in resolved:
        if ticker not in meta:
            known = ", ".join(sorted(meta.keys()) or _INDEX_ALIASES.values())
            results[ticker] = _row(
                ticker,
                error=f"unknown index '{ticker}'. Use list_indices() to see "
                      f"available tickers.")
            continue
        if ticker not in anchors or anchors[ticker][0] is None:
            results[ticker] = _row(ticker, error="no price history for this index")
            continue
        anchor, inception = anchors[ticker]
        if window is not None:
            s_target, e_target, snap = window[0], window[1], "before"
        else:
            s_target, e_target, snap = _resolve_window(p, anchor, inception)
        start_snap = snap
        targets.append((ticker, s_target, e_target))

    start_rows, end_rows = {}, {}
    if targets:
        con.execute("CREATE OR REPLACE TEMP TABLE _idx_targets("
                    "ticker VARCHAR, start_target DATE, end_target DATE);")
        con.executemany("INSERT INTO _idx_targets VALUES (?, ?, ?);", targets)

        order = "DESC" if start_snap == "before" else "ASC"
        op = "<=" if start_snap == "before" else ">="
        for t, d, v in con.execute(f"""
            SELECT t.ticker, h.nav_date, h.close
            FROM _idx_targets t
            JOIN index_history h
              ON h.ticker = t.ticker AND h.nav_date {op} t.start_target
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY t.ticker ORDER BY h.nav_date {order}) = 1
        """).fetchall():
            start_rows[t] = (d, v)

        for t, d, v in con.execute("""
            SELECT t.ticker, h.nav_date, h.close
            FROM _idx_targets t
            JOIN index_history h
              ON h.ticker = t.ticker AND h.nav_date <= t.end_target
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY t.ticker ORDER BY h.nav_date DESC) = 1
        """).fetchall():
            end_rows[t] = (d, v)

    for ticker, s_target, _e in targets:
        s, e = start_rows.get(ticker), end_rows.get(ticker)
        if not s:
            results[ticker] = _row(
                ticker,
                end_date=e[0] if e else None, end_close=e[1] if e else None,
                error=f"no close on/around start target {s_target}")
            continue
        if not e:
            results[ticker] = _row(ticker, start_date=s[0], start_close=s[1],
                                   error="no close on/before anchor")
            continue
        results[ticker] = _row(
            ticker,
            start_date=s[0], start_close=s[1],
            end_date=e[0], end_close=e[1],
            return_pct=_absolute_return(s[1], e[1]),
            cagr_pct=_cagr(s[1], e[1], s[0], e[0], p),
        )

    return [results[t] for t in resolved]


def _index_series(
    ticker: str,
    start_date: date,
    end_date: date,
    frequency: str = "monthly",
    con=None,
) -> list[dict]:
    """
    Index close series for one ticker between two dates. Index counterpart of
    _nav_series — same "last point on/before each month-end" convention, so a
    fund's monthly NAV series and a benchmark's monthly close series line up
    month-for-month for Beta (Cov(fund, benchmark) / Var(benchmark)).

    Returns oldest-first [{nav_date, close}, ...]. Ticker is expected already
    resolved (see _resolve_ticker); an unknown ticker simply yields no rows.
    """
    con = con or _db()
    freq = (frequency or "monthly").lower().strip()

    if freq == "daily":
        rows = con.execute(
            """SELECT nav_date, close FROM index_history
               WHERE ticker = ? AND nav_date BETWEEN ? AND ?
               ORDER BY nav_date""",
            [ticker, start_date, end_date],
        ).fetchall()
    elif freq == "monthly":
        rows = con.execute(
            """SELECT nav_date, close FROM index_history
               WHERE ticker = ? AND nav_date BETWEEN ? AND ?
               QUALIFY ROW_NUMBER() OVER (
                   PARTITION BY date_trunc('month', nav_date)
                   ORDER BY nav_date DESC) = 1
               ORDER BY nav_date""",
            [ticker, start_date, end_date],
        ).fetchall()
    else:
        raise ValueError(
            f"frequency must be 'daily' or 'monthly', got {frequency!r}.")

    return [{"nav_date": r[0], "close": r[1]} for r in rows]


# ── fund holdings profile (market cap / sector exposure) ───────────────────────
#
# Loaded once at import into a plain dict — it is a few MB of JSON, read-only,
# and every lookup is a dict hit, so there is nothing to gain from putting it in
# DuckDB. Absent file is not fatal: the holdings tools report it and the rest of
# the server works unchanged.

def _load_fund_holdings() -> dict:
    if not os.path.exists(FUND_HOLDINGS_PATH):
        sys.stderr.write(
            f"NAV MCP: fund holdings not found at {FUND_HOLDINGS_PATH}; "
            "holdings tools will be unavailable. Generate it with "
            "holdings_enricher/main.py --json.\n"
        )
        return {}
    try:
        import json as _json
        with open(FUND_HOLDINGS_PATH, encoding="utf-8") as fh:
            return _json.load(fh)
    except Exception as exc:                       # malformed / unreadable
        sys.stderr.write(f"NAV MCP: could not read fund holdings: {exc}\n")
        return {}


HOLDINGS = _load_fund_holdings()
# scheme_name -> canonical key, for case/space-insensitive lookup.
_HOLDINGS_INDEX = {
    re.sub(r"\s+", " ", k).strip().lower(): k
    for k in (HOLDINGS.get("funds") or {})
}


def _holdings_available() -> bool:
    return bool(HOLDINGS.get("funds"))


def _find_fund_holdings(fund_name: str) -> tuple[Optional[str], Optional[dict]]:
    """
    Resolve a fund name to its holdings entry.

    Keys are the NAV parquet's exact scheme_name, so a code path that already
    has a scheme_name (search_funds, get_fund_returns) hits directly. Falls back
    to a case-insensitive match, then to a unique substring match so a caller
    can pass a shortened name.
    """
    funds = HOLDINGS.get("funds") or {}
    if fund_name in funds:
        return fund_name, funds[fund_name]

    norm = re.sub(r"\s+", " ", fund_name).strip().lower()
    if norm in _HOLDINGS_INDEX:
        key = _HOLDINGS_INDEX[norm]
        return key, funds[key]

    # Unique substring match — ambiguous input is an error, not a guess, so a
    # partial name that hits several funds returns none of them.
    hits = [orig for low, orig in _HOLDINGS_INDEX.items() if norm in low]
    if len(hits) == 1:
        return hits[0], funds[hits[0]]
    return None, None


# ── auth (optional; enabled when PUBLIC_BASE_URL is set) ────────────────────────

def _build_auth():
    """
    Return an OAuth provider for the claude.ai custom-connector handshake, or None.

    claude.ai's "Add custom connector" flow performs an OAuth 2.0 exchange. It does
    NOT support Dynamic Client Registration — the connector UI requires you to paste
    a pre-existing OAuth Client ID (and Secret). So we stand up fastmcp's
    self-contained InMemoryOAuthProvider (its own authorization server — no external
    identity provider, no Azure app registration, no scope config) and pre-register
    ONE fixed client from env vars. You paste that same client_id / client_secret
    into claude.ai when adding the connector.

    Required env vars to enable auth:
        PUBLIC_BASE_URL     externally reachable https base, e.g.
                            https://<app>.azurewebsites.net (OAuth endpoints are
                            advertised relative to it).
        OAUTH_CLIENT_ID     the client id you'll paste into claude.ai.
        OAUTH_CLIENT_SECRET the client secret you'll paste into claude.ai.
    Optional:
        OAUTH_REDIRECT_URIS space-separated allowed redirect URIs; defaults to
                            claude.ai's connector callback.

    If PUBLIC_BASE_URL is unset the server runs UNAUTHENTICATED (local stdio / HTTP
    smoke tests, and the existing mcp_client_test.py, rely on this). If
    PUBLIC_BASE_URL is set but the client id/secret are not, we fail fast with a
    clear message rather than booting an unusable connector.

    Note: registrations/tokens live in memory. The pre-registered client is
    re-seeded on every startup (so it survives restarts), but issued access tokens
    do not — a restart makes teammates click "reconnect" once. For durable tokens
    or real per-user identity + revocation, swap in a hosted provider (Google/
    GitHub/WorkOS) later — same call site, only this function changes.
    """
    base_url = os.environ.get("PUBLIC_BASE_URL")
    if not base_url:
        return None

    client_id = os.environ.get("OAUTH_CLIENT_ID")
    client_secret = os.environ.get("OAUTH_CLIENT_SECRET")
    if not client_id or not client_secret:
        sys.stderr.write(
            "NAV MCP: PUBLIC_BASE_URL is set (OAuth on) but OAUTH_CLIENT_ID / "
            "OAUTH_CLIENT_SECRET are not. Set both, then paste the same values "
            "into claude.ai's Add-custom-connector form.\n"
        )
        raise SystemExit(1)

    redirect_uris = os.environ.get(
        "OAUTH_REDIRECT_URIS", "https://claude.ai/api/mcp/auth_callback"
    ).split()

    from fastmcp.server.auth.providers.in_memory import InMemoryOAuthProvider
    from mcp.shared.auth import OAuthClientInformationFull

    provider = InMemoryOAuthProvider(base_url=base_url)
    # Pre-register the fixed client so claude.ai (which doesn't do Dynamic Client
    # Registration) can authenticate with the id/secret you paste into its UI.
    provider.clients[client_id] = OAuthClientInformationFull(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uris=redirect_uris,
        grant_types=["authorization_code", "refresh_token"],
        response_types=["code"],
        token_endpoint_auth_method="client_secret_post",
    )
    return provider


# ── MCP app + tools ─────────────────────────────────────────────────────────────

mcp = FastMCP("nav-analytics", auth=_build_auth())


@mcp.tool()
def search_funds(query: str, limit: int = 10) -> dict:
    """Resolve a plain-English fund name to scheme_code(s).

    Fuzzy, word-order-insensitive match against scheme_name: every whitespace
    token in `query` must appear somewhere in the name. Returns ranked candidates
    with their scheme_code, fund_house and category. Plan variants (Regular vs
    Direct, Growth vs IDCW) come back as distinct candidates so the caller can
    pick an exact scheme_code. Use this first when the user names a fund; feed the
    resulting scheme_code into get_fund_returns.

    Args:
        query: Free-text fund name, e.g. "HDFC Balanced Advantage".
        limit: Max candidates to return (default 10).
    """
    tokens = [t for t in re.split(r"\s+", query.strip().lower()) if t]
    if not tokens:
        return {"query": query, "count": 0, "candidates": []}

    where = " AND ".join(["lower(scheme_name) LIKE ?"] * len(tokens))
    params = [f"%{t}%" for t in tokens]
    rows = _db().execute(
        f"""SELECT scheme_code, scheme_name, fund_house, category
            FROM scheme_master WHERE {where}
            ORDER BY length(scheme_name), scheme_name""",
        params,
    ).fetchall()

    q_norm = query.strip().lower()
    candidates = []
    for code, name, house, cat in rows[:limit]:
        nm = (name or "").lower()
        score = 1.0 if nm == q_norm else (0.85 if nm.startswith(q_norm) else 0.7)
        candidates.append({
            "scheme_code": code, "scheme_name": name,
            "fund_house": house, "category": cat, "score": round(score, 2),
        })
    return {"query": query, "count": len(candidates), "candidates": candidates}


@mcp.tool()
def get_fund_returns(scheme_codes: Union[str, list[str]], period: str) -> dict:
    """Point-to-point return and CAGR for one or many funds over a named period.

    Each fund's window ends at its own latest available NAV. return_pct is the
    absolute point-to-point return; cagr_pct is the annualized return and is only
    populated for windows longer than a year (2Y/3Y/5Y, and SI when the fund is
    older than a year) — for shorter windows it is null. Resolve names to
    scheme_codes with search_funds first.

    Args:
        scheme_codes: A single scheme_code or a list of them.
        period: One of 1W,2W,1M,3M,6M,9M,1Y,2Y,3Y,5Y,YTD,MTD,SI.
    """
    if isinstance(scheme_codes, str):
        scheme_codes = [scheme_codes]
    results = _compute_returns(scheme_codes, period)
    return {
        "period": period.upper().strip(),
        "period_end": "per-fund-last-nav",
        "results": results,
    }


@mcp.tool()
def get_fund_returns_between(
    scheme_codes: Union[str, list[str]],
    start_date: str,
    end_date: str,
) -> dict:
    """Point-to-point return and CAGR over an EXPLICIT date range.

    Use this when the user names actual dates ("returns from March 2024 to June
    2025"); use get_fund_returns for the standard trailing windows (1Y, 3Y, YTD…).
    Both dates are ISO YYYY-MM-DD and apply to every fund in the list.

    The start snaps back to the latest NAV on/before start_date and the end to
    the latest NAV on/before end_date, so the realized window may be a day or two
    narrower than requested — non-trading days have no NAV. The actual dates used
    are always returned as start_nav_date / end_nav_date; read those, not the
    requested ones, when reporting the window. cagr_pct is populated only when the
    realized window exceeds a year.

    A start_date before a fund's inception yields an error row for that fund
    rather than silently starting at inception, which would answer a different
    question than the one asked.

    Args:
        scheme_codes: A single scheme_code or a list of them.
        start_date: Window start, ISO YYYY-MM-DD.
        end_date: Window end, ISO YYYY-MM-DD.
    """
    try:
        d_start = date.fromisoformat(start_date.strip())
        d_end = date.fromisoformat(end_date.strip())
    except ValueError as exc:
        raise ValueError(
            f"start_date and end_date must be ISO YYYY-MM-DD dates "
            f"(got '{start_date}' and '{end_date}'): {exc}"
        ) from None

    if d_start >= d_end:
        raise ValueError(
            f"start_date ({d_start}) must be earlier than end_date ({d_end})."
        )

    if isinstance(scheme_codes, str):
        scheme_codes = [scheme_codes]
    results = _compute_returns(scheme_codes, "CUSTOM", window=(d_start, d_end))
    return {
        "period": "CUSTOM",
        "start_date": str(d_start),
        "end_date": str(d_end),
        "period_end": "explicit-end-date",
        "results": results,
    }


@mcp.tool()
def get_category_returns(
    category: str,
    period: str,
    sort_by: str = "return_pct",
    ascending: bool = False,
    staleness_days: int = 7,
) -> dict:
    """Returns for every fund in a category, ranked, with a staleness guard.

    Because each fund anchors to its own last NAV, funds can have different as-of
    dates. This flags any fund whose last NAV lags the category's most-recent
    as-of date by more than `staleness_days`, keeps it in the results list marked
    stale, and EXCLUDES it (and any errored/None-return fund) from avg_return_pct
    and avg_cagr_pct so the averages never blend mismatched dates.

    Args:
        category: Category name (case-insensitive).
        period: One of 1W,2W,1M,3M,6M,9M,1Y,2Y,3Y,5Y,YTD,MTD,SI.
        sort_by: return_pct | cagr_pct | scheme_name | fund_house | start_nav | end_nav.
        ascending: Sort direction (default False = best/highest first).
        staleness_days: Max allowed lag from the category as-of date (default 7).
    """
    rows = _db().execute(
        """SELECT scheme_code FROM scheme_master
           WHERE UPPER(category) = UPPER(?) ORDER BY scheme_name""",
        [category],
    ).fetchall()

    if not rows:
        return {
            "category": category.upper(), "period": period.upper().strip(),
            "total_funds": 0, "computed": 0, "excluded_stale": 0,
            "avg_return_pct": None, "avg_cagr_pct": None, "results": [],
            "error": f"No funds found for category '{category}'. "
                     "Use list_categories() to see valid names.",
        }

    results = _compute_returns([r[0] for r in rows], period)

    # staleness: lag each fund's end NAV against the category's freshest as-of
    end_dates = [r["end_nav_date"] for r in results if r["end_nav_date"]]
    as_of = max(end_dates) if end_dates else None
    for r in results:
        if as_of and r["end_nav_date"]:
            lag = (as_of - r["end_nav_date"]).days
            r["as_of_lag_days"] = lag
            r["stale"] = lag > staleness_days
        else:
            r["as_of_lag_days"] = None
            r["stale"] = r["return_pct"] is None

    valid_cols = {"return_pct", "cagr_pct", "scheme_name", "fund_house", "start_nav", "end_nav"}
    key = sort_by if sort_by in valid_cols else "return_pct"
    none_rows = [r for r in results if r.get(key) is None]
    good_rows = [r for r in results if r.get(key) is not None]
    good_rows.sort(key=lambda r: r[key], reverse=not ascending)
    ordered = good_rows + none_rows

    fresh = [r for r in ordered if not r["stale"] and r["return_pct"] is not None]
    avg_ret = round(sum(r["return_pct"] for r in fresh) / len(fresh), 4) if fresh else None
    cagrs = [r["cagr_pct"] for r in fresh if r["cagr_pct"] is not None]
    avg_cagr = round(sum(cagrs) / len(cagrs), 4) if cagrs else None

    return {
        "category": category.upper(),
        "period": period.upper().strip(),
        "as_of": str(as_of) if as_of else None,
        "staleness_days": staleness_days,
        "total_funds": len(ordered),
        "computed": len([r for r in ordered if r["return_pct"] is not None]),
        "excluded_stale": len([r for r in ordered if r["stale"]]),
        "avg_return_pct": avg_ret,
        "avg_cagr_pct": avg_cagr,
        "results": ordered,
    }


@mcp.tool()
def list_indices() -> dict:
    """List the benchmark indices available, with their tickers and history span.

    Call this first when the user names an index; the returned ticker feeds into
    get_index_returns / get_index_returns_between. Friendly aliases (NIFTY50,
    NIFTY100, NIFTY500, SENSEX, MIDCAP50) are also accepted by those tools.
    """
    if not _index_available():
        return {"indices": [], "count": 0,
                "error": "Index data is not loaded on this server. Run "
                         "`python Index/fetch_index_data.py` to generate it."}
    rows = _db().execute("""
        SELECT m.ticker, m.index_name,
               MIN(h.nav_date), MAX(h.nav_date), COUNT(*)
        FROM index_master m
        LEFT JOIN index_history h ON h.ticker = m.ticker
        GROUP BY m.ticker, m.index_name
        ORDER BY m.index_name
    """).fetchall()
    return {
        "count": len(rows),
        "return_type": "price",
        "note": "Index levels are PRICE return (dividends excluded); fund NAVs "
                "are TOTAL return. Index figures therefore understate a "
                "like-for-like comparison by roughly 1-1.5%/yr for equity.",
        "indices": [
            {"ticker": r[0], "index_name": r[1],
             "history_from": str(r[2]) if r[2] else None,
             "history_to": str(r[3]) if r[3] else None,
             "trading_days": r[4]}
            for r in rows
        ],
    }


@mcp.tool()
def get_index_returns(tickers: Union[str, list[str]], period: str) -> dict:
    """Point-to-point return and CAGR for one or many benchmark indices.

    The index counterpart of get_fund_returns: same period strings, same window
    conventions (each index's window ends at its own latest close), and the same
    maths, so index and fund figures over the same period are directly
    comparable — with one caveat, below.

    Accepts tickers (^NSEI) or friendly aliases (NIFTY50, NIFTY500, SENSEX).
    Use list_indices() to see what is available.

    IMPORTANT: index levels are PRICE return — they exclude dividends — whereas
    fund NAVs are TOTAL return. A fund will therefore look better against its
    benchmark than it truly is, by roughly 1-1.5%/yr for Indian equity. Say so
    when presenting a fund-vs-index comparison.

    Args:
        tickers: A single ticker/alias or a list of them.
        period: One of 1W,2W,1M,3M,6M,9M,1Y,2Y,3Y,5Y,YTD,MTD,SI.
    """
    if not _index_available():
        raise ValueError(
            "Index data is not loaded on this server. Run "
            "`python Index/fetch_index_data.py` to generate ./data/index_*.parquet.")
    if isinstance(tickers, str):
        tickers = [tickers]
    return {
        "period": period.upper().strip(),
        "period_end": "per-index-last-close",
        "return_type": "price",
        "results": _compute_index_returns(tickers, period),
    }


@mcp.tool()
def get_index_returns_between(
    tickers: Union[str, list[str]],
    start_date: str,
    end_date: str,
) -> dict:
    """Index return and CAGR over an EXPLICIT date range.

    The index counterpart of get_fund_returns_between — use it to compare a fund
    and its benchmark over exactly the same window. Both dates are ISO
    YYYY-MM-DD and apply to every index in the list; each end snaps to the latest
    close on/before the requested date, so read start_date/end_date in the
    results for the window actually used.

    Index levels are PRICE return (dividends excluded) while fund NAVs are TOTAL
    return — see get_index_returns for what that means for comparisons.

    Args:
        tickers: A single ticker/alias or a list of them.
        start_date: Window start, ISO YYYY-MM-DD.
        end_date: Window end, ISO YYYY-MM-DD.
    """
    if not _index_available():
        raise ValueError(
            "Index data is not loaded on this server. Run "
            "`python Index/fetch_index_data.py` to generate ./data/index_*.parquet.")
    try:
        d_start = date.fromisoformat(start_date.strip())
        d_end = date.fromisoformat(end_date.strip())
    except ValueError as exc:
        raise ValueError(
            f"start_date and end_date must be ISO YYYY-MM-DD dates "
            f"(got '{start_date}' and '{end_date}'): {exc}"
        ) from None

    if d_start >= d_end:
        raise ValueError(
            f"start_date ({d_start}) must be earlier than end_date ({d_end})."
        )

    if isinstance(tickers, str):
        tickers = [tickers]
    return {
        "period": "CUSTOM",
        "start_date": str(d_start),
        "end_date": str(d_end),
        "period_end": "explicit-end-date",
        "return_type": "price",
        "results": _compute_index_returns(tickers, "CUSTOM",
                                          window=(d_start, d_end)),
    }


@mcp.tool()
def get_fund_holdings_profile(
    fund_name: str,
    include_sectors: bool = True,
    include_top_holdings: bool = True,
) -> dict:
    """Market-cap split, sector exposure and top holdings for a fund.

    Answers "how much of this fund is large cap?", "what is its exposure to
    Banking?", "what does it actually hold?" — the portfolio composition
    questions the returns tools cannot answer.

    Pass the fund's scheme_name as returned by search_funds; the holdings data
    is keyed by exactly that name, so it joins directly with the returns tools.
    A case-insensitive or unique partial name also works.

    Weights are percentages of the WHOLE portfolio and sum to the fund's equity
    weight, NOT to 100 — the remainder is cash, repo and derivatives, which are
    not equity holdings. `unmatched` lists holdings that could not be classified
    against AMFI reference data, so "no exposure to X" can be told apart from
    "held something unclassifiable".

    Args:
        fund_name: Fund scheme_name (from search_funds), or a unique partial.
        include_sectors: Include the sector/industry breakdown (default True).
        include_top_holdings: Include the largest holdings (default True).
    """
    if not _holdings_available():
        raise ValueError(
            "Fund holdings data is not loaded on this server. Generate it with "
            "holdings_enricher/main.py --json and place it at "
            f"{FUND_HOLDINGS_PATH}.")

    key, prof = _find_fund_holdings(fund_name)
    if not prof:
        return {
            "fund_name": fund_name, "found": False,
            "error": f"No holdings data for '{fund_name}'. Holdings are "
                     "currently available for Large/Mid/Small Cap equity funds "
                     "only; use list_funds_with_holdings() to see which.",
        }

    out = {
        "fund_name": key,
        "found": True,
        "as_of": prof.get("as_of"),
        "cap_category": prof.get("cap_category"),
        "equity_pct": prof.get("equity_pct"),
        "holding_count": prof.get("holding_count"),
        "market_cap": prof.get("market_cap"),
        "note": "Weights are % of the whole portfolio and sum to equity_pct, "
                "not to 100. The remainder is cash/repo/derivatives.",
    }
    if include_sectors:
        # Drop the per-sector holdings arrays — sector weight and the industry
        # split are what a caller almost always wants, and the full arrays
        # duplicate top_holdings at several times the payload size.
        out["sectors"] = {
            name: {"weight_pct": s["weight_pct"], "industries": s["industries"]}
            for name, s in (prof.get("sectors") or {}).items()
        }
    if include_top_holdings:
        out["top_holdings"] = prof.get("top_holdings")
    if prof.get("unmatched"):
        out["unmatched"] = prof["unmatched"]
        out["unmatched_pct"] = round(
            sum(u.get("weight_pct") or 0 for u in prof["unmatched"]), 4)
    return out


def _full_stock_holdings(prof: dict) -> dict:
    """
    Full per-stock holdings for a fund, keyed by ISIN.

    `top_holdings` on the profile is truncated (top 10); the complete list is
    the union of `sectors[*].holdings`, which together account for exactly
    `holding_count` entries. Entries without an ISIN (see `unmatched`) can't be
    compared across funds, so they are excluded here — callers get `unmatched_pct`
    from get_fund_holdings_profile if they need to know how much was dropped.

    Returns {isin: {"stock", "amfi_name", "weight_pct", "industry", "market_cap_cat"}}.
    A stock appearing in more than one sector bucket (shouldn't happen, but the
    data is enricher output, not a guarantee) keeps the higher weight.
    """
    by_isin: dict = {}
    for sector in (prof.get("sectors") or {}).values():
        for h in sector.get("holdings") or []:
            isin = h.get("isin")
            if not isin:
                continue
            existing = by_isin.get(isin)
            if existing is None or (h.get("weight_pct") or 0) > existing["weight_pct"]:
                by_isin[isin] = {
                    "stock": h.get("stock"),
                    "amfi_name": h.get("amfi_name"),
                    "weight_pct": h.get("weight_pct") or 0,
                    "industry": h.get("industry"),
                    "market_cap_cat": h.get("market_cap_cat"),
                }
    return by_isin


@mcp.tool()
def get_portfolio_overlap(fund_names: Union[str, list[str]]) -> dict:
    """Compute stock-level portfolio overlap across two or more funds.

    Answers "how much do these funds duplicate each other's holdings?" — useful
    before adding a fund to a portfolio that already holds similar ones.

    Overlap between any two funds A and B is the sum, over every stock held by
    both, of min(weight_in_A, weight_in_B) — the standard "overlap %" definition
    used by portfolio-overlap tools, capped at 100. Weights are % of the whole
    portfolio (same convention as get_fund_holdings_profile), so overlap is
    naturally reduced by cash/derivatives exposure and any unmatched holdings.

    Pass 2+ fund names (scheme_name from search_funds, or unique partials).
    Returns:
        - pairwise: overlap %% and shared holding count for every fund pair
        - common_holdings: stocks held by ALL requested funds, with each fund's
          weight and the min-weight contribution
        - not_found: any input names that couldn't be resolved to holdings data
    Funds without holdings data (see list_funds_with_holdings) are skipped and
    reported in not_found rather than raising, so a mixed valid/invalid list
    still returns overlap for the funds that resolved.

    Args:
        fund_names: 2+ fund names/partials, e.g.
            ["HDFC Flexicap", "Parag Parikh Flexi Cap", "Quant Flexi Cap"].
    """
    if isinstance(fund_names, str):
        fund_names = [fund_names]
    fund_names = [f for f in fund_names if f and f.strip()]
    if len(fund_names) < 2:
        raise ValueError("get_portfolio_overlap needs at least 2 fund names.")

    if not _holdings_available():
        raise ValueError(
            "Fund holdings data is not loaded on this server. Generate it with "
            "holdings_enricher/main.py --json and place it at "
            f"{FUND_HOLDINGS_PATH}.")

    resolved: dict[str, dict] = {}   # key -> {isin -> holding}
    not_found = []
    seen_keys = set()
    for name in fund_names:
        key, prof = _find_fund_holdings(name)
        if not prof:
            not_found.append(name)
            continue
        if key in seen_keys:
            continue  # same fund requested twice under different aliases
        seen_keys.add(key)
        resolved[key] = _full_stock_holdings(prof)

    fund_keys = list(resolved.keys())
    if len(fund_keys) < 2:
        return {
            "funds_requested": fund_names,
            "funds_compared": fund_keys,
            "not_found": not_found,
            "error": "Fewer than 2 requested funds have holdings data; "
                     "overlap requires at least 2.",
            "pairwise": [],
            "common_holdings": [],
        }

    pairwise = []
    for i in range(len(fund_keys)):
        for j in range(i + 1, len(fund_keys)):
            a_key, b_key = fund_keys[i], fund_keys[j]
            a_hold, b_hold = resolved[a_key], resolved[b_key]
            shared_isins = set(a_hold) & set(b_hold)
            overlap_pct = sum(
                min(a_hold[isin]["weight_pct"], b_hold[isin]["weight_pct"])
                for isin in shared_isins
            )
            pairwise.append({
                "fund_a": a_key,
                "fund_b": b_key,
                "overlap_pct": round(min(overlap_pct, 100.0), 2),
                "shared_holding_count": len(shared_isins),
                "fund_a_holding_count": len(a_hold),
                "fund_b_holding_count": len(b_hold),
            })
    pairwise.sort(key=lambda p: p["overlap_pct"], reverse=True)

    common_isins = set.intersection(*(set(h) for h in resolved.values()))
    common_holdings = []
    for isin in common_isins:
        per_fund = {
            key: resolved[key][isin]["weight_pct"] for key in fund_keys
        }
        sample = resolved[fund_keys[0]][isin]
        common_holdings.append({
            "isin": isin,
            "stock": sample.get("stock"),
            "amfi_name": sample.get("amfi_name"),
            "industry": sample.get("industry"),
            "market_cap_cat": sample.get("market_cap_cat"),
            "weight_pct_by_fund": {k: round(v, 4) for k, v in per_fund.items()},
            "min_weight_pct": round(min(per_fund.values()), 4),
        })
    common_holdings.sort(key=lambda c: c["min_weight_pct"], reverse=True)

    return {
        "funds_requested": fund_names,
        "funds_compared": fund_keys,
        "not_found": not_found,
        "pairwise": pairwise,
        "common_to_all_count": len(common_holdings),
        "common_holdings": common_holdings,
        "note": "overlap_pct = sum of min(weight_A, weight_B) over shared "
                "holdings, as %% of whole portfolio. common_holdings lists "
                "only stocks held by every requested fund; see pairwise for "
                "2-fund overlaps within a larger list.",
    }


@mcp.tool()
def list_funds_with_holdings() -> dict:
    """List the funds that have holdings/market-cap data available.

    Holdings cover a subset of the funds in the NAV data — currently Large, Mid
    and Small Cap equity funds. Names match the NAV scheme_name exactly, so they
    can be passed straight to the returns tools.
    """
    if not _holdings_available():
        return {"count": 0, "funds": [],
                "error": "Fund holdings data is not loaded on this server."}
    funds = HOLDINGS.get("funds") or {}
    by_cap: dict = {}
    for name, p in funds.items():
        by_cap.setdefault(p.get("cap_category") or "Unknown", []).append(name)
    return {
        "count": len(funds),
        "as_of": next(iter(funds.values())).get("as_of") if funds else None,
        "generated_at": HOLDINGS.get("generated_at"),
        "by_cap_category": {k: sorted(v) for k, v in sorted(by_cap.items())},
    }


def _nav_series(
    scheme_code: str,
    start_date: date,
    end_date: date,
    frequency: str = "monthly",
    con=None,
) -> list[dict]:
    """
    NAV series for one fund between two dates. Internal helper: the Sharpe
    tools call this directly rather than round-tripping through the MCP tool,
    so a 36-point series costs one query instead of 36.

    frequency:
        "daily"   -> every NAV in the window.
        "monthly" -> one point per calendar month, the latest NAV ON OR BEFORE
                     that month's end. Month-ends land on weekends and holidays,
                     so snapping backwards to the last traded day is what AMCs
                     do; it also matches the "before" snap _resolve_window
                     already uses, keeping this consistent with get_fund_returns.

    Returns oldest-first [{nav_date, nav}, ...]. An empty list means no NAV in
    the window, which the caller must distinguish from a bad scheme_code.
    """
    con = con or _db()
    freq = (frequency or "monthly").lower().strip()

    if freq == "daily":
        rows = con.execute(
            """SELECT nav_date, nav FROM nav_history
               WHERE scheme_code = ? AND nav_date BETWEEN ? AND ?
               ORDER BY nav_date""",
            [scheme_code, start_date, end_date],
        ).fetchall()
    elif freq == "monthly":
        # One row per (year, month): the last NAV in that month. Because the
        # window is bounded by end_date, the final month yields the latest NAV
        # on or before it rather than a future one.
        rows = con.execute(
            """SELECT nav_date, nav FROM nav_history
               WHERE scheme_code = ? AND nav_date BETWEEN ? AND ?
               QUALIFY ROW_NUMBER() OVER (
                   PARTITION BY date_trunc('month', nav_date)
                   ORDER BY nav_date DESC) = 1
               ORDER BY nav_date""",
            [scheme_code, start_date, end_date],
        ).fetchall()
    else:
        raise ValueError(
            f"frequency must be 'daily' or 'monthly', got {frequency!r}.")

    return [{"nav_date": r[0], "nav": r[1]} for r in rows]


@mcp.tool()
def get_fund_nav_history(
    scheme_code: str,
    start_date: str,
    end_date: str,
    frequency: str = "monthly",
) -> dict:
    """A fund's NAV series between two dates, as a list of points.

    The returns tools answer "what did this fund do between A and B" with a
    single number. This returns the whole path instead, which is what any
    series-based measure needs: volatility, rolling returns, drawdowns.

    Monthly frequency gives one point per calendar month — the last NAV on or
    before each month-end, since month-ends fall on weekends and holidays. That
    is the convention AMCs use for the "36 monthly data points" behind Std Dev
    and Sharpe, and it matches how get_fund_returns snaps its own windows.

    Points are oldest-first. The series is bounded by the fund's own data, so
    a window starting before inception simply begins later — check
    `requested_start` against `start_nav_date` rather than assuming coverage.

    Args:
        scheme_code: Fund scheme code (from search_funds).
        start_date: ISO date, YYYY-MM-DD.
        end_date: ISO date, YYYY-MM-DD.
        frequency: "monthly" (default) or "daily".
    """
    try:
        s_date = date.fromisoformat(start_date)
        e_date = date.fromisoformat(end_date)
    except ValueError as exc:
        return {"scheme_code": scheme_code, "error": f"Bad date: {exc}"}

    if s_date > e_date:
        return {"scheme_code": scheme_code,
                "error": f"start_date {s_date} is after end_date {e_date}."}

    con = _db()
    meta = con.execute(
        """SELECT scheme_name, fund_house, category
           FROM scheme_master WHERE scheme_code = ?""",
        [scheme_code],
    ).fetchone()
    if not meta:
        return {"scheme_code": scheme_code,
                "error": f"Unknown scheme_code '{scheme_code}'. "
                         "Use search_funds to resolve a fund name."}

    try:
        series = _nav_series(scheme_code, s_date, e_date, frequency, con=con)
    except ValueError as exc:
        return {"scheme_code": scheme_code, "error": str(exc)}

    return {
        "scheme_code": scheme_code,
        "scheme_name": meta[0],
        "fund_house": meta[1],
        "category": meta[2],
        "frequency": (frequency or "monthly").lower().strip(),
        "requested_start": str(s_date),
        "requested_end": str(e_date),
        "start_nav_date": str(series[0]["nav_date"]) if series else None,
        "end_nav_date": str(series[-1]["nav_date"]) if series else None,
        "count": len(series),
        "series": [{"nav_date": str(p["nav_date"]), "nav": p["nav"]}
                   for p in series],
    }


def _load_risk_free_rates() -> dict:
    """
    {month_end_iso: rate_pct} from data/risk_free_rates.json.

    Kept as data rather than a constant because the rate is itself as-of a date:
    AMCs use the 1-day MIBOR as of the factsheet's month-end, so a Sharpe only
    reproduces when the rate and the NAV window describe the SAME month. A
    hardcoded default would silently go stale and pair, say, a December window
    with a July rate.
    """
    if not os.path.exists(RISK_FREE_RATES_PATH):
        sys.stderr.write(
            f"NAV MCP: risk-free rates not found at {RISK_FREE_RATES_PATH}; "
            "get_sharpe_ratio will require risk_free_rate_pct to be passed.\n")
        return {}
    try:
        import json as _json
        with open(RISK_FREE_RATES_PATH, encoding="utf-8") as fh:
            return (_json.load(fh) or {}).get("rates") or {}
    except Exception as exc:
        sys.stderr.write(f"NAV MCP: could not read risk-free rates: {exc}\n")
        return {}


RISK_FREE_RATES = _load_risk_free_rates()


def _month_end(d: date) -> date:
    """Last calendar day of d's month."""
    import calendar
    return date(d.year, d.month, calendar.monthrange(d.year, d.month)[1])


# window label -> n_months, for fund_risk_data / index_risk_data's `window`
# shortcut. "3Y"/"5Y" are the two AMC-standard trailing windows; n_months
# stays available underneath for a non-standard length either tool still
# accepts directly.
_RISK_WINDOWS = {"3Y": 36, "5Y": 60}


def _resolve_risk_windows(window: Optional[str], n_months: int) -> list[tuple[str, int]]:
    """
    Return [(label, n_months), ...] to compute, from the `window` shortcut.

    window=None uses n_months as-is as before (label "custom" unless it
    happens to match a known window, kept internal — n_months alone is still
    the escape hatch for a length neither "3Y" nor "5Y" names). "3Y"/"5Y" map
    to 36/60 regardless of n_months. "both" computes both windows in one call
    (nested per-fund by label) rather than requiring two round trips.
    """
    if not window:
        label = next((lbl for lbl, m in _RISK_WINDOWS.items() if m == n_months), "custom")
        return [(label, n_months)]
    w = window.strip().upper()
    if w == "BOTH":
        return [("3Y", 36), ("5Y", 60)]
    if w in _RISK_WINDOWS:
        return [(w, _RISK_WINDOWS[w])]
    raise ValueError(
        f"window must be '3Y', '5Y', 'both', or omitted, got {window!r}.")


def _annualized_std_dev_pct(monthly_returns_pct: list[float]) -> float:
    """Sample std dev (ddof=1) of monthly returns, scaled by sqrt(12)."""
    if len(monthly_returns_pct) < 2:
        raise ValueError("Need at least 2 monthly returns to compute std dev.")
    return statistics.stdev(monthly_returns_pct) * (12 ** 0.5)


def _cagr_from_monthly_returns(monthly_returns_pct: list[float]) -> float:
    """Compound monthly returns, then annualize over the series length."""
    growth = 1.0
    for r in monthly_returns_pct:
        growth *= (1 + r / 100.0)
    return (growth ** (1 / (len(monthly_returns_pct) / 12.0)) - 1) * 100.0


def _beta(fund_returns_pct: list[float], benchmark_returns_pct: list[float]) -> float:
    """
    Beta = Cov(fund, benchmark) / Var(benchmark), on month-for-month aligned
    monthly returns — the same series length/window fund_risk_data already
    uses for std_dev and Sharpe, so all three numbers describe one window.

    Sample covariance/variance (ddof=1); scale is irrelevant to the ratio, so
    computing on percentage points rather than decimals changes nothing.
    """
    if len(fund_returns_pct) != len(benchmark_returns_pct):
        raise ValueError(
            f"fund and benchmark return series must be the same length, got "
            f"{len(fund_returns_pct)} vs {len(benchmark_returns_pct)}.")
    if len(fund_returns_pct) < 2:
        raise ValueError("Need at least 2 monthly returns to compute Beta.")

    var_b = statistics.variance(benchmark_returns_pct)
    if var_b == 0:
        raise ValueError("Benchmark has zero variance over the window; Beta undefined.")
    cov = statistics.covariance(fund_returns_pct, benchmark_returns_pct)
    return cov / var_b


def _max_drawdown_pct(navs: list[float]) -> float:
    """
    Max peak-to-trough decline over the series, as a positive percentage.

    Walks the same monthly NAV points std_dev/Sharpe/beta already use (not a
    daily series), so it describes the same window as the rest of the row.
    Running peak rather than a full O(n^2) scan: at each point the only
    trough that can beat the current worst is the deepest drop from the
    highest NAV seen so far.
    """
    if len(navs) < 2:
        raise ValueError("Need at least 2 NAV points to compute Max Drawdown.")
    peak = navs[0]
    worst = 0.0
    for nav in navs[1:]:
        peak = max(peak, nav)
        drawdown = (peak - nav) / peak * 100.0
        worst = max(worst, drawdown)
    return worst


def _calmar_ratio(cagr_pct: float, max_drawdown_pct: float) -> float:
    """Calmar = CAGR / Max Drawdown, both in percentage points.

    Uses the same CAGR already computed for Sharpe (one source of truth per
    window) rather than recomputing it. Undefined at zero drawdown, same as
    Sharpe is undefined at zero volatility.
    """
    if max_drawdown_pct <= 0:
        raise ValueError("Zero drawdown over the window; Calmar Ratio undefined.")
    return cagr_pct / max_drawdown_pct


@mcp.tool()
def fund_risk_data(
    scheme_codes: Union[str, list[str]],
    as_of: Optional[str] = None,
    risk_free_rate_pct: Optional[float] = None,
    n_months: int = 36,
    benchmark_ticker: Optional[str] = None,
    window: Optional[str] = None,
) -> dict:
    """Risk metrics for one or many funds, as published on factsheets.

    Returns, per fund (per window — see `window` below):
      - std_dev_pct      annualized standard deviation (volatility)
      - sharpe_ratio     (CAGR - risk-free rate) / std_dev
      - beta             Cov(fund, benchmark) / Var(benchmark) — only when
                         benchmark_ticker (or a category default) applies
      - max_drawdown_pct largest peak-to-trough decline over the window's
                         monthly NAVs, as a positive percentage
      - calmar_ratio     CAGR / max_drawdown_pct — return earned per unit of
                         worst-case decline, using the same CAGR as Sharpe.
                         Omitted (with calmar_error) at zero drawdown, same
                         as Sharpe is omitted at zero volatility.

    WINDOW: pass window="3Y" (36 months, the default), "5Y" (60 months), or
    "both" to get one result per fund at EACH window in a single call —
    factsheets publish both, and this avoids two round trips. "both" nests
    each fund's results under "3Y"/"5Y" keys instead of a flat dict (see
    return shape below); a single window keeps today's flat shape. window
    overrides n_months when given; pass n_months alone for a non-standard
    trailing length (e.g. 12 months) — that always returns the flat shape.

    The CAGR feeding the Sharpe is computed internally but not returned — use
    get_fund_returns for returns, so there is one source of truth for them.

    Everything is derived here from a monthly NAV series — the AMC methodology
    of "last 36 monthly data points" — rather than taken on trust. Verified
    against a published factsheet: Bandhan Large Cap (108799) at as_of
    2026-07-31 gives Std Dev 14.895 and Sharpe 0.476, matching the published
    14.90 and 0.476. Both are rounded to 3dp.

    THE AS-OF DATE MATTERS. The risk-free rate is itself dated (AMCs use the
    1-day MIBOR at the factsheet's month-end), so the rate and the NAV window
    must describe the same month or the result silently mixes two moments.

    EXPECT A LAG, AND DO NOT CALL IT STALE. The rate is read off factsheets,
    which publish mid-month for the month just ended, so the newest rate on file
    normally trails the newest NAV by about a month. The default `as_of` is
    therefore the latest month-end WE HOLD A RATE FOR, not the latest NAV month
    - in early September that means an as_of of 31 July, which is correct and is
    exactly the factsheet being reproduced. Pass `as_of` to pin a specific one.

    BETA'S BENCHMARK IS PER-FUND, NOT PER-CALL. Passing benchmark_ticker pins
    every fund in the call to that one index — use this for an explicit
    comparison. Leave it unset and each fund gets its OWN benchmark from its
    scheme_master category (Large Cap -> NIFTY_100, Mid Cap -> NIFTY_MIDCAP_150,
    Small Cap -> NIFTY_SMALLCAP_250), so mixing a large-cap and a mid-cap fund
    in one call does not silently benchmark both against the same index. Only
    those three categories have a default; anything else (Large & Mid Cap,
    Multi Cap, Flexi Cap, hybrids, sectoral, ...) gets no beta unless you pass
    benchmark_ticker explicitly. `category` and, when beta is present,
    `benchmark_source` ("category_default" or "explicit") are in each result
    so you can see what was actually used.

    Only NIFTY-family tickers exist in this server's index data — there is no
    BSE series, so a fund whose factsheet benchmarks against a BSE index (e.g.
    BSE 100 TRI) gets a NIFTY proxy here, and the beta will not exactly
    reproduce the factsheet's. Beta is computed on the SAME monthly window
    used for std_dev/Sharpe (fund and benchmark are matched by calendar month,
    so a month either side is missing simply drops from both series) and
    omitted per-fund (with a beta_error) if that fund's benchmark lacks history
    over the window.

    Interpretation: lower std_dev means a smoother ride; higher sharpe means
    more return per unit of volatility. beta < 1 means historically less
    volatile than the benchmark, beta > 1 more. A fund with the better CAGR
    can still score worse on Sharpe if it got there with bigger swings. Only
    compare across funds computed at the same as_of, and for beta, only across
    funds sharing the same benchmark_ticker in their result.

    Args:
        scheme_codes: A scheme_code or list of them (from search_funds).
        as_of: Month-end to anchor on, ISO YYYY-MM-DD. Defaults to the newest
            month-end present in risk_free_rates.json. Snapped to month-end.
        risk_free_rate_pct: Percentage, e.g. 5.41. Defaults to the rate stored
            for as_of in data/risk_free_rates.json; required if that month has
            no entry.
        n_months: Number of monthly RETURNS in the window (default 36, the AMC
            standard). Needs n_months+1 NAV points, so a fund with a shorter
            history returns an error rather than a non-comparable figure.
        benchmark_ticker: Optional index ticker (from list_indices) to compute
            Beta against for EVERY fund in the call, e.g. "NIFTY_100". Omit to
            auto-select per fund from its category (Large/Mid/Small Cap only;
            other categories get no beta unless this is passed explicitly).
        window: "3Y", "5Y", or "both". Omit to use n_months directly (default
            36, i.e. 3Y).
    """
    codes = [scheme_codes] if isinstance(scheme_codes, str) else list(scheme_codes)
    if not codes:
        return {"error": "Pass at least one scheme_code."}
    try:
        windows = _resolve_risk_windows(window, n_months)
    except ValueError as exc:
        return {"error": str(exc)}
    if any(m < 12 for _, m in windows):
        return {"error": f"n_months must be at least 12 to annualize "
                         f"meaningfully, got {n_months}."}

    con = _db()

    # Anchor: an explicit as_of wins, else the newest month-end WE HAVE A RATE
    # FOR. Deliberately not the newest NAV month: the rate comes from factsheets,
    # which publish mid-month for the month just ended, so the rates file trails
    # the NAV data by roughly a month every month. Anchoring on NAV would send
    # the default path looking for a rate that does not exist yet and error for
    # ~2 weeks out of every 4. The rate is the scarcer input, so it sets the
    # window; adding the next month's line to risk_free_rates.json moves the
    # anchor forward on its own, no code change.
    if as_of:
        try:
            anchor = _month_end(date.fromisoformat(as_of))
        except ValueError as exc:
            return {"error": f"Bad as_of date: {exc}"}
    else:
        if not RISK_FREE_RATES:
            return {"error": f"No risk-free rates on file at "
                             f"{RISK_FREE_RATES_PATH}; pass as_of and "
                             f"risk_free_rate_pct explicitly."}
        anchor = date.fromisoformat(max(RISK_FREE_RATES))

    rf = risk_free_rate_pct
    rf_source = "caller"
    if rf is None:
        rf = RISK_FREE_RATES.get(str(anchor))
        rf_source = "risk_free_rates.json"
    if rf is None:
        return {
            "error": f"No risk-free rate on file for {anchor}. Add it to "
                     f"{RISK_FREE_RATES_PATH} or pass risk_free_rate_pct.",
            "as_of": str(anchor),
            "latest_rate_on_file": max(RISK_FREE_RATES) if RISK_FREE_RATES else None,
            "rates_available": sorted(RISK_FREE_RATES, reverse=True)[:6],
        }

    def _compute_for(m: int) -> list[dict]:
        """Everything below is the original single-window computation,
        unchanged, just parameterized by m (n_months) so it can run once per
        requested window instead of assuming n_months from the outer scope."""
        # Window start: far enough back to cover m+1 month-ends.
        start = _month_end(anchor - relativedelta(months=m + 1))

        # Per-ticker benchmark series cache: with auto-selection, funds in the
        # same call can land on different benchmarks (Large Cap -> NIFTY_100,
        # Mid Cap -> NIFTY_MIDCAP_150, ...), but funds sharing a category share
        # a ticker, so fetch each ticker's series at most once rather than once
        # per fund. Value is (bench_by_date, error) — bench_by_date keyed by
        # (year, month), not exact nav_date: funds and indices can strike their
        # last value of the month on different trading days (different
        # holiday/closure calendars), even though both sides already
        # independently snap to "last point on/before month-end". Matching on
        # calendar month is what actually pairs the same month's fund NAV with
        # the same month's benchmark close.
        bench_cache: dict[str, tuple[dict, Optional[str]]] = {}

        def _get_benchmark(ticker: str) -> tuple[dict, Optional[str]]:
            resolved = _resolve_ticker(ticker)
            if resolved in bench_cache:
                return bench_cache[resolved]
            if not _index_available():
                result = ({}, "No index data loaded on this server.")
            else:
                bench_series = _index_series(resolved, start, anchor, "monthly", con=con)
                if len(bench_series) < m + 1:
                    result = ({}, f"Benchmark '{resolved}' has {len(bench_series)} monthly "
                                  f"points over the window, needs {m + 1}. Use "
                                  f"list_indices() to check its coverage.")
                else:
                    by_date = {
                        (p["nav_date"].year, p["nav_date"].month): p["close"]
                        for p in bench_series
                    }
                    result = (by_date, None)
            bench_cache[resolved] = result
            return result

        results = []
        for code in codes:
            meta = con.execute(
                "SELECT scheme_name, category FROM scheme_master WHERE scheme_code = ?",
                [code]).fetchone()
            if not meta:
                results.append({"scheme_code": code, "error": "Unknown scheme_code."})
                continue
            scheme_name, category = meta

            # Explicit benchmark_ticker always wins (and applies to every fund
            # in the call, as before). Otherwise auto-select from the fund's
            # own category, so a mixed Large Cap + Mid Cap batch gets each fund
            # its own correct benchmark instead of silently sharing one.
            fund_benchmark = benchmark_ticker
            auto_selected = False
            if not fund_benchmark and category:
                fund_benchmark = _CATEGORY_BENCHMARKS.get(category.strip().upper())
                auto_selected = fund_benchmark is not None

            series = _nav_series(code, start, anchor, "monthly", con=con)
            if len(series) < m + 1:
                results.append({
                    "scheme_code": code, "scheme_name": scheme_name,
                    "error": f"Needs {m + 1} monthly NAV points, has "
                             f"{len(series)}. Fund history is too short for a "
                             f"{m}-month Sharpe.",
                })
                continue

            pts = series[-(m + 1):]          # exactly m returns
            navs = [p["nav"] for p in pts]
            rets = [(navs[i + 1] / navs[i] - 1) * 100.0 for i in range(len(navs) - 1)]

            try:
                sd = _annualized_std_dev_pct(rets)
                cagr = _cagr_from_monthly_returns(rets)
            except (ValueError, ZeroDivisionError) as exc:
                results.append({"scheme_code": code, "scheme_name": scheme_name,
                                "error": str(exc)})
                continue

            if sd <= 0:
                results.append({
                    "scheme_code": code, "scheme_name": scheme_name,
                    "error": "Zero volatility over the window; Sharpe undefined.",
                })
                continue

            row = {
                "scheme_code": code,
                "scheme_name": scheme_name,
                "category": category,
                "std_dev_pct": round(sd, 3),
                "sharpe_ratio": round((cagr - rf) / sd, 3),
                "window_start": str(pts[0]["nav_date"]),
                "window_end": str(pts[-1]["nav_date"]),
                "months_used": len(rets),
            }

            # Max Drawdown / Calmar: same monthly navs as everything else in
            # this row, so it describes the same window. Soft-fail like beta
            # (mdd_error) rather than dropping the fund entirely — std_dev and
            # Sharpe above are still valid even if drawdown is degenerate.
            try:
                mdd = _max_drawdown_pct(navs)
                row["max_drawdown_pct"] = round(mdd, 3)
                row["calmar_ratio"] = round(_calmar_ratio(cagr, mdd), 3)
            except ValueError as exc:
                row["calmar_error"] = str(exc)

            if fund_benchmark:
                bench_by_date, bench_error = _get_benchmark(fund_benchmark)
                if bench_error:
                    row["beta_error"] = bench_error
                else:
                    # Align on nav_date, not position: a fund's month-end dates
                    # and the benchmark's need not fall on exactly the same
                    # calendar day (e.g. index closed on a day the fund's AMC
                    # still struck a NAV, or vice versa), and each side already
                    # snaps independently to "last point on/before month-end".
                    # Keep only the fund month-ends that have a matching
                    # benchmark close, in order, then difference THAT reduced
                    # series — so both a dropped month-end and the interval on
                    # either side of it are excluded from both series together,
                    # keeping fund/benchmark returns pairwise aligned to the
                    # same NAV interval.
                    aligned = [
                        (p["nav_date"], p["nav"], bench_by_date[(p["nav_date"].year, p["nav_date"].month)])
                        for p in pts
                        if (p["nav_date"].year, p["nav_date"].month) in bench_by_date
                    ]
                    if len(aligned) < m + 1:
                        row["beta_error"] = (
                            f"Only {len(aligned)} of {m + 1} fund month-ends "
                            f"have a matching benchmark close; too many gaps to align.")
                    else:
                        fund_rets = [
                            (aligned[i + 1][1] / aligned[i][1] - 1) * 100.0
                            for i in range(len(aligned) - 1)
                        ]
                        bench_rets = [
                            (aligned[i + 1][2] / aligned[i][2] - 1) * 100.0
                            for i in range(len(aligned) - 1)
                        ]
                        try:
                            row["beta"] = round(_beta(fund_rets, bench_rets), 3)
                            row["benchmark_ticker"] = _resolve_ticker(fund_benchmark)
                            row["benchmark_source"] = "category_default" if auto_selected else "explicit"
                        except ValueError as exc:
                            row["beta_error"] = str(exc)

            results.append(row)

        return results

    windowed = {label: _compute_for(m) for label, m in windows}

    if len(windows) == 1:
        # Single window: same flat shape as before window/_resolve_risk_windows
        # existed, so a caller passing plain n_months (no window) sees no
        # change at all.
        (label, m), = windows
        return {
            "as_of": str(anchor),
            "risk_free_rate_pct": rf,
            "risk_free_rate_source": rf_source,
            "n_months": m,
            "window": label,
            "count": len(windowed[label]),
            "results": windowed[label],
        }

    return {
        "as_of": str(anchor),
        "risk_free_rate_pct": rf,
        "risk_free_rate_source": rf_source,
        "windows": {label: m for label, m in windows},
        "count": {label: len(rows) for label, rows in windowed.items()},
        "results": windowed,
    }


@mcp.tool()
def index_risk_data(
    tickers: Union[str, list[str]],
    as_of: Optional[str] = None,
    risk_free_rate_pct: Optional[float] = None,
    n_months: int = 36,
    window: Optional[str] = None,
) -> dict:
    """Risk metrics for one or many indices — the benchmark-side counterpart
    of fund_risk_data.

    Returns, per index (per window — see `window` below):
      - std_dev_pct    annualized standard deviation of the index's own
                       monthly returns (volatility)
      - sharpe_ratio   (CAGR - risk-free rate) / std_dev, on the index's own
                       returns — how the benchmark itself would have scored
                       if it were a fund

    Same methodology as fund_risk_data: "last 36 monthly data points" (last
    close on/before each month-end), sample std dev (ddof=1) annualized by
    sqrt(12), CAGR compounded from the same monthly series. Use this to see
    what a fund's std_dev/sharpe_ratio (from fund_risk_data) is actually being
    measured against, or to compare a fund's Beta-input benchmark's own risk
    profile across indices.

    WINDOW: pass window="3Y" (36 months, the default), "5Y" (60 months), or
    "both" to get one result per index at EACH window in a single call. "both"
    nests each index's results under "3Y"/"5Y" keys instead of a flat dict
    (same shape fund_risk_data uses); a single window keeps the flat shape.
    window overrides n_months when given; pass n_months alone for a
    non-standard trailing length.

    THE AS-OF DATE MATTERS, same as fund_risk_data: the risk-free rate is
    dated (AMCs use the 1-day MIBOR at the factsheet's month-end), so the rate
    and the NAV window must describe the same month. The default `as_of` is
    the latest month-end WE HOLD A RATE FOR (see data/risk_free_rates.json),
    not the latest index close — pass `as_of` to pin a specific one.

    Only NIFTY-family tickers exist in this server's index data — there is no
    BSE series (list_indices() shows what is available).

    Args:
        tickers: An index ticker or list of them, e.g. "NIFTY_100" or
            ["NIFTY_100", "NIFTY_MIDCAP_150"]. Aliases resolve the same way as
            get_index_returns (see list_indices).
        as_of: Month-end to anchor on, ISO YYYY-MM-DD. Defaults to the newest
            month-end present in risk_free_rates.json. Snapped to month-end.
        risk_free_rate_pct: Percentage, e.g. 5.41. Defaults to the rate stored
            for as_of in data/risk_free_rates.json; required if that month has
            no entry.
        n_months: Number of monthly RETURNS in the window (default 36, the AMC
            standard). Needs n_months+1 close points, so an index with a
            shorter history returns an error rather than a non-comparable
            figure.
        window: "3Y", "5Y", or "both". Omit to use n_months directly (default
            36, i.e. 3Y).
    """
    raw_tickers = [tickers] if isinstance(tickers, str) else list(tickers)
    if not raw_tickers:
        return {"error": "Pass at least one ticker."}
    try:
        windows = _resolve_risk_windows(window, n_months)
    except ValueError as exc:
        return {"error": str(exc)}
    if any(m < 12 for _, m in windows):
        return {"error": f"n_months must be at least 12 to annualize "
                         f"meaningfully, got {n_months}."}
    if not _index_available():
        return {"error": "No index data loaded on this server."}

    con = _db()

    # Same anchor/rate resolution as fund_risk_data — see the comment there for
    # why the rate, not the newest close, sets the default as_of.
    if as_of:
        try:
            anchor = _month_end(date.fromisoformat(as_of))
        except ValueError as exc:
            return {"error": f"Bad as_of date: {exc}"}
    else:
        if not RISK_FREE_RATES:
            return {"error": f"No risk-free rates on file at "
                             f"{RISK_FREE_RATES_PATH}; pass as_of and "
                             f"risk_free_rate_pct explicitly."}
        anchor = date.fromisoformat(max(RISK_FREE_RATES))

    rf = risk_free_rate_pct
    rf_source = "caller"
    if rf is None:
        rf = RISK_FREE_RATES.get(str(anchor))
        rf_source = "risk_free_rates.json"
    if rf is None:
        return {
            "error": f"No risk-free rate on file for {anchor}. Add it to "
                     f"{RISK_FREE_RATES_PATH} or pass risk_free_rate_pct.",
            "as_of": str(anchor),
            "latest_rate_on_file": max(RISK_FREE_RATES) if RISK_FREE_RATES else None,
            "rates_available": sorted(RISK_FREE_RATES, reverse=True)[:6],
        }

    resolved = list(dict.fromkeys(_resolve_ticker(t) for t in raw_tickers))
    ph = ", ".join(["?"] * len(resolved))
    meta_rows = con.execute(
        f"SELECT ticker, index_name FROM index_master WHERE ticker IN ({ph})",
        resolved,
    ).fetchall()
    meta = {r[0]: r[1] for r in meta_rows}

    def _compute_for(m: int) -> list[dict]:
        start = _month_end(anchor - relativedelta(months=m + 1))
        results = []
        for ticker in resolved:
            if ticker not in meta:
                results.append({
                    "ticker": ticker,
                    "error": f"Unknown ticker '{ticker}'. Use list_indices() to "
                             "see available tickers.",
                })
                continue

            series = _index_series(ticker, start, anchor, "monthly", con=con)
            if len(series) < m + 1:
                results.append({
                    "ticker": ticker, "index_name": meta[ticker],
                    "error": f"Needs {m + 1} monthly close points, has "
                             f"{len(series)}. Index history is too short for a "
                             f"{m}-month Sharpe.",
                })
                continue

            pts = series[-(m + 1):]
            closes = [p["close"] for p in pts]
            rets = [(closes[i + 1] / closes[i] - 1) * 100.0 for i in range(len(closes) - 1)]

            try:
                sd = _annualized_std_dev_pct(rets)
                cagr = _cagr_from_monthly_returns(rets)
            except (ValueError, ZeroDivisionError) as exc:
                results.append({"ticker": ticker, "index_name": meta[ticker], "error": str(exc)})
                continue

            if sd <= 0:
                results.append({
                    "ticker": ticker, "index_name": meta[ticker],
                    "error": "Zero volatility over the window; Sharpe undefined.",
                })
                continue

            results.append({
                "ticker": ticker,
                "index_name": meta[ticker],
                "std_dev_pct": round(sd, 3),
                "sharpe_ratio": round((cagr - rf) / sd, 3),
                "window_start": str(pts[0]["nav_date"]),
                "window_end": str(pts[-1]["nav_date"]),
                "months_used": len(rets),
            })
        return results

    windowed = {label: _compute_for(m) for label, m in windows}

    if len(windows) == 1:
        (label, m), = windows
        return {
            "as_of": str(anchor),
            "risk_free_rate_pct": rf,
            "risk_free_rate_source": rf_source,
            "n_months": m,
            "window": label,
            "count": len(windowed[label]),
            "results": windowed[label],
        }

    return {
        "as_of": str(anchor),
        "risk_free_rate_pct": rf,
        "risk_free_rate_source": rf_source,
        "windows": {label: m for label, m in windows},
        "count": {label: len(rows) for label, rows in windowed.items()},
        "results": windowed,
    }


@mcp.tool()
def data_status() -> dict:
    """Freshness of the NAV data this server is serving.

    The parquet is downloaded from Blob at startup and refreshed periodically in
    the background, so the numbers every other tool returns are anchored to the
    snapshot described here. Call this to check how current the data is, or when
    a fund/NAV you expect to exist seems to be missing.
    """
    con = _db()
    latest, earliest, schemes = con.execute(
        "SELECT max(nav_date), min(nav_date), count(DISTINCT scheme_code) "
        "FROM nav_history").fetchone()
    age_h = (_dt.datetime.now(_dt.timezone.utc) - _LAST_RELOAD).total_seconds() / 3600
    return {
        "latest_nav_date": str(latest),
        "earliest_nav_date": str(earliest),
        "schemes": schemes,
        "snapshot_loaded_at_utc": _LAST_RELOAD.strftime("%Y-%m-%d %H:%M:%SZ"),
        "snapshot_age_hours": round(age_h, 2),
        "refresh_interval_hours": (BLOB_REFRESH_SECONDS / 3600) if BLOB_REFRESH_SECONDS else None,
        "auto_refresh": bool(BLOB_REFRESH_SECONDS and AZURE_CONN),
        "source": "azure-blob" if AZURE_CONN else "local-parquet",
    }


@mcp.tool()
def list_categories() -> dict:
    """List every distinct category name present in scheme_master (sorted)."""
    rows = _db().execute(
        """SELECT DISTINCT category FROM scheme_master
           WHERE category IS NOT NULL ORDER BY category"""
    ).fetchall()
    return {"categories": [r[0] for r in rows]}


@mcp.tool()
def list_funds_in_category(category: str) -> dict:
    """List every scheme in a category with its scheme_code, name and fund house.

    Args:
        category: Category name (case-insensitive).
    """
    rows = _db().execute(
        """SELECT scheme_code, scheme_name, fund_house FROM scheme_master
           WHERE UPPER(category) = UPPER(?) ORDER BY fund_house, scheme_name""",
        [category],
    ).fetchall()
    funds = [{"scheme_code": r[0], "scheme_name": r[1], "fund_house": r[2]} for r in rows]
    return {"category": category.upper(), "total": len(funds), "funds": funds}


if __name__ == "__main__":
    # Transport selection:
    #   MCP_TRANSPORT=stdio  -> local use with Cursor / Claude Desktop (original mode)
    #   otherwise            -> streamable HTTP, for hosting as a claude.ai connector.
    # App Service (and most PaaS) inject the listen port via $PORT; default to 8000
    # locally. Bind 0.0.0.0 so the container's port mapping can reach it. The MCP
    # endpoint is served at /mcp on the deployed host.
    transport = os.environ.get("MCP_TRANSPORT", "http")
    if transport == "stdio":
        mcp.run(transport="stdio")
    else:
        # stateless_http: each request is self-contained (no server-side session
        # pinned to one worker). Required for horizontally-scaled hosts like App
        # Service, and it's what the claude.ai connector expects.
        mcp.run(
            transport="http",
            host="0.0.0.0",
            port=int(os.environ.get("PORT", "8000")),
            stateless_http=True,
        )
