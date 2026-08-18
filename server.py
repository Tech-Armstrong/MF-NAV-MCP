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


CON = _build_connection()


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
    meta_rows = CON.execute(
        f"""SELECT scheme_code, scheme_name, fund_house, category
            FROM scheme_master WHERE scheme_code IN ({ph})""",
        codes,
    ).fetchall()
    meta = {r[0]: {"scheme_name": r[1], "fund_house": r[2], "category": r[3]}
            for r in meta_rows}

    # 2) per-fund anchor (last NAV) and inception (first NAV), one grouped query
    anchor_rows = CON.execute(
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
        CON.execute("CREATE OR REPLACE TEMP TABLE _targets("
                    "scheme_code VARCHAR, start_target DATE, end_target DATE);")
        CON.executemany("INSERT INTO _targets VALUES (?, ?, ?);", targets)

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
        for c, d, v in CON.execute(start_sql).fetchall():
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
        for c, d, v in CON.execute(end_sql).fetchall():
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
        CON.execute("SELECT 1 FROM index_master LIMIT 1;")
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

    meta_rows = CON.execute(
        f"SELECT ticker, index_name FROM index_master WHERE ticker IN ({ph})",
        resolved,
    ).fetchall()
    meta = {r[0]: r[1] for r in meta_rows}

    anchor_rows = CON.execute(
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
        CON.execute("CREATE OR REPLACE TEMP TABLE _idx_targets("
                    "ticker VARCHAR, start_target DATE, end_target DATE);")
        CON.executemany("INSERT INTO _idx_targets VALUES (?, ?, ?);", targets)

        order = "DESC" if start_snap == "before" else "ASC"
        op = "<=" if start_snap == "before" else ">="
        for t, d, v in CON.execute(f"""
            SELECT t.ticker, h.nav_date, h.close
            FROM _idx_targets t
            JOIN index_history h
              ON h.ticker = t.ticker AND h.nav_date {op} t.start_target
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY t.ticker ORDER BY h.nav_date {order}) = 1
        """).fetchall():
            start_rows[t] = (d, v)

        for t, d, v in CON.execute("""
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
    rows = CON.execute(
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
    rows = CON.execute(
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
    rows = CON.execute("""
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


@mcp.tool()
def list_categories() -> dict:
    """List every distinct category name present in scheme_master (sorted)."""
    rows = CON.execute(
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
    rows = CON.execute(
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
