# NAV Analytics MCP Server (standalone)

A self-contained MCP server that exposes Indian mutual fund NAV analytics as
tools, querying **parquet** data through **DuckDB** — local files or Azure Blob.
No external package or framework of yours is required. The server runs only
read-only `SELECT`s and never fabricates data.

Tested with `mcp==1.28.1`, `duckdb==1.5.4`, `python-dateutil==2.9.0.post0`.

## Files

    server.py            the MCP server (all tool logic; self-contained)
    make_sample_data.py  SYNTHETIC fixture generator — dev/smoke-test only
    requirements.txt     pinned deps

## Install

    python3 -m venv venv
    source venv/bin/activate        # Windows: venv\Scripts\activate
    pip install -r requirements.txt

## Data

The server expects two parquet sources with this schema:

    nav_history     scheme_code VARCHAR, nav_date DATE, nav DOUBLE
    scheme_master   scheme_code VARCHAR, scheme_name VARCHAR,
                    fund_house VARCHAR, category VARCHAR

Point it at data with environment variables (defaults shown):

    NAV_HISTORY_PATH     ./data/nav_history.parquet
    SCHEME_MASTER_PATH   ./data/scheme_master.parquet

**No data yet?** Generate a synthetic fixture to prove the server runs:

    python make_sample_data.py      # writes ./data/*.parquet

The fixture is a random walk — **not real fund data**. Replace it with real
AMFI-derived parquet before trusting any number.

**Azure Blob.** Set a connection string and the paths may be `az://` URLs; the
server loads DuckDB's `azure` extension and registers a secret automatically:

    export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=...;AccountKey=...;"
    export NAV_HISTORY_PATH="az://mycontainer/nav/*.parquet"
    export SCHEME_MASTER_PATH="az://mycontainer/scheme_master.parquet"

## Run

    python server.py                     # streamable HTTP at /mcp (default)
    MCP_TRANSPORT=stdio python server.py  # stdio (for local Cursor / Claude Desktop)

The HTTP mode is what you host as a **claude.ai custom connector** (below). The
stdio mode is the original local mode for Cursor / Claude Desktop.

## Host it for the team as a claude.ai connector

Goal: your team uses this in the **claude.ai web app**, plug-and-play — each
teammate adds one URL, nobody installs Python or handles the Azure key. That
means hosting the server once over HTTPS with OAuth.

### 1. Rotate the Azure key first

The old `.cursor/mcp.json` committed a live Storage account key. **Rotate it in
the Azure portal.** The new key goes only into the host's settings below — never
into a committed file.

### 2. Deploy (Azure App Service)

A `Dockerfile` is included. Build/push the image (or use App Service's
build-from-source), then set these **application settings**:

    AZURE_STORAGE_CONNECTION_STRING   <rotated connection string>
    NAV_HISTORY_PATH                  az://mfnavdata/processed/nav_history/year=*/*.parquet
    SCHEME_MASTER_PATH                az://mfnavdata/processed/scheme_master.parquet
    PUBLIC_BASE_URL                   https://<your-app>.azurewebsites.net

App Service terminates HTTPS and injects `$PORT` automatically.

**Auth.** Setting `PUBLIC_BASE_URL` turns on OAuth: the server runs its own
self-contained OAuth authorization server (fastmcp's `InMemoryOAuthProvider`) and
advertises the discovery endpoints claude.ai probes — **no Azure App
Registration, external identity provider, or scope configuration required.**
claude.ai registers itself dynamically and completes the handshake. Any client
that completes the flow is granted access, so this gates on *knowing the URL* plus
the OAuth handshake — appropriate for a small trusted team behind a URL you
control. (Tokens are in-memory: a server restart means teammates click "reconnect"
once. To add real per-user identity + revocation later, swap in a hosted provider
— Google/GitHub/WorkOS — by editing only `_build_auth()` in `server.py`.)

If `PUBLIC_BASE_URL` is **unset**, the server runs **unauthenticated** — use that
only for local testing, never public hosting.

### 3. Each teammate adds the connector (one-time)

In claude.ai → **Settings → Connectors → Add custom connector** → paste:

    https://<your-app>.azurewebsites.net/mcp

Complete the Entra sign-in when prompted. The 5 NAV tools then appear in the
connector picker in any chat. (Personal claude.ai plans can't push org-wide, so
each teammate does this once — but that's the whole setup.)

## Register with a local client (stdio)

For local use in **Cursor** (`.cursor/mcp.json`) or **Claude Desktop**
(`claude_desktop_config.json`) — set `MCP_TRANSPORT=stdio`:

```json
{
  "mcpServers": {
    "nav-analytics": {
      "command": "/absolute/path/to/venv/bin/python",
      "args": ["/absolute/path/to/server.py"],
      "env": {
        "MCP_TRANSPORT": "stdio",
        "NAV_HISTORY_PATH": "/absolute/path/to/data/nav_history.parquet",
        "SCHEME_MASTER_PATH": "/absolute/path/to/data/scheme_master.parquet"
      }
    }
  }
}
```

Use absolute paths for `command`, `args`, and any file paths; MCP clients don't
run in your project directory. For Azure, put the connection string in `env`
instead of the local paths.

## Tools

- **search_funds(query, limit=10)** — fuzzy fund-name → scheme_code resolver.
  Call this first when the user names a fund; feed the resulting code into the
  returns tools. Returns Regular/Direct and Growth/IDCW variants separately.
- **get_fund_returns(scheme_codes, period)** — point-to-point `return_pct` and
  annualized `cagr_pct` for one or many funds. `cagr_pct` is populated only for
  windows longer than a year (2Y/3Y/5Y, and SI when the fund is >1yr old); it is
  `null` for shorter windows.
- **get_fund_returns_between(scheme_codes, start_date, end_date)** — the same
  numbers over an **explicit** ISO `YYYY-MM-DD` range instead of a named period,
  for when the user gives actual dates. Unlike the named periods, the window is
  the same absolute pair for every fund rather than anchored per-fund. Both ends
  snap to the latest NAV on/before the requested date, so the realized window can
  be a day or two narrower — read `start_nav_date` / `end_nav_date` for what was
  actually used. `cagr_pct` follows the realized duration (>1yr). A `start_date`
  before a fund's inception gives that fund an error row rather than silently
  starting at inception.
- **get_category_returns(category, period, sort_by, ascending, staleness_days=7)**
  — returns for every fund in a category, ranked, with a staleness guard.
- **list_categories()** / **list_funds_in_category(category)** — discovery.

Periods: `1W 2W 1M 3M 6M 9M 1Y 2Y 3Y 5Y YTD MTD SI`.

### Benchmark indices

Three more tools mirror the fund ones for market indices, so a fund and its
benchmark can be compared over an identical window:

- **list_indices()** — the available indices with ticker, name and history span.
- **get_index_returns(tickers, period)** — mirrors `get_fund_returns`: same
  period strings, same window conventions, same maths.
- **get_index_returns_between(tickers, start_date, end_date)** — mirrors
  `get_fund_returns_between`.

Tickers accept friendly aliases (`NIFTY50`, `NIFTY100`, `NIFTY500`, `SENSEX`,
`MIDCAP50`) or the raw Yahoo symbol (`^NSEI`). Five broad indices are covered —
Nifty 50, Nifty 100, Nifty 500, BSE Sensex, Nifty Midcap 50 — with ~20 years of
daily closes each. Sector indices are deliberately excluded: Yahoo returns only
a handful of rows for them, so their returns would be silently wrong.

**Index data is committed to this repo**, not fetched at runtime — see
`data/index_history.parquet`. The server reads it as a plain local file, so
there is no Yahoo dependency, no rate limit and no network call in the request
path. Refresh it with:

    pip install -r requirements-dev.txt
    python fetch_index_data.py
    git add data/index_history.parquet data/index_master.parquet

The data is therefore only as current as the last deploy.

> **Price return vs total return.** Index levels from Yahoo are **price
> return** — they exclude dividends — while fund NAVs are **total return**.
> Comparing them directly flatters the fund by roughly 1–1.5%/yr for Indian
> equity. Every index response carries `return_type: "price"`; say so when
> presenting a fund-vs-benchmark comparison.

## Conventions worth knowing

- **Per-fund anchor.** Every window ends at each fund's own latest NAV, not the
  calendar today (the latest NAV may lag a day or two; non-trading days have no
  NAV). This matches how Value Research / ET Money report.
- **Full-period start.** For trailing windows the start snaps to the latest NAV
  on/before `(anchor − period)` so you capture a complete period. YTD/MTD anchor
  to the first NAV on/after the calendar start; SI starts at inception.
- **Staleness.** In category queries, a fund whose last NAV lags the category's
  freshest as-of date by more than `staleness_days` is flagged `stale`, kept in
  the results list, and excluded from `avg_return_pct` / `avg_cagr_pct` so the
  averages never blend mismatched as-of dates.
- **One connection.** The server opens a single read-only DuckDB connection at
  startup and holds it for the process lifetime.

## Before trusting the numbers

Validate the start-boundary convention against a golden reference: take one real
fund, compute 1Y/3Y/5Y, and compare to its published figures for the **same
as-of date** (align the as-of first, or a date mismatch will look like a math
bug). If trailing returns come out consistently low, flip the trailing start
snap from on/before to on/after `(anchor − period)` in `_resolve_window`.
