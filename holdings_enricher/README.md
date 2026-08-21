# Holdings Enricher

Pulls mutual-fund holdings from the **Advisorkhoj API** and enriches them with
AMFI sector, industry and market-cap data. Output feeds the MCP server's
holdings tools (`data/fund_holdings.json`).

---

## Project Structure

```
holdings_enricher/
│
├── main.py                  ← CLI entry point (run this)
├── requirements.txt
├── README.md
│
├── src/
│   ├── api_client.py        ← HTTP client for the Advisorkhoj API
│   ├── api_source.py        ← API rows → enricher record contract
│   ├── builder.py           ← Builds isin_mapping.json from AMFI + sector files
│   ├── matcher.py           ← Alias rules + fuzzy stock-name matching
│   ├── enricher.py          ← Joins holdings with AMFI data via ISIN
│   ├── nav_names.py         ← Resolves fund names to NAV parquet scheme names
│   ├── validator.py         ← Checks per-fund holding % sums
│   ├── writer.py            ← Writes colour-coded xlsx output
│   └── json_export.py       ← Writes the hierarchical JSON for the MCP server
│
├── data/
│   ├── amfi/                     ← AMFI reference workbook (place here)
│   ├── sector_files/             ← Sector source workbooks (place here)
│   ├── mappings/isin_mapping.json ← Auto-generated; commit to freeze it
│   └── nav_scheme_names.txt      ← Canonical NAV parquet fund names
│
└── outputs/                      ← All output files land here
```

---

## Setup

```bash
pip install -r requirements.txt
```

Set the API key in the project `.env` (the same file that holds
`AZURE_STORAGE_CONNECTION_STRING`):

```
API_KEY=<your advisorkhoj key>
```

`main.py` loads it automatically. Pass `--api-key` to override.

### One-time: place the AMFI source files

| File(s) | Folder |
|---------|--------|
| `AMFI_data_*.xlsx` | `data/amfi/` |
| `*multigroup*.xlsx`, `*utilities.xlsx` | `data/sector_files/` |

Then build the ISIN mapping:

```bash
python main.py --rebuild-mapping
```

This creates `data/mappings/isin_mapping.json`, which supplies **market-cap
category** — the one field the API does not return. Once built it is cached.

---

## Usage

### Discover what the API offers

```bash
python main.py --list-categories
python main.py --list-funds "Equity: Large Cap"
```

### Pull one category for one month

```bash
python main.py --category "Equity: Large Cap" --cap "Large Cap" \
    --year 2026 --month JUNE --json
```

### Several categories in one run (combined output)

```bash
python main.py --year 2026 --month JUNE --json \
    --category "Equity: Large Cap" --cap "Large Cap" \
    --category "Equity: Mid Cap"   --cap "Mid Cap"   \
    --category "Equity: Small Cap" --cap "Small Cap"
```

`--cap` labels must be in the same order as `--category`. The label is stamped
on every row, becomes `cap_category` in the JSON, and **scopes NAV name
resolution** — so it must match a category in `data/nav_scheme_names.txt`.

### Specific funds only

```bash
python main.py --cap "Large Cap" --year 2026 --month JUNE \
    --fund "Axis Large Cap Fund" --fund "SBI Large Cap Fund" --json
```

Names must be the API's *common* names — see `--list-funds`.

---

## CLI Reference

| Flag | Default | Description |
|------|---------|-------------|
| `--category` | — | API category to pull. Repeat for several. |
| `--cap` / `-c` | `""` | Cap label for each `--category` (same order). |
| `--fund` | — | Pull specific funds instead of a whole category. |
| `--year` | required | Portfolio year, e.g. `2026`. |
| `--month` | required | Portfolio month, e.g. `JUNE`. |
| `--list-categories` | off | Print the API's categories and exit. |
| `--list-funds CAT` | off | Print the funds in one category and exit. |
| `--api-key` | `$API_KEY` | Override the key from `.env`. |
| `--api-delay` | `0.3` | Pause between per-fund API calls, in seconds. |
| `--allow-partial` | off | Continue when some funds have no published portfolio. |
| `--skip-unresolved` | off | Write JSON even if some funds have no NAV name match. |
| `--output` / `-o` | `outputs/Holdings_Enriched_<ts>.xlsx` | Output xlsx path. |
| `--rebuild-mapping` | off | Force rebuild of `isin_mapping.json`. |
| `--fuzzy-cutoff` | `88` | Minimum fuzzy score (0–100). |
| `--tolerance` | `2.0` | Fund weight validation tolerance in ±pp. |
| `--json [PATH]` | off | **Also** write the fund profile as hierarchical JSON. |
| `--json-top-n` | `10` | Top holdings per fund in the JSON. |

---

## How enrichment works

The API supplies `isin`, `sector` and `industry` per holding; it does **not**
supply market-cap category. So each holding is resolved in this order:

1. **ISIN → AMFI mapping.** Exact, and it sidesteps instrument strings the name
   matcher cannot parse (`HDFC BANK LIMITED EQ NEW FV RE. 1/-`). Reported as
   `isin-exact`.
2. **Name fallback,** when the ISIN is absent from the mapping. A company's ISIN
   changes on corporate actions while AMFI still carries the old one — Kotak
   Bank arrives as `INE237A01028` but is mapped under `INE237A01036`. Reported
   as `isin-stale`.
3. **Unmatched.** No AMFI record; the holding keeps the API's own sector and
   industry, but has no market-cap category and is listed in `unmatched`.

Measured on Large Cap for June 2026: **1794/1794 holdings matched by ISIN
alone**, versus 19 unmatched holdings (9.69 pp of weight) on the old CSV path.

### Sector vocabulary

**AMFI is authoritative for sector and industry**, not the API. The two
disagree — AMFI says `Banking and Finance` where the API says
`Financial Services` — and the MCP server groups holdings by these strings, so
mixing them would split one real sector into two keys. The API's labels are
used only when AMFI has no record at all.

---

## Two guards that stop a bad dataset

Both are on by default. Each blocks the run rather than writing something that
looks complete but is not.

**Unpublished months.** A month is published progressively, so an early pull
returns nothing for some funds. Those funds are reported and the run stops.
Pass `--allow-partial` to write what is available.

**Unresolvable fund names.** Every fund must map to an exact `scheme_name` in
the NAV parquet, via `data/nav_scheme_names.txt`; a fund that does not is
invisible to the MCP tool. The export fails rather than dropping it silently.
Either add the fund to `nav_scheme_names.txt`, or pass `--skip-unresolved` to
omit it with a warning — appropriate when the API lists a genuinely new fund
your NAV data does not carry yet (e.g. JioBlackRock Large Cap, June 2026).

---

## Output Workbook

The output `.xlsx` has four sheets:

| Sheet | Contents |
|-------|----------|
| **Holdings Enriched** | All equity rows with AMFI Name, ISIN, Mkt Cap Cat, Mkt Cap ₹Cr, Industry, Sector |
| **Alias Report** | One row per unique stock — shows how it was matched |
| **Weight Validation** | Per-fund % sum check; flags funds outside ±2 pp of 100% |
| **Legend** | Colour key |

### Row colours

| Colour | Meaning |
|--------|---------|
| 🟢 Green  | Exact match — high confidence |
| 🟡 Yellow | Manual alias rule — curated |
| 🔵 Blue   | Fuzzy match ≥ 88 score — review advised |
| 🔴 Red    | No match found in AMFI data |
| ⚪ Grey   | Stock known to be absent from AMFI (unlisted / non-equity) |

---

## JSON Export (for the MCP server)

```bash
python main.py --category "Equity: Large Cap" --cap "Large Cap" \
    --year 2026 --month JUNE --json
```

The xlsx is **always** written; the JSON is purely additive. Both come from the
same enriched rows, so they cannot disagree.

```jsonc
{
  "generated_at": "2026-08-21T12:00:00",
  "fund_count": 34,
  "keyed_by": "nav_scheme_name",
  "funds": {
    "Axis Large Cap Fund - Regular - Growth": {
      "cap_category": "Large Cap",
      "as_of": "30-Jun-2026",
      "equity_pct": 95.67,              // total equity weight, NOT 100
      "market_cap": { "Large Cap": 91.21, "Mid Cap": 4.46, ... },
      "sectors": {                      // heaviest first
        "Banking and Finance": {
          "weight_pct": 30.1,
          "industries": { "Banks": 25.1 },
          "holdings": [ /* full holding dicts */ ]
        }
      },
      "top_holdings": [ /* descending by weight */ ],
      "holding_count": 44,
      "unmatched": [ /* only when non-empty */ ]
    }
  }
}
```

Weights are percentages of the **whole portfolio** and sum to the fund's equity
weight, not to 100 — cash, repo and derivatives are excluded. `unmatched` lists
equity rows that never resolved to an AMFI record, so a consumer can tell "no
exposure" apart from "held but unclassifiable".

To deploy, copy the JSON to `../data/fund_holdings.json`, commit, push, and sync
— see [OPERATIONS.md](../OPERATIONS.md); holdings data refreshes on **deploy**,
not on restart.

---

## Extending Alias Rules

Open `src/matcher.py` and add entries to the `ALIASES` dict:

```python
ALIASES = {
    "New Abbrev.": "Full AMFI Company Name ltd",  # matched stock name
    "Unlisted Co": None,                           # known absent from AMFI
}
```

Re-run `main.py` — no rebuild of `isin_mapping.json` needed.

---

## Note for Windows

The scripts print Unicode (`→`, `✅`, `₹`) which the default `cp1252` console
encoding cannot render — the run dies on the first status line. Set:

```bash
set PYTHONIOENCODING=utf-8      # cmd
$env:PYTHONIOENCODING="utf-8"   # PowerShell
```

The JSON file itself is always written UTF-8 regardless.
