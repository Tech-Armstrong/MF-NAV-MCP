# Holdings Enricher

Enriches mutual-fund holdings CSVs with AMFI sector, industry, and market-cap data.

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
│   ├── builder.py           ← Builds isin_mapping.json from AMFI + sector files
│   ├── matcher.py           ← Alias rules + fuzzy stock-name matching
│   ├── parser.py            ← Reads raw holdings CSVs
│   ├── enricher.py          ← Joins holdings with AMFI data via ISIN
│   ├── validator.py         ← Checks per-fund holding % sums
│   └── writer.py            ← Writes colour-coded xlsx output
│
├── input/                                ← Monthly holdings CSVs go here
│
├── data/
│   ├── amfi/
│   │   └── AMFI_data_JAN_JUL_2026.xlsx   ← AMFI reference file (place here)
│   ├── sector_files/
│   │   └── 2026-07-21-multigroup*.xlsx   ← Sector source files (place here)
│   │   └── 2026-07-21-utilities.xlsx
│   └── mappings/
│       └── isin_mapping.json             ← Auto-generated; commit to freeze it
│
└── outputs/                              ← All output files land here
```

---

## Setup

```bash
pip install -r requirements.txt
```

---

## One-time: Place Source Files

| File(s) | Folder |
|---------|--------|
| `AMFI_data_JAN_JUL_2026.xlsx` | `data/amfi/` |
| `2026-07-21-multigroup*.xlsx`, `2026-07-21-utilities.xlsx` | `data/sector_files/` |

Then build the ISIN mapping:

```bash
python main.py --rebuild-mapping
```

This creates `data/mappings/isin_mapping.json`. Once built it is cached — subsequent runs load it instantly.

---

## Usage

Put the monthly CSVs in `input/` (see `input/README.md` for the expected
format), then run from the `holdings_enricher/` directory.

### Single file

```bash
python main.py --input "input/Large Cap Holdings.csv" --cap "Large Cap"
```

### Multiple files (combined output)

```bash
python main.py \
  --input "input/Large Cap Holdings.csv"  --cap "Large Cap"  \
  --input "input/Mid Cap Holdings.csv"    --cap "Mid Cap"    \
  --input "input/Small Cap Holdings.csv"  --cap "Small Cap"
```

`--cap` labels must be in the same order as the `--input` files. Relative paths
resolve against your current directory, not the script.

### Custom output path

```bash
python main.py \
  --input "Large Cap Holdings.csv" --cap "Large Cap" \
  --output "outputs/my_report.xlsx"
```

### Force rebuild mapping (after updating AMFI / sector files)

```bash
python main.py --rebuild-mapping \
  --input "Large Cap Holdings.csv" --cap "Large Cap"
```

---

## Output Workbook

The output `.xlsx` has four sheets:

| Sheet | Contents |
|-------|----------|
| **Holdings Enriched** | All equity rows with AMFI Name, ISIN, Mkt Cap Cat, Mkt Cap ₹Cr, Industry, Sector |
| **Alias Report** | One row per unique stock — shows how it was matched, for validation |
| **Weight Validation** | Per-fund % sum check; flags funds outside ±2 pp of 100% |
| **Legend** | Colour key |

### Row colours (Holdings Enriched / Alias Report)

| Colour | Meaning |
|--------|---------|
| 🟢 Green  | Exact normalised match — high confidence |
| 🟡 Yellow | Manual alias rule — curated |
| 🔵 Blue   | Fuzzy match ≥ 88 score — review advised |
| 🔴 Red    | No match found in AMFI data |
| ⚪ Grey   | Stock known to be absent from AMFI (unlisted / non-equity) |

---

## Extending Alias Rules

Open `src/matcher.py` and add entries to the `ALIASES` dict:

```python
ALIASES = {
    ...
    "New Abbrev.": "Full AMFI Company Name ltd",  # matched stock name
    "Unlisted Co": None,                           # known absent from AMFI
}
```

Re-run `main.py` — no rebuild of `isin_mapping.json` needed.

---

## CLI Reference

| Flag | Default | Description |
|------|---------|-------------|
| `--input` / `-i` | required | Holdings CSV path. Repeat for multiple files. |
| `--cap` / `-c` | `""` | Cap category label for each `--input` (same order). |
| `--output` / `-o` | `outputs/Holdings_Enriched_<ts>.xlsx` | Output xlsx path. |
| `--rebuild-mapping` | off | Force rebuild of `isin_mapping.json`. |
| `--fuzzy-cutoff` | `88` | Minimum fuzzy score (0–100). |
| `--tolerance` | `2.0` | Fund weight validation tolerance in ±pp. |
| `--json [PATH]` | off | **Also** write the fund profile as hierarchical JSON. Path optional — defaults to the xlsx path with a `.json` suffix. |
| `--json-top-n` | `10` | Top holdings per fund in the JSON. |

---

## JSON Export (for the MCP server)

The xlsx **Fund Profile** sheet is flat — one row per fund, one column per
sector — which reads well in a spreadsheet but is awkward for a tool answering
"what is this fund's exposure to Financial Services?". `--json` writes the same
data nested instead:

```bash
python main.py -i "Large Cap Holdings.csv" -c "Large Cap" --json
```

The xlsx is **always** written; the JSON is purely additive. Both come from the
same enriched rows, so they cannot disagree — the cap and sector totals
reconcile exactly.

```jsonc
{
  "generated_at": "2026-08-13T19:29:50",
  "fund_count": 1,
  "funds": {
    "<fund name>": {
      "cap_category": "Large Cap",
      "as_of": "31-Jul-2026",
      "equity_pct": 42.5,              // total equity weight, NOT 100
      "market_cap": { "Large Cap": 41.9, "Mid Cap": 0.0, ... },
      "sectors": {                      // heaviest first
        "Banking and Finance": {
          "weight_pct": 13.1,
          "industries": { "Banks": 13.1 },
          "holdings": [ /* full holding dicts */ ]
        }
      },
      "top_holdings": [ /* descending by weight */ ],
      "holding_count": 9,
      "unmatched": [ { "stock": "...", "weight_pct": 0.6 } ]
    }
  }
}
```

Weights are percentages of the **whole portfolio** and sum to the fund's equity
weight, not to 100 — cash, repo and derivatives are excluded. `unmatched` lists
equity rows that never resolved to an AMFI record, so a consumer can tell "no
exposure" apart from "held but unclassifiable".

---

## Note for Windows

The scripts print Unicode (`→`, `✅`, `₹`) which the default `cp1252` console
encoding cannot render — the run dies on the first status line. Until that is
fixed in the code, set:

```bash
set PYTHONIOENCODING=utf-8      # cmd
$env:PYTHONIOENCODING="utf-8"   # PowerShell
```

The JSON file itself is always written UTF-8 regardless.
