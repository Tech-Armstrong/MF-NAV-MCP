# Input holdings CSVs

Drop the monthly holdings files here, then point `--input` at them.

```bash
# from the holdings_enricher/ directory
python main.py \
  -i "input/Large Cap Holdings.csv" -c "Large Cap" \
  -i "input/Mid Cap Holdings.csv"   -c "Mid Cap"   \
  -i "input/Small Cap Holdings.csv" -c "Small Cap" \
  --json
```

`--cap` labels must appear in the same order as the `--input` files — the label
is stamped onto every row from that file and becomes `cap_category` in the JSON.

## Expected CSV format

As downloaded from Value Research / MFI Explorer:

```
Mutliple Fund Holdings Download            <- title row (ignored)
Fund,Holding,Holding type,As of,Percentage,Sector,Rating
HDFC Large Cap Fund,Reliance Industries Ltd.,Equity,31-Jul-2026,8.50,Energy,
...
```

The parser scans for the first row containing both `Fund` and `Holding`, so
extra preamble above the header is fine. These column names must match exactly:

| Column | Used for |
|--------|----------|
| `Fund` | groups rows into funds |
| `Holding` | stock name, matched against AMFI |
| `Holding type` | only `Equity` rows are enriched |
| `As of` | portfolio date, carried into the output |
| `Percentage` | holding weight |
| `Sector` | raw source sector (AMFI sector wins in the output) |

Non-equity rows (cash, TREPS, repo, derivatives) are **not** dropped — their
weight is kept so the validator can reconcile each fund to a true 100%.

## Notes

- Relative paths resolve against your **current directory**, not the script, so
  either `cd` into `holdings_enricher/` first or pass absolute paths.
- Files here are gitignored — they are monthly inputs, not source code.
