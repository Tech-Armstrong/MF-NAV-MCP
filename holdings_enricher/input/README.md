# input/ — legacy CSV holdings (no longer used)

The enricher now pulls holdings from the Advisorkhoj API; there is no CSV input
path. See the top-level README for the current workflow:

```bash
python main.py --category "Equity: Large Cap" --cap "Large Cap" \
    --year 2026 --month JUNE --json
```

The CSVs still here are the monthly files from the Value Research era, kept as a
record of what was loaded before the switch. Nothing reads them. They are
gitignored, so a fresh clone sees only this note.
