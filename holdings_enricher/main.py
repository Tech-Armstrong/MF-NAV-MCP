"""
main.py  —  Holdings Enricher CLI
==================================

Usage
-----
# Enrich a single holdings file:
    python main.py --input "Large Cap Holdings.csv" --cap "Large Cap"

# Enrich multiple files at once:
    python main.py \
        --input "Large Cap Holdings.csv" --cap "Large Cap" \
        --input "Mid Cap Holdings.csv"   --cap "Mid Cap"   \
        --input "Small Cap holdings.csv" --cap "Small Cap"

# Rebuild isin_mapping.json from source files (run after updating AMFI / sector files):
    python main.py --rebuild-mapping

Optional flags
--------------
    --output PATH       custom output xlsx path  (default: outputs/Holdings_Enriched_<timestamp>.xlsx)
    --fuzzy-cutoff N    minimum fuzzy score (default 88)
    --tolerance N       fund weight sum tolerance in ±pp (default 2.0)
    --rebuild-mapping   rebuild isin_mapping.json before enriching
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Project root is the directory containing this file
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR     = PROJECT_ROOT / "data"
MAPPING_FILE = DATA_DIR / "mappings" / "isin_mapping.json"
OUTPUT_DIR   = PROJECT_ROOT / "outputs"

sys.path.insert(0, str(PROJECT_ROOT))

from src.builder  import build_isin_mapping
from src.matcher  import Matcher
from src.parser   import parse_holdings_csv
from src.enricher import enrich, match_summary
from src.validator import validate_weights, validation_rows
from src.writer   import write_output
from src.json_export import write_fund_profile_json


# ── Helpers ────────────────────────────────────────────────────────────────

def load_mapping(rebuild: bool = False):
    """Load (or build) isin_mapping.json. Returns (isin_dict, amfi_by_norm)."""
    if rebuild or not MAPPING_FILE.exists():
        print("[main] Building ISIN mapping from source files …")
        isin_dict, amfi_by_norm = build_isin_mapping(
            amfi_glob   = str(DATA_DIR / "amfi"         / "*.xlsx"),
            sector_glob = str(DATA_DIR / "sector_files" / "*.xlsx"),
            output_path = MAPPING_FILE,
        )
    else:
        print(f"[main] Loading cached mapping → {MAPPING_FILE}")
        with open(MAPPING_FILE, encoding="utf-8") as f:
            isin_dict = json.load(f)

        # Rebuild the name→isin index from the cached dict
        from src.matcher import normalize
        amfi_by_norm = {normalize(v["amfi_name"]): isin
                        for isin, v in isin_dict.items()
                        if v.get("amfi_name")}

    return isin_dict, amfi_by_norm


def build_alias_report(enriched_rows):
    """One row per unique stock name, sorted by match method then name."""
    seen = {}
    for row in enriched_rows:
        s = row["Stock"]
        if s not in seen:
            seen[s] = row
    priority = {"no-match": 0, "manual-none": 1, "fuzzy-high": 2,
                "manual-alias": 3, "exact-norm": 4}
    return sorted(seen.values(),
                  key=lambda r: (priority.get(r["Match Method"], 99), r["Stock"]))


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Enrich mutual fund holdings CSVs with AMFI sector/industry/market-cap data."
    )
    parser.add_argument(
        "--input", "-i",
        action="append",
        metavar="CSV_PATH",
        help="Path to a holdings CSV file. Repeat for multiple files.",
    )
    parser.add_argument(
        "--cap", "-c",
        action="append",
        metavar="LABEL",
        default=[],
        help='Cap category label for each --input file (e.g. "Large Cap"). '
             'Must appear in the same order as --input.',
    )
    parser.add_argument(
        "--output", "-o",
        metavar="XLSX_PATH",
        default=None,
        help="Output xlsx path. Defaults to outputs/Holdings_Enriched_<timestamp>.xlsx",
    )
    parser.add_argument(
        "--rebuild-mapping",
        action="store_true",
        help="Force rebuild of isin_mapping.json from source AMFI / sector files.",
    )
    parser.add_argument(
        "--fuzzy-cutoff",
        type=int,
        default=88,
        help="Minimum fuzzy match score (0-100, default 88).",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=2.0,
        help="Allowed deviation from 100%% for fund weight validation (default ±2 pp).",
    )
    parser.add_argument(
        "--json",
        nargs="?",
        const="",
        default=None,
        metavar="JSON_PATH",
        help="Also export the fund profile as hierarchical JSON for the MCP "
             "server. Optional path; defaults to the xlsx path with a .json "
             "suffix. The xlsx is always written regardless.",
    )
    parser.add_argument(
        "--json-top-n",
        type=int,
        default=10,
        help="How many top holdings to include per fund in the JSON (default 10).",
    )

    args = parser.parse_args()

    # ── Rebuild mapping if requested (can run standalone) ──────────────────
    if args.rebuild_mapping and not args.input:
        load_mapping(rebuild=True)
        print("[main] Mapping rebuilt. Exiting.")
        return

    # ── Validate inputs ────────────────────────────────────────────────────
    if not args.input:
        parser.error("Provide at least one --input CSV file.")

    # Pad cap labels with empty string if fewer provided than files
    cap_labels = args.cap + [""] * (len(args.input) - len(args.cap))

    # ── Load mapping + build matcher ───────────────────────────────────────
    isin_dict, amfi_by_norm = load_mapping(rebuild=args.rebuild_mapping)
    matcher = Matcher(amfi_by_norm, fuzzy_cutoff=args.fuzzy_cutoff,
                      known_isins=set(isin_dict))

    # ── Parse + enrich all input files ────────────────────────────────────
    all_enriched = []
    all_non_equity = []
    for csv_path, cap_label in zip(args.input, cap_labels):
        print(f"[main] Parsing  → {csv_path}  (cap: '{cap_label}')")
        records, non_equity = parse_holdings_csv(
            csv_path, cap_category=cap_label, return_non_equity=True
        )
        all_non_equity.extend(non_equity)
        enriched = enrich(records, isin_dict, matcher)
        all_enriched.extend(enriched)
        stats = match_summary(enriched)
        print(f"        {stats['matched']}/{stats['total']} matched "
              f"({stats['match_pct']}%) | by method: {stats['by_method']}")

    if not all_enriched:
        print("[main] No equity rows found. Check your input files.")
        return

    # ── Overall stats ──────────────────────────────────────────────────────
    overall = match_summary(all_enriched)
    print(f"\n[main] OVERALL: {overall['matched']}/{overall['total']} "
          f"= {overall['match_pct']}% matched")
    print(f"       Methods: {overall['by_method']}")

    # ── Validate weights ───────────────────────────────────────────────────
    fund_totals, flagged, equity_totals = validate_weights(
        all_enriched, tolerance=args.tolerance, non_equity=all_non_equity
    )
    v_rows = validation_rows(fund_totals, flagged, tolerance=args.tolerance,
                             equity_totals=equity_totals)

    if flagged:
        print(f"\n[main] ⚠️  {len(flagged)} fund(s) outside ±{args.tolerance}% tolerance:")
        for f in flagged:
            print(f"        {f}  →  {fund_totals[f]:.2f}% "
                  f"(equity {equity_totals.get(f, 0):.2f}%)")
    else:
        print(f"\n[main] ✅  All {len(fund_totals)} funds reconcile to 100% "
              f"(±{args.tolerance}pp)")

    # ── Build alias report ─────────────────────────────────────────────────
    alias_rows = build_alias_report(all_enriched)

    # ── Write output ───────────────────────────────────────────────────────
    if args.output:
        out_path = Path(args.output)
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = OUTPUT_DIR / f"Holdings_Enriched_{ts}.xlsx"

    write_output(all_enriched, alias_rows, v_rows, out_path)
    print(f"\n[main] Done ✓  →  {out_path}")

    # ── Optional: hierarchical JSON for the MCP server ─────────────────────
    # Additive only — the xlsx above is written either way. Built from the same
    # enriched rows, so the two can never disagree.
    if args.json is not None:
        json_path = Path(args.json) if args.json else out_path.with_suffix(".json")
        write_fund_profile_json(all_enriched, json_path, top_n=args.json_top_n)


if __name__ == "__main__":
    main()
