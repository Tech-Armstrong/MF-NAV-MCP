"""
main.py  —  Holdings Enricher CLI
==================================

Pulls mutual-fund holdings from the Advisorkhoj API and enriches them with AMFI
sector, industry and market-cap data.

Usage
-----
# One category for one month:
    python main.py --category "Equity: Large Cap" --cap "Large Cap" \
        --year 2026 --month JUNE --json

# Several categories in one run (combined output):
    python main.py --year 2026 --month JUNE --json \
        --category "Equity: Large Cap" --cap "Large Cap" \
        --category "Equity: Mid Cap"   --cap "Mid Cap"   \
        --category "Equity: Small Cap" --cap "Small Cap"

# Specific funds only (skips the category listing):
    python main.py --cap "Large Cap" --year 2026 --month JUNE \
        --fund "Axis Large Cap Fund" --fund "SBI Large Cap Fund"

# Discover what the API offers:
    python main.py --list-categories
    python main.py --list-funds "Equity: Large Cap"

# Rebuild isin_mapping.json from source AMFI / sector files:
    python main.py --rebuild-mapping

The API key is read from API_KEY in the project .env (or --api-key).

Optional flags
--------------
    --output PATH       custom output xlsx path  (default: outputs/Holdings_Enriched_<timestamp>.xlsx)
    --fuzzy-cutoff N    minimum fuzzy score (default 88)
    --tolerance N       fund weight sum tolerance in ±pp (default 2.0)
    --api-delay S       pause between per-fund API calls (default 0.3s)
    --allow-partial     continue even if some funds have no published portfolio
"""

import argparse
import json
import os
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
from src.api_client import AdvisorkhojClient, AdvisorkhojError, MONTHS
from src.api_source import fetch_category, DEFAULT_DELAY
from src.enricher import enrich, match_summary
from src.validator import validate_weights, validation_rows
from src.writer   import write_output
from src.json_export import write_fund_profile_json


# ── Helpers ────────────────────────────────────────────────────────────────

def load_dotenv():
    """
    Load the project .env so API_KEY is available.

    The enricher lives one level below the repo root, and the .env sits beside
    the repo (with AZURE_STORAGE_CONNECTION_STRING), so check both. Existing
    environment variables win — an explicitly exported key should not be
    overridden by a stale file.
    """
    for candidate in (PROJECT_ROOT.parent / ".env",
                      PROJECT_ROOT.parent.parent / ".env",
                      PROJECT_ROOT / ".env"):
        if not candidate.exists():
            continue
        for line in candidate.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


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
    priority = {"no-match": 0, "manual-none": 1, "ambiguous": 2, "fuzzy-high": 3,
                "isin-stale": 4, "manual-alias": 5, "prefix": 6,
                "exact-norm": 7, "isin-exact": 8}
    return sorted(seen.values(),
                  key=lambda r: (priority.get(r["Match Method"], 99), r["Stock"]))


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Pull mutual fund holdings from the Advisorkhoj API and "
                    "enrich them with AMFI sector/industry/market-cap data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--category",
        action="append",
        metavar="NAME",
        help='API category to pull, e.g. "Equity: Large Cap". Repeat for '
             "several. Use --list-categories to see the options.",
    )
    parser.add_argument(
        "--cap", "-c",
        action="append",
        metavar="LABEL",
        default=[],
        help='Cap category label for each --category (e.g. "Large Cap"). Must '
             "appear in the same order. This is stamped on every row, becomes "
             "cap_category in the JSON, and scopes NAV name resolution — so it "
             "must match a category in data/nav_scheme_names.txt.",
    )
    parser.add_argument(
        "--fund",
        action="append",
        metavar="NAME",
        help="Pull specific fund(s) by their API common name instead of a whole "
             "category. Repeat for several. Uses the first --cap as the label.",
    )
    parser.add_argument(
        "--year",
        type=int,
        metavar="YYYY",
        help="Portfolio year, e.g. 2026.",
    )
    parser.add_argument(
        "--month",
        metavar="MONTH",
        help=f"Portfolio month: {', '.join(m.title() for m in MONTHS)}.",
    )
    parser.add_argument(
        "--api-key",
        metavar="KEY",
        default=None,
        help="Advisorkhoj API key. Defaults to API_KEY from the project .env.",
    )
    parser.add_argument(
        "--api-delay",
        type=float,
        default=DEFAULT_DELAY,
        metavar="SECONDS",
        help=f"Pause between per-fund API calls (default {DEFAULT_DELAY}s).",
    )
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Continue when some funds have no published portfolio for the "
             "month. Off by default: a partial month silently produces a "
             "half-empty dataset that looks complete.",
    )
    parser.add_argument(
        "--list-categories",
        action="store_true",
        help="Print the API's categories and exit.",
    )
    parser.add_argument(
        "--list-funds",
        metavar="CATEGORY",
        default=None,
        help="Print the funds in one API category and exit.",
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
    parser.add_argument(
        "--skip-unresolved",
        action="store_true",
        help="Write the JSON even if some funds have no match in "
             "data/nav_scheme_names.txt, warning instead of failing. Use when "
             "the API lists a fund your NAV data does not carry yet (a new "
             "launch). Those funds are OMITTED from the JSON, so prefer adding "
             "them to nav_scheme_names.txt when they do exist.",
    )

    args = parser.parse_args()
    load_dotenv()

    # ── Discovery modes (no enrichment) ────────────────────────────────────
    if args.list_categories:
        client = AdvisorkhojClient(args.api_key)
        for c in client.get_categories():
            print(c)
        return

    if args.list_funds:
        client = AdvisorkhojClient(args.api_key)
        for f in client.get_schemes_in_category(args.list_funds):
            print(f)
        return

    # ── Rebuild mapping if requested (can run standalone) ──────────────────
    if args.rebuild_mapping and not (args.category or args.fund):
        load_mapping(rebuild=True)
        print("[main] Mapping rebuilt. Exiting.")
        return

    # ── Validate inputs ────────────────────────────────────────────────────
    if not args.category and not args.fund:
        parser.error("Provide --category (or --fund), plus --year and --month. "
                     "See --list-categories.")
    if not args.year or not args.month:
        parser.error("--year and --month are required "
                     '(e.g. --year 2026 --month JUNE).')

    # Pad cap labels with empty string if fewer provided than categories
    targets = args.category or ["(explicit funds)"]
    cap_labels = args.cap + [""] * (len(targets) - len(args.cap))

    # ── Load mapping + build matcher ───────────────────────────────────────
    isin_dict, amfi_by_norm = load_mapping(rebuild=args.rebuild_mapping)
    matcher = Matcher(amfi_by_norm, fuzzy_cutoff=args.fuzzy_cutoff,
                      known_isins=set(isin_dict))

    # ── Pull from the API + enrich ─────────────────────────────────────────
    try:
        client = AdvisorkhojClient(args.api_key)
    except AdvisorkhojError as e:
        print(f"[main] {e}")
        sys.exit(1)

    all_enriched = []
    all_non_equity = []
    reports = []

    for target, cap_label in zip(targets, cap_labels):
        try:
            records, non_equity, report = fetch_category(
                client,
                category=target,
                year=args.year,
                month=args.month,
                cap_category=cap_label,
                funds=args.fund if args.fund else None,
                delay=args.api_delay,
            )
        except AdvisorkhojError as e:
            print(f"[main] API error for '{target}': {e}")
            sys.exit(1)

        reports.append(report)
        all_non_equity.extend(non_equity)
        enriched = enrich(records, isin_dict, matcher)
        all_enriched.extend(enriched)

        stats = match_summary(enriched)
        print(f"        {stats['matched']}/{stats['total']} matched "
              f"({stats['match_pct']}%) | by method: {stats['by_method']}")

        # Only the explicit-fund path ignores the category loop.
        if args.fund:
            break

    # ── Report funds with no published portfolio ───────────────────────────
    missing = [(r["category"], f) for r in reports for f in r["empty"]]
    failed  = [(r["category"], f) for r in reports for f in r["failed"]]

    if failed:
        print(f"\n[main] ⚠️  {len(failed)} fund(s) errored:")
        for cat, f in failed[:10]:
            print(f"        {f['fund']} — {f['error']}")

    if missing:
        print(f"\n[main] ⚠️  {len(missing)} fund(s) have no portfolio published "
              f"for {args.month} {args.year}:")
        for cat, name in missing[:10]:
            print(f"        {name}")
        if len(missing) > 10:
            print(f"        … and {len(missing) - 10} more")
        if not args.allow_partial:
            print("\n[main] Refusing to write a partial dataset. Either pick a "
                  "month that is fully published, or pass --allow-partial to "
                  "write what is available.")
            sys.exit(2)

    if not all_enriched:
        print("[main] No equity rows returned. Check the category, year and month.")
        sys.exit(2)

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
        write_fund_profile_json(all_enriched, json_path, top_n=args.json_top_n,
                                strict=not args.skip_unresolved)


if __name__ == "__main__":
    main()
