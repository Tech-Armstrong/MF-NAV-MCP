"""
builder.py
----------
Builds (or rebuilds) the master stock mapping JSON from:
  - AMFI reference file  →  ISIN, market-cap category, industry, sector
  - Sector / multigroup xlsx files  →  numeric market cap (₹ Cr)

Run once (or whenever source files are updated):
    python -m src.builder
"""

import glob
import json
import re
from pathlib import Path

import openpyxl

# ── Paths (relative to project root) ──────────────────────────────────────
DATA_DIR        = Path(__file__).resolve().parent.parent / "data"
AMFI_GLOB       = str(DATA_DIR / "amfi" / "*.xlsx")
SECTOR_GLOB     = str(DATA_DIR / "sector_files" / "*.xlsx")
MAPPING_OUT     = DATA_DIR / "mappings" / "isin_mapping.json"


# ── Text normaliser ────────────────────────────────────────────────────────
_STRIP_SUFFIX = re.compile(
    r"\b(ltd\.?|limited|pvt\.?|private|inc\.?|corp\.?|co\.?|llp|llc|"
    r"holdings?|hold\.?|industries?|ind\.?|enterprises?|enterp\.?|"
    r"solutions?|sol\.?|services?|serv\.?|technologies?|tech\.?|"
    r"pharmaceuticals?|pharma\.?|chemicals?|chem\.?|finance|fin\.?|"
    r"bank|group|grp\.?|international|intl\.?)\s*$",
    re.IGNORECASE,
)
_STRIP_CHARS  = re.compile(r"[.\-&()/,\'`]")
_MULTI_SPACE  = re.compile(r"\s+")


def normalize(name: str) -> str:
    """Lowercase, strip punctuation, strip common corporate suffixes."""
    if not name:
        return ""
    n = name.lower()
    n = _STRIP_CHARS.sub(" ", n)
    for _ in range(5):
        prev = n.strip()
        n = _STRIP_SUFFIX.sub("", n).strip()
        if n == prev:
            break
    return _MULTI_SPACE.sub(" ", n).strip()


# ── Load AMFI reference ────────────────────────────────────────────────────
def load_amfi(glob_pattern: str) -> dict:
    """
    Returns:
        amfi_by_isin  : {isin -> {name, mktcap_category, industry, sector}}
        amfi_by_norm  : {normalized_name -> isin}
    """
    files = glob.glob(glob_pattern)
    if not files:
        raise FileNotFoundError(f"No AMFI file found at: {glob_pattern}")

    amfi_by_isin = {}
    amfi_by_norm = {}

    for fpath in files:
        wb = openpyxl.load_workbook(fpath, read_only=True, data_only=True)
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            headers = [str(c.value or "").strip() for c in next(ws.iter_rows(min_row=1, max_row=1))]

            try:
                col_name   = headers.index("Company Name")
                col_isin   = headers.index("ISIN Code")
                col_mc_cat = headers.index("Market Cap")
                col_ind    = headers.index("Industry")
                col_sec    = headers.index("Sector")
            except ValueError:
                continue  # sheet not in expected format

            for row in ws.iter_rows(min_row=2, values_only=True):
                isin = row[col_isin]
                name = row[col_name]
                if not isin or not name:
                    continue
                isin = str(isin).strip()
                rec = {
                    "amfi_name":       str(name).strip(),
                    "mktcap_category": str(row[col_mc_cat] or "").strip(),
                    "industry":        str(row[col_ind]    or "").strip(),
                    "sector":          str(row[col_sec]    or "").strip(),
                }
                amfi_by_isin[isin] = rec
                amfi_by_norm[normalize(name)] = isin

        wb.close()

    print(f"[builder] AMFI loaded: {len(amfi_by_isin)} ISINs from {len(files)} file(s)")
    return amfi_by_isin, amfi_by_norm


# ── Load numeric market caps from sector xlsx files ────────────────────────
def load_sector_numeric_mc(glob_pattern: str) -> dict:
    """Returns {isin -> market_cap_crores (float)}"""
    isin_mc = {}
    files = glob.glob(glob_pattern)
    if not files:
        print(f"[builder] WARNING: No sector files found at {glob_pattern}")
        return isin_mc

    for fpath in files:
        wb = openpyxl.load_workbook(fpath, read_only=True, data_only=True)
        ws = wb.active
        headers = [str(c.value or "").strip() for c in next(ws.iter_rows(min_row=1, max_row=1))]

        try:
            col_isin = headers.index("ISIN")
            # 'Market Capitalization' may appear twice; take first occurrence
            col_mc   = next(i for i, h in enumerate(headers) if h == "Market Capitalization")
        except (ValueError, StopIteration):
            wb.close()
            continue

        for row in ws.iter_rows(min_row=2, values_only=True):
            isin = row[col_isin]
            mc   = row[col_mc]
            if isin and mc is not None:
                isin_str = str(isin).strip()
                if isin_str not in isin_mc:
                    isin_mc[isin_str] = float(mc)

        wb.close()

    print(f"[builder] Sector files loaded: {len(isin_mc)} ISINs with numeric MC from {len(files)} file(s)")
    return isin_mc


# ── Build and persist the master ISIN mapping ──────────────────────────────
def build_isin_mapping(
    amfi_glob:   str = AMFI_GLOB,
    sector_glob: str = SECTOR_GLOB,
    output_path: Path = MAPPING_OUT,
) -> dict:
    """
    Merges AMFI data and numeric market cap into one dict keyed by ISIN.
    Saves result to output_path as JSON.

    Schema:
    {
      "INE002A01018": {
        "amfi_name":       "Reliance Industries ltd",
        "mktcap_category": "Large Cap",
        "mktcap_cr":       1764237.07,
        "industry":        "Refineries/Petro-Products",
        "sector":          "Oil & Gas"
      },
      ...
    }
    """
    amfi_by_isin, amfi_by_norm = load_amfi(amfi_glob)
    isin_mc = load_sector_numeric_mc(sector_glob)

    master = {}
    for isin, rec in amfi_by_isin.items():
        master[isin] = {
            **rec,
            "mktcap_cr": isin_mc.get(isin),  # None if not found in sector files
        }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(master, f, indent=2, ensure_ascii=False, default=str)

    print(f"[builder] isin_mapping.json saved → {output_path}  ({len(master)} ISINs)")
    return master, amfi_by_norm


# ── CLI entry ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    build_isin_mapping()
