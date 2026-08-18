"""
nav_names.py
------------
Resolves a holdings fund name to the exact scheme_name used in the NAV parquet.

Why this exists
---------------
The two sources name the same fund differently and NEVER agree on the raw
string. Value Research (holdings) writes plan/option as a suffix:

    Sundaram Large Cap Fund - Regular (G)

while the AMFI-derived NAV parquet spells it out:

    Sundaram Large Cap Fund - Regular - Growth

Measured over 103 funds: 0 exact matches, 97 resolvable by fuzzy match after
normalisation, 6 unresolvable because they are AMC abbreviations —
"Aditya Birla SL" vs "ABSL", "Bank of India" vs "BOI". Those 6 are the reason
this is not just a fuzzy call: at a naive threshold the matcher does not merely
miss them, it confidently proposes the WRONG fund (it offered "ITI Large Cap"
for an Aditya Birla fund at 63%). Silently attributing one AMC's holdings to
another is the worst possible failure here, so unresolved names raise instead.

Strategy, in order:
    1. normalise away plan/option noise and punctuation
    2. apply the AMC alias table (abbreviation expansion)
    3. exact match on the normalised form
    4. fuzzy match, but only above FUZZY_FLOOR and only within the same cap
       category — a Large Cap fund can never resolve to a Mid Cap scheme
    5. otherwise: unresolved, and the caller decides whether to fail
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from rapidfuzz import fuzz, process

# Below this score a fuzzy match is not trustworthy. The 97 correct pairings in
# the reference set all scored >=86; the 6 wrong ones scored <=77.
FUZZY_FLOOR = 85

DEFAULT_NAV_NAMES = Path(__file__).resolve().parent.parent / "data" / "nav_scheme_names.txt"

# AMC name <-> abbreviation. Applied to the normalised string, longest first so
# "aditya birla sl" wins over a bare "aditya birla". Extend this when a new AMC
# appears with a different abbreviation on each side.
AMC_ALIASES = {
    "aditya birla sl": "absl",
    "aditya birla sun life": "absl",
    "aditya birla": "absl",
    "bank of india": "boi",
    # The NAV data uses BOTH forms: "ICICI Pru" for the equity categories
    # (Flexicap, Large Cap, MidCap, Smallcap) and "ICICI Prudential" elsewhere
    # (Equity & Debt, Balanced Advantage, index funds). Collapsing to the short
    # form makes either side match whichever the other happens to use.
    "icici prudential": "icici pru",
    "the wealth company": "the wealth co",
    "trustmf": "trust mf",
}

# Plan / option / structural words that carry no identity. Removed from both
# sides before comparison.
#
# NOTE these run AFTER punctuation has already been collapsed to spaces, so the
# patterns must match the bare tokens — "(G)" has become " g " by this point,
# not "(g)". Matching on the parenthesised form silently left a stray "g" token
# behind, which cost real points on every "... Fund (G)" name.
_NOISE = [
    r"\bg\b", r"\bidcw\b", r"\bregular\b", r"\bdirect\b",
    r"\bgrowth\b", r"\bplan\b", r"\boption\b", r"\bfund\b",
]


def normalize(name: str) -> str:
    """Lowercase, strip punctuation and plan/option noise, expand AMC aliases."""
    s = name.lower().strip()
    s = s.replace("&", "and")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # Alias expansion before noise removal — aliases may contain noise words
    # ("the wealth company" -> "the wealth co").
    for long_form in sorted(AMC_ALIASES, key=len, reverse=True):
        if long_form in s:
            s = s.replace(long_form, AMC_ALIASES[long_form])
            break

    for pat in _NOISE:
        s = re.sub(pat, " ", s)

    # Collapse cap-size spellings so "Mid Cap" == "Midcap".
    s = re.sub(r"\blarge\s*cap\b", "largecap", s)
    s = re.sub(r"\bmid\s*cap\b", "midcap", s)
    s = re.sub(r"\bsmall\s*cap\b", "smallcap", s)

    return re.sub(r"\s+", " ", s).strip()


def load_nav_names(path: str | Path = DEFAULT_NAV_NAMES) -> List[Tuple[str, str]]:
    """Read the reference file -> [(cap_category, exact_scheme_name), ...]."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"NAV scheme-name reference not found: {path}\n"
            "It maps holdings fund names to the exact names in the NAV parquet."
        )
    out: List[Tuple[str, str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "|" not in line:
            continue
        cap, name = line.split("|", 1)
        out.append((cap.strip(), name.strip()))
    return out


class NavNameResolver:
    """Resolves holdings fund names to NAV parquet scheme names."""

    def __init__(self, nav_names: Optional[List[Tuple[str, str]]] = None,
                 fuzzy_floor: int = FUZZY_FLOOR):
        self.fuzzy_floor = fuzzy_floor
        entries = nav_names if nav_names is not None else load_nav_names()

        # normalised -> exact, indexed per cap category so a Large Cap fund can
        # never resolve to a Mid Cap scheme with a similar name.
        self.by_cap: Dict[str, Dict[str, str]] = {}
        self.all_norm: Dict[str, str] = {}
        for cap, exact in entries:
            n = normalize(exact)
            self.by_cap.setdefault(cap, {})[n] = exact
            self.all_norm[n] = exact

    def resolve(self, fund_name: str, cap_category: str = "") -> dict:
        """
        Returns {"nav_scheme_name": str|None, "method": str, "score": int}.

        method is one of: exact, alias-exact, fuzzy, unresolved.
        """
        n = normalize(fund_name)
        pool = self.by_cap.get(cap_category) or self.all_norm
        scoped = bool(self.by_cap.get(cap_category))

        if n in pool:
            return {"nav_scheme_name": pool[n],
                    "method": "exact", "score": 100}

        # Try the unscoped pool too — a mislabelled cap category should not
        # cause a miss, though we prefer the scoped hit above.
        if not scoped and n in self.all_norm:
            return {"nav_scheme_name": self.all_norm[n],
                    "method": "exact", "score": 100}

        best = process.extractOne(n, list(pool.keys()),
                                  scorer=fuzz.token_sort_ratio)
        if best and best[1] >= self.fuzzy_floor:
            return {"nav_scheme_name": pool[best[0]],
                    "method": "fuzzy", "score": int(round(best[1]))}

        return {"nav_scheme_name": None, "method": "unresolved",
                "score": int(round(best[1])) if best else 0}
