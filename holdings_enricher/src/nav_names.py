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

# Product-name synonyms: some AMCs market a fund under one name while AMFI's
# NAV data carries the SEBI-category name instead — same scheme_code, wholly
# different words, so fuzzy matching alone won't clear FUZZY_FLOOR (e.g. Baroda
# BNP Paribas 145387 / Sundaram 149715 are sold as "Dynamic Asset Allocation"
# but the NAV parquet lists both as "Balanced Advantage"). Applied the same way
# as AMC_ALIASES — longest phrase first, before noise removal.
PRODUCT_ALIASES = {
    "dynamic asset allocation": "balanced advantage",
    # HDFC (103131) and ICICI Prudential (101144) brand their Multi Asset
    # Allocation scheme without "Allocation" in the NAV data ("HDFC
    # Multi-Asset Fund", "ICICI Prudential Multi-Asset Fund"), while every
    # other AMC's NAV name keeps the word. Applied symmetrically by
    # normalize(), so it collapses "allocation" away on both sides rather
    # than risking a one-sided alias — without it, HDFC's holdings-source
    # name fuzzy-matched HSBC's NAV name at 92.6 (blocked only by the
    # same-AMC guard, not a safe outcome to rely on).
    "multi asset allocation": "multi asset",
}

# Plan / option / structural words that carry no identity. Removed from both
# sides before comparison.
#
# NOTE these run AFTER punctuation has already been collapsed to spaces, so the
# patterns must match the bare tokens — "(G)" has become " g " by this point,
# not "(g)". Matching on the parenthesised form silently left a stray "g" token
# behind, which cost real points on every "... Fund (G)" name.
#
# The Advisorkhoj API spells IDCW out in SEBI's full legal form —
# "Income Distribution Cum Capital Withdrawal Option (IDCW)" — which is six
# tokens of pure noise. Left in, it swamped the comparison: an SBI fund scored
# 52 against its own NAV name and the export refused it. The long form is
# stripped BEFORE the short patterns so the phrase goes as a unit.
_NOISE = [
    # Longest first: "payout of ..." contains "income distribution ...", so the
    # inner phrase must not strip first and strand a bare "payout of".
    #
    # "cumcapital" (no space) is not a typo here — Kotak's API name really does
    # run the words together, and it scored 46 against its own NAV name.
    # "re investment" is the same word: the API writes "Re-Investment", and the
    # hyphen has already become a space by the time these patterns run.
    r"\bre ?investment of income distribution cum ?capital withdrawal\b",
    r"\bpayout of income distribution cum ?capital withdrawal\b",
    r"\bincome distribution cum ?capital withdrawal\b",
    # Payout/reinvestment qualifiers, which the API appends in several spellings
    # — "(Payout/Reinvestment)", "(Payout & Reinvestment)", "Payout of IDCW".
    r"\bpayout\b", r"\bre ?investment\b", r"\bdividend\b",
    # IDCW payout frequencies — plan detail, not fund identity.
    r"\bannual\b", r"\bquarterly\b", r"\bmonthly\b", r"\bhalf yearly\b",
    r"\bdaily\b", r"\bweekly\b", r"\bfortnightly\b",
    r"\bg\b", r"\bidcw\b", r"\bregular\b", r"\bdirect\b",
    # "standard" and "bonus" are plan names (Kotak's legacy Standard Plan,
    # Nippon's Bonus Option), not fund identity.
    r"\bgrowth\b", r"\bplan\b", r"\boption\b", r"\bfund\b",
    r"\bstandard\b", r"\bbonus\b", r"\bcumulative\b",
    # Conjunctions stranded by the removals above ("... (Payout & Reinvestment)"
    # leaves a bare "and"; "Payout of IDCW" leaves "of"). Harmless to the match
    # but they make the normalised form confusing to read when debugging.
    r"\band\b", r"\bof\b",
]

# Parenthesised asides that identify a renamed scheme rather than a plan, e.g.
# "ICICI Prudential Large Cap Fund (erstwhile Bluechip Fund)". The NAV data
# carries only the current name, so the aside must go — but as a whole phrase,
# since "bluechip" would otherwise linger and drag the score down.
_ERSTWHILE = re.compile(r"\(\s*erstwhile[^)]*\)", re.I)


def _same_amc(a: str, b: str) -> bool:
    """
    Do two normalised fund names belong to the same AMC?

    Guards the fuzzy branch. The AMC is the leading token(s), so comparing the
    first token catches the dangerous near-misses (hsbc/hdfc, iti/icici) that a
    whole-string ratio smooths over. Multi-word AMCs ("canara robeco", "white
    oak capital") share their first token with themselves, so a first-token
    match is sufficient; where the first token is a generic prefix the second
    token settles it.
    """
    ta, tb = a.split(), b.split()
    if not ta or not tb:
        return False
    if ta[0] != tb[0]:
        return False
    # "360 one" / "bank of india" style: a bare number or a short generic first
    # token is not distinctive on its own, so require the second token too.
    if (ta[0].isdigit() or len(ta[0]) <= 3) and len(ta) > 1 and len(tb) > 1:
        return ta[1] == tb[1]
    return True


def normalize(name: str) -> str:
    """Lowercase, strip punctuation and plan/option noise, expand AMC aliases."""
    s = name.lower().strip()
    # Drop "(erstwhile …)" while the brackets survive — once punctuation is
    # collapsed below there is nothing left to delimit the aside.
    s = _ERSTWHILE.sub(" ", s)
    s = s.replace("&", "and")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # Alias expansion before noise removal — aliases may contain noise words
    # ("the wealth company" -> "the wealth co").
    for long_form in sorted(AMC_ALIASES, key=len, reverse=True):
        if long_form in s:
            s = s.replace(long_form, AMC_ALIASES[long_form])
            break

    for long_form in sorted(PRODUCT_ALIASES, key=len, reverse=True):
        if long_form in s:
            s = s.replace(long_form, PRODUCT_ALIASES[long_form])
            break

    for pat in _NOISE:
        s = re.sub(pat, " ", s)

    # Collapse cap-size spellings so "Mid Cap" == "Midcap". Both sources use
    # both spellings freely, and on a short name the difference is worth ~20
    # points — "Sundaram Flexi Cap" vs "Sundaram Flexicap" scored 80 against a
    # floor of 85, i.e. a fund silently lost to a missing space.
    s = re.sub(r"\blarge\s*cap\b", "largecap", s)
    s = re.sub(r"\bmid\s*cap\b", "midcap", s)
    s = re.sub(r"\bsmall\s*cap\b", "smallcap", s)
    s = re.sub(r"\bflexi\s*cap\b", "flexicap", s)
    s = re.sub(r"\bmulti\s*cap\b", "multicap", s)
    s = re.sub(r"\bmicro\s*cap\b", "microcap", s)

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
            # The AMC token must agree. Fund names differ mostly in their tail
            # ("... Tax Saver Fund"), so a high token_sort_ratio says little
            # about WHOSE fund it is: "HSBC ELSS Tax Saver" scored 89 against
            # "HDFC ELSS Tax Saver" — one character apart, different AMCs, and
            # HSBC's is simply absent from the NAV data. Attributing one AMC's
            # holdings to another is the worst failure this module can produce,
            # so an AMC mismatch is refused outright rather than scored.
            if _same_amc(n, best[0]):
                return {"nav_scheme_name": pool[best[0]],
                        "method": "fuzzy", "score": int(round(best[1]))}
            return {"nav_scheme_name": None, "method": "unresolved",
                    "score": int(round(best[1]))}

        return {"nav_scheme_name": None, "method": "unresolved",
                "score": int(round(best[1])) if best else 0}
