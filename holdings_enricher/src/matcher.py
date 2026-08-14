"""
matcher.py
----------
Resolves a holdings stock name → ISIN using a three-tier strategy:

  1. Manual alias rules   (curated dict — catches abbreviations & truncated names)
  2. Exact normalised match against AMFI company names
  3. Fuzzy token-set ratio ≥ 88 against AMFI names

The ALIASES dict below is the full curated list built during analysis of the
Jan–Jul 2026 holdings files. Extend it as new funds / naming patterns appear.
"""

import re
from typing import Optional, Tuple

from rapidfuzz import fuzz, process

# ── Normaliser (shared with builder) ──────────────────────────────────────
# Strip ONLY true corporate-form suffixes. Words like bank / finance / pharma /
# industries / enterprises / technologies / solutions are part of a company's
# identity, not legal boilerplate: stripping them merged distinct companies onto
# one key ("AXIS Bank ltd" and "AXIS SOLUTIONS ltd" both became "axis", and every
# fund holding Axis Bank was reported as Small Cap). Narrowing this list takes the
# AMFI universe from 22 colliding keys down to 3.
_STRIP_SUFFIX = re.compile(
    r"\b(ltd\.?|limited|pvt\.?|private|inc\.?|corp\.?|corporation|co\.?|llp|llc)\s*$",
    re.IGNORECASE,
)
_STRIP_CHARS = re.compile(r"[.\-&()/,\'`]")
_MULTI_SPACE = re.compile(r"\s+")

# The holdings CSV truncates long names ("Torrent Pharma.", "Apar Inds.",
# "Grasim Inds"). The old normaliser bridged that by DELETING those words, which
# is what collapsed distinct companies onto one key. Instead we EXPAND the
# abbreviation to its full form: identity is preserved, and both sides of the
# match converge on the same string.
_ABBREV = {
    "inds":     "industries",
    "ind":      "industries",
    "pharma":   "pharmaceuticals",
    "chem":     "chemicals",
    "chemi":    "chemicals",
    "fin":      "finance",
    "financ":   "financial",
    "serv":     "services",
    "sol":      "solutions",
    "solut":    "solutions",
    "tech":     "technologies",
    "technol":  "technologies",
    "enterp":   "enterprises",
    "intl":     "international",
    "inter":    "international",
    "corpn":    "corporation",
    "grp":      "group",
    "hold":     "holdings",
    "insur":    "insurance",
    "engg":     "engineering",
    "constr":   "construction",
    "const":    "construction",
    "mfg":      "manufacturing",
    "elec":     "electricals",
    "lab":      "laboratories",
    "labs":     "laboratories",
    "auto":     "automotive",
    "comm":     "communications",
    "prod":     "products",
    "equip":    "equipment",
}

# Indian ISIN: "IN" + 10 alphanumerics (e.g. INE238A01034, INE00H001014)
_ISIN_RE = re.compile(r"^IN[0-9A-Z]{10}$")

# Minimum normalised length (excluding spaces) before fuzzy matching is allowed.
_MIN_FUZZY_CHARS = 5

# Minimum prefix length before a startswith() match is trusted.
_MIN_PREFIX_CHARS = 6


def normalize(name: str) -> str:
    if not name:
        return ""
    n = name.lower()
    n = _STRIP_CHARS.sub(" ", n)
    # Drop legal-form suffixes (ltd / pvt / corp …) — pure boilerplate.
    for _ in range(5):
        prev = n.strip()
        n = _STRIP_SUFFIX.sub("", n).strip()
        if n == prev:
            break
    # Expand truncated identity words so "Apar Inds." and "Apar Industries ltd"
    # converge instead of both being reduced to "apar".
    n = " ".join(_ABBREV.get(tok, tok) for tok in n.split())
    return _MULTI_SPACE.sub(" ", n).strip()


# ── Manual alias rules  ────────────────────────────────────────────────────
# Key   = stock name exactly as it appears in the holdings CSV
# Value = one of:
#           - full AMFI company name  (resolved via normalize() at startup)
#           - a literal ISIN, e.g. "INE238A01034"  (used as-is, no normalising)
#           - None = known not in AMFI, skip gracefully
#
# Use an ISIN when the company name is ambiguous under normalize(). The
# normaliser strips trailing words such as "bank"/"solutions", so "Axis Bank",
# "AXIS Bank ltd" and "AXIS SOLUTIONS ltd" all collapse to "axis" — a name
# target cannot separate them, an ISIN can.
#
# Add new rows here whenever you encounter a new truncated / aliased name.
ALIASES: dict[str, Optional[str]] = {
    # ── ISIN-pinned: ambiguous or too short to resolve by name ──────────────
    # These must never fall through to prefix/fuzzy matching — each one either
    # collides with a differently-sized company or is a bare acronym.
    "Axis Bank":           "INE238A01034",   # else → AXIS SOLUTIONS ltd (Small Cap)
    "M & M":               "INE101A01026",   # else → M.R.F. ltd (Mid Cap) via fuzzy
    "Reliance Industr":    "INE002A01018",   # vs Reliance Industrial Infrastructure
    "Aeroflex":            "INE024001021",   # vs Aeroflex Enterprises / Aeroflex Neu
    "SBI":                 "INE062A01020",   # State Bank of India, not SBI Life/Cards
    "O N G C":             "INE213A01029",   # Oil & Natural Gas Corporation
    "S C I":               "INE109A01011",   # Shipping Corporation of India
    "G N F C":             "INE113A01013",   # Gujarat Narmada Valley Fertilizers
    "NBCC":                "INE095N01031",   # NBCC (India) ltd
    "ICICI AMC":           "INE346A01027",   # ICICI Prudential Asset Management
    "HDFC AMC":            "INE127D01025",   # HDFC Asset Management Company
    "J & K Bank":          "INE168A01041",   # Jammu & Kashmir Bank
    "South Ind.Bank":      "INE683A01023",   # South Indian Bank
    "Nippon Life Ind.":    "INE298J01013",   # Nippon Life India Asset Management
    "Oracle Fin.Serv.":    "INE881D01027",   # Oracle Financial Services Software
    "Poonawalla Fin":      "INE511C01022",   # Poonawalla Fincorp
    "Edelweiss.Fin.":      "INE532F01054",   # Edelweiss Financial Services
    "ICICI Pru Life":      "INE726G01019",   # ICICI Prudential Life Insurance
    "Kaynes Tech":         "INE918Z01012",   # Kaynes Technology India
    "IRB Infra.Devl.":     "INE821I01022",   # IRB Infrastructure Developers
    "Sky Gold & Diam.":    "INE01IU01018",   # Sky Gold And Diamonds
    "21st Cent. Prin.":    None,
    "A B B":               "ABB India ltd",
    "A B Lifestyle":       "Aditya Birla Fashion and Retail ltd",
    "A B Real Estate":     None,
    "A C J K Exports":     None,
    "AAVAS Financiers":    "AAVAS Financiers ltd",
    "Aadhar Hsg. Fin.":    "Aadhar Housing Finance ltd",
    "Action Const.Eq.":    "Action Construction Equipments ltd",
    "Aditya Bir. Fas.":    "Aditya Birla Fashion and Retail ltd",
    "Advit Jewels":        None,
    "Aegis Vopak Term":    "AEGIS VOPAK TERMINALS ltd",
    "Afcons Infrastr.":    "Afcons Infrastructure ltd",
    "Ahluwalia Contr.":    "Ahluwalia Contracts (India) ltd",
    "Alicon Cast.":        "Alicon Castalloy ltd",
    "Alkem Lab":           "Alkem Laboratories ltd",
    "Allcargo Global":     "Allcargo Logistics ltd",
    "Amara Raja Ener.":    "Amara Raja Energy & Mobility ltd",
    "Amrutanjan Healt":    "Amrutanjan Health Care ltd",
    "Apeejay Surrend.":    "Apeejay Surrendra Park Hotels ltd",
    "Aster DM Quality":    "Aster DM Healthcare ltd",
    "Astrazeneca Phar":    "AstraZeneca Pharma India ltd",
    "Avenue Super.":       "Avenue Supermarts ltd",
    "AWL Agri Busine.":    "AWL AGRI BUSINESS ltd",
    "B H E L":             "Bharat Heavy Electricals ltd",
    "B P C L":             "Bharat Petroleum Corpn. ltd",
    "Bank of Maha":        "Bank of maharashtra",
    "Bayer Crop Sci.":     "Bayer CropScience ltd.",
    "Birla Corpn.":        "Birla Corporation ltd",
    "Bluestone Jewel":     "BlueStone Jewellery and Lifestyle ltd",
    "Butterfly Gan Ap":    "Butterfly Gandhimathi Appliances ltd",
    "C D S L":             "Central Depository Services (India) ltd",
    "Caliber Mining":      None,
    "Cams Services":       "Computer Age Management Services ltd",
    "Caplin Point Lab":    "Caplin Point Laboratories ltd",
    "Carborundum Uni.":    "Carborundum Universal ltd",
    "Central Mine Pla":    "Central Mine Planning & Design Institute ltd",
    "Century Plyboard":    "Century Plyboards (India) ltd",
    "Cera Sanitary.":      "Cera Sanitaryware ltd",
    "Chambal Fert.":       "Chambal Fertilisers & Chemicals ltd",
    "Chola Financial":     "CHOLAMANDALAM FINANCIAL HOLDINGS ltd",
    "Cholaman.Inv.&Fn":    "Cholamandalam Investment and Finance Company ltd",
    "Cohance Life":        "Cohance Lifesciences ltd",
    "Colgate-Palmoliv":    "Colgate-Palmolive (India) ltd",
    "Container Corpn.":    "Container Corporation Of India ltd",
    "Craftsman Auto":      "Craftsman Automation ltd",
    "CreditAcc. Gram.":    "CreditAccess Grameen ltd",
    "Crompton Gr. Con":    "Crompton Greaves Consumer Electricals ltd",
    "Data Pattern":        "Data Patterns (India) ltd",
    "Deepak Fertilis.":    "Deepak Fertilizers &Petrochemicals",
    "Del Dot Systems":     None,
    "Divgi Torq":          "DIVGI TORQTRANSFER SYSTEMS ltd",
    "Divis Lab.":          "Divi's Laboratories ltd",
    "Dixon Technolog.":    "Dixon Technologies (India) ltd",
    "Dr Agarwals Hea":     "Dr.Agarwals Eye Hospital ltd",
    "Dr Reddys Labs":      "Dr. Reddy's Laboratories ltd",
    "EID Parry":           "E.I.D. Parry (India) ltd",
    "EMA Partners":        None,
    "EPack PrefabTech":    "Epack Prefab Technologies ltd",
    "Elecon Engg.Co":      "Elecon Engineering Co.ltd",
    "Ellen.Indl.Gas":      "ELLENBARRIE INDUSTRIAL GASES ltd",
    "Embassy Develop":     "EMBASSY DEVELOPMENTS ltd",
    "Emmvee Photovol.":    "Emmvee Photovoltaic Power ltd",
    "Eveready Inds.":      "Eveready Industries India ltd",
    "Finbud Financial":    None,
    "Firstsour.Solu.":     "Firstsource Solutions ltd",
    "Five-Star Bus.Fi":    "Five-Star Business Finance ltd",
    "Fortis Health.":      "Fortis Healthcare ltd",
    "G S F C":             "Gujarat State Fertilizers & Chem.ltd",
    "GE Shipping Co":      "Great Eastern Shipping Co. ltd",
    "GNA Axles":           None,
    "GSPL India Trans":    None,
    "GSPL Transmissio":    "Gujarat State Petronet ltd",
    "Galaxy Surfact.":     "Galaxy Surfactants ltd",
    "Gateway Distri":      "Gateway Distriparks ltd",
    "General Insuranc":    "General Insurance Corporation of India",
    "Glaxosmi. Pharma":    "GlaxoSmithkline Pharmaceuticals ltd",
    "Globsyn Techno":      None,
    "Godavari Bioref.":    "Godavari Biorefineries ltd",
    "Guj. Ambuja Exp":     "Gujarat Ambuja Exports ltd",
    "Gujarat Energy":      None,
    "Gujarat Fluoroch":    "Gujarat Fluorochemicals ltd",
    "Gulf Oil Lubric.":    "Gulf Oil Lubricants India ltd",
    "H P C L":             "Hindustan Petroleum Corporation ltd",
    "H U D C O":           "Housing &Urban Development Corporation ltd",
    "HDB FINANC SER":      "HDB FINANCIAL SERVICES ltd",
    "Harsha Engg Intl":    "Harsha Engineers International ltd",
    "Heidelberg Cem.":     "Nuvoco Vistas Corporation ltd",
    "Hexagon Nutri.":      "Hexagon Nutrition ltd",
    "Hind. Unilever":      "Hindustan Unilever ltd",
    "Hind.Aeronautics":    "Hindustan Aeronautics ltd",
    "Hind.Dorr-Oliver":    None,
    "Home First Finan":    "Home First Finance Company India ltd",
    "Honeywell Auto":      "Honeywell Automation India ltd",
    "I O C L":             "Indian Oil Corporation ltd",
    "I R F C":             "Indian Railway Finance Corporation",
    "INDIA SHELTE FIN":    "India Shelter Finance Corporation ltd",
    "Indo-MIM":            None,
    "Inox Renewable":      "INOX GREEN ENERGY SERVICES ltd",
    "Ipca Labs.":          "Ipca Laboratories ltd",
    "Jindal Stain.":       "Jindal Stainless ltd",
    "Jubilant Food.":      "Jubilant Foodworks ltd",
    "Juniper Green":       None,
    "Jupiter Life Lin":    "Jupiter Life Line Hospitals ltd",
    "KNR Construct.":      "KNR Constructions ltd",
    "KRN Heat Exchan":     "KRN HEAT EXCHANGER AND REFRIGERATION ltd",
    "Kewal Kir.Cloth.":    "Kewal Kiran Clothing ltd",
    "Kilburn Engg.":       "Kilburn Engineering ltd",
    "Kirl. Brothers":      "Kirloskar Brothers ltd",
    "Kirl. Ferrous":       "Kirloskar Ferrous Industries ltd",
    "Kirl.Pneumatic":      "Kirloskar Pneumatic Co.ltd",
    "Knack Packaging":     None,
    "Kolte Patil Dev.":    "Kolte-Patil Developers ltd",
    "Kotak Mah. Bank":     "Kotak Mahindra Bank ltd",
    "Krishna Institu.":    "Krishna Institute of Medical Sciences ltd",
    "Kusumgar":            None,
    "L G Balakrishnan":    None,
    "Laser Power":         None,
    "Lohia Corp":          None,
    "MAS FINANC SER":      "MAS Financial Services ltd",
    "MRF":                 None,
    "Mah. Seamless":       "Maharashtra Seamless ltd",
    "Manipal Health":      None,
    "Manpasand Bever.":    None,
    "Medi Assist Ser.":    "Medi Assist Healthcare Services ltd",
    "Metropolis Healt":    "Metropolis Healthcare ltd",
    "Motil.Oswal.Fin.":    "Motilal Oswal Financial Services ltd",
    "Multi Comm. Exc.":    "Multi Commodity Exchange of India ltd",
    "N S D L":             "National Securities Depository ltd",
    "Natl. Aluminium":     "National Aluminium Co. ltd",
    "Navin Fluo.Intl.":    "Navin Fluorine International ltd",
    "Netweb Technol.":     "NETWEB TECHNOLOGIES INDIA ltd",
    "Neuland Labs.":       "Neuland Laboratories ltd.",
    "Omnitech Engg.":      "Omnitech Engineering ltd",
    "P & G Health Ltd":    "PROCTER & GAMBLE HEALTH ltd",
    "P & G Hygiene":       "Procter & Gamble Hygiene & Health Care ltd",
    "P N Gadgil Jewe.":    None,
    "PNGS Reva Diamo.":    "PNGS Reva Diamond Jewellery ltd",
    "Pitti Engg.":         "PITTI ENGINEERING ltd",
    "Polyplex Corpn":      "Polyplex Corporation ltd",
    "Power Fin.Corpn.":    "Power Finance Corporation ltd",
    "Power Grid Corpn":    "POWER GRID CORPORATION OF INDIA ltd",
    "Privi Speci.":        "PRIVI SPECIALITY CHEMICALS ltd",
    "Punjab Natl.Bank":    "Punjab National Bank",
    "Q-Line Biotech":      None,
    "Rainbow Child.":      "Rainbow Children's Medicare ltd",
    "Ratnaveer Precis":    "Ratnaveer Precision Engineering ltd",
    "Restaurant Brand":    "Restaurant Brands Asia ltd",
    "S A I L":             "Steel Authority of India ltd",
    "SBI Funds Mgt.":      None,
    "SBI Life Insuran":    "SBI Life Insurance Company ltd",
    "SJS Enterprises":     None,
    "SPR Auto Technol":    None,
    "Safari Inds.":        "Safari Industries (India) ltd",
    "Samvardh. Mothe.":    "Samvardhana Motherson International ltd",
    "Schneider Elect.":    "SCHNEIDER ELECTRIC INFRASTRUCTURE ltd",
    "Shaily Engineer.":    "Shaily Engineering Plastics ltd",
    "Shreeji Ship. Gl":    "Shreeji Shipping Global ltd",
    "Shubh Shanti Ser":    None,
    "Smartworks Cowor":    "SMARTWORKS COWORKING SPACES ltd",
    "Sona BLW Precis.":    "Sona BLW Precision Forgings ltd",
    "SPR Auto Technol":    None,
    "Star Health Insu":    "Star Health and Allied Insurance Company ltd",
    "Styrenix Perfor.":    "Styrenix Performance Materials ltd",
    "Sumitomo Chemi.":     "SUMITOMO CHEMICAL INDIA ltd",
    "Suprajit Engg.":      "Suprajit Engineering ltd",
    "TCS":                 "Tata Consultancy Services ltd",
    "Team Lease Serv.":    "TeamLease Services ltd",
    "Techno Elec.Engg":    "Techno Electric & Engineering Company ltd",
    "Thangamayil Jew.":    "Thangamayil Jewellery ltd",
    "The Bombay Burma":    "The Bombay Burmah Trading Corporation ltd",
    "Triven.Engg.Ind.":    "Triveni Engineering & Industries ltd",
    "UTI AMC":             "UTI Asset Management Company ltd",
    "Unichem Labs.":       "Unichem Laboratories ltd",
    "Vijaya Diagnost.":    "Vijaya Diagnostic Centre ltd",
    "Volt.Transform.":     "VOLTAMP TRANSFORMERS ltd",
    "Waterways Leisur":    None,
    "Westlife Food":       "WESTLIFE FOODWORLD ltd",
    "Yatharth Hospit.":    "YATHARTH HOSPITAL & TRAUMA CARE SERVICES ltd",
    "Zydus Lifesci.":      "Zydus Lifesciences ltd",
}

# Match-method labels
METHOD_EXACT     = "exact-norm"
METHOD_ALIAS     = "manual-alias"
METHOD_PREFIX    = "prefix"        # CSV name is a unique prefix of an AMFI name
METHOD_FUZZY     = "fuzzy-high"
METHOD_NONE      = "manual-none"   # in alias dict but mapped to None
METHOD_AMBIGUOUS = "ambiguous"     # prefix matched >1 company — needs an alias
METHOD_NOMATCH   = "no-match"


class Matcher:
    """
    Resolves a holdings stock name to an ISIN.

    Parameters
    ----------
    amfi_by_norm : dict  {normalized_amfi_name -> isin}
    fuzzy_cutoff : int   minimum score for fuzzy matching (default 88)
    """

    def __init__(self, amfi_by_norm: dict, fuzzy_cutoff: int = 88,
                 known_isins: Optional[set] = None):
        self._norm_to_isin  = amfi_by_norm          # normalized name → ISIN
        self._norm_list     = list(amfi_by_norm)    # for rapidfuzz
        self._fuzzy_cutoff  = fuzzy_cutoff

        valid_isins = known_isins if known_isins is not None else set(amfi_by_norm.values())

        # Pre-resolve alias targets → ISIN (at construction time, once)
        self._alias_isin: dict[str, Optional[str]] = {}
        for stock, target in ALIASES.items():
            if target is None:
                self._alias_isin[stock] = None
            elif _ISIN_RE.match(target):
                # Literal ISIN — use directly, bypassing normalize() entirely.
                if target in valid_isins:
                    self._alias_isin[stock] = target
                else:
                    raise ValueError(
                        f"ALIASES[{stock!r}] pins ISIN {target!r}, which is absent "
                        f"from the AMFI mapping. Rebuild the mapping or fix the alias."
                    )
            else:
                isin = amfi_by_norm.get(normalize(target))
                if isin:
                    self._alias_isin[stock] = isin
                else:
                    # Target name not found in AMFI — treat as None
                    self._alias_isin[stock] = None

    def match(self, stock_name: str) -> Tuple[Optional[str], str, float]:
        """
        Returns (isin_or_None, method, score).
        """
        # 1. Manual alias
        if stock_name in self._alias_isin:
            isin = self._alias_isin[stock_name]
            if isin:
                return isin, METHOD_ALIAS, 100.0
            return None, METHOD_NONE, 0.0

        # 2. Exact normalised
        norm = normalize(stock_name)
        if norm in self._norm_to_isin:
            return self._norm_to_isin[norm], METHOD_EXACT, 100.0

        # Very short keys carry too little signal to match safely — "M & M"
        # normalises to "m m" and scored 90+ against "m r f", and "SBI" scores
        # 100 against "SBI Life Insurance". Anything this short must be resolved
        # by an explicit alias instead.
        if len(norm.replace(" ", "")) < _MIN_FUZZY_CHARS:
            return None, METHOD_NOMATCH, 0.0

        # 3. Prefix match — the CSV truncates long names to a fixed width
        #    ("JSW Infrast", "Aptus Value Hou."), so the holdings name is
        #    usually a literal prefix of the AMFI name. This is far safer than
        #    fuzzy scoring: it anchors at the start, so it cannot jump to an
        #    unrelated company that merely shares tokens. Ambiguity (more than
        #    one AMFI name sharing the prefix) is rejected rather than guessed.
        if len(norm) >= _MIN_PREFIX_CHARS:
            hits = [k for k in self._norm_list if k.startswith(norm)]
            if len(hits) == 1:
                return self._norm_to_isin[hits[0]], METHOD_PREFIX, 100.0
            if len(hits) > 1:
                # Prefer an exact token-boundary continuation if only one exists
                exact_word = [k for k in hits
                              if k == norm or k[len(norm):].startswith(" ")]
                if len(exact_word) == 1:
                    return self._norm_to_isin[exact_word[0]], METHOD_PREFIX, 100.0
                return None, METHOD_AMBIGUOUS, 0.0

        # 4. Fuzzy token-set ratio (last resort)
        result = process.extractOne(
            norm,
            self._norm_list,
            scorer=fuzz.token_set_ratio,
            score_cutoff=self._fuzzy_cutoff,
        )
        if result:
            matched_norm, score, _ = result
            return self._norm_to_isin[matched_norm], METHOD_FUZZY, round(score, 1)

        return None, METHOD_NOMATCH, 0.0
