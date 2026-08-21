"""
api_client.py
-------------
Thin HTTP client for the Advisorkhoj mutual-fund API.

Three endpoints are used:

    getAllSchemeCategories              -> every category name
    getAllSchemesCommonNamesbyCategory  -> the funds in one category
    getMutualfundHistoricalPortfolio    -> one fund's holdings for a month

They chain: pick a category, list its funds, then pull each fund's portfolio.

The API key is read from the environment (API_KEY), which `main.py` loads from
the project `.env`. The key is a query parameter, so it appears in every URL —
`_safe_url()` masks it before anything is printed or raised, otherwise a stack
trace would leak the credential into logs.

Every response is JSON of the form:

    {"status": 200, "status_msg": "Success", "msg": "...", <payload key>: [...]}

`status` is in the BODY, not just the HTTP status line — a 200 HTTP response can
still carry a failure status, so `_get()` checks both.
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, List, Optional

BASE_URL = "https://mfapi.advisorkhoj.com"

# The API is a third-party dependency pulled once per month, not a hot path.
# Retries are generous and the pause between funds is deliberate — see fetch_*.
DEFAULT_TIMEOUT = 60
DEFAULT_RETRIES = 3
RETRY_BACKOFF = 2.0

MONTHS = [
    "JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE",
    "JULY", "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER",
]


class AdvisorkhojError(RuntimeError):
    """Any API-level failure: transport, bad status, or malformed payload."""


def get_api_key(explicit: Optional[str] = None) -> str:
    """Return the API key, preferring an explicit value over the environment."""
    key = (explicit or os.environ.get("API_KEY") or "").strip()
    if not key:
        raise AdvisorkhojError(
            "No API key. Set API_KEY in the project .env (or pass --api-key).\n"
            "The enricher pulls holdings from mfapi.advisorkhoj.com, which "
            "requires a key on every request."
        )
    return key


def _safe_url(url: str) -> str:
    """Mask the key so it never reaches a log line or traceback."""
    return re.sub(r"(key=)[^&]*", r"\1***", url)


class AdvisorkhojClient:
    """Client for the three endpoints the enricher needs."""

    def __init__(self, api_key: Optional[str] = None,
                 timeout: int = DEFAULT_TIMEOUT,
                 retries: int = DEFAULT_RETRIES,
                 verbose: bool = True):
        self.api_key = get_api_key(api_key)
        self.timeout = timeout
        self.retries = retries
        self.verbose = verbose

    # ── transport ──────────────────────────────────────────────────────────

    def _get(self, endpoint: str, params: Dict[str, str]) -> Dict:
        """GET one endpoint and return the parsed body, or raise."""
        query = urllib.parse.urlencode({**params, "key": self.api_key})
        url = f"{BASE_URL}/{endpoint}?{query}"

        last_err = None
        for attempt in range(1, self.retries + 1):
            try:
                req = urllib.request.Request(
                    url, headers={"User-Agent": "holdings-enricher/1.0"})
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    body = json.loads(resp.read().decode("utf-8"))
                break
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
                last_err = e
                if attempt == self.retries:
                    raise AdvisorkhojError(
                        f"{endpoint} failed after {self.retries} attempts: {e}\n"
                        f"  {_safe_url(url)}"
                    ) from e
                wait = RETRY_BACKOFF ** attempt
                if self.verbose:
                    print(f"        retry {attempt}/{self.retries - 1} "
                          f"in {wait:.0f}s ({e})")
                time.sleep(wait)

        # The body carries its own status; HTTP 200 does not imply success.
        status = body.get("status")
        if status != 200:
            raise AdvisorkhojError(
                f"{endpoint} returned status {status}: "
                f"{body.get('status_msg') or body.get('msg')}\n"
                f"  {_safe_url(url)}"
            )
        return body

    # ── endpoints ──────────────────────────────────────────────────────────

    def get_categories(self) -> List[str]:
        """Every scheme category, e.g. 'Equity: Large Cap'."""
        return self._get("getAllSchemeCategories", {}).get("list") or []

    def get_schemes_in_category(self, category: str) -> List[str]:
        """
        The common scheme names in one category.

        These are the `scheme_amfi_common` values the portfolio endpoint expects
        — a plan/option-independent name ("Axis Large Cap Fund"), not the full
        NAV scheme name.
        """
        body = self._get("getAllSchemesCommonNamesbyCategory",
                         {"category": category})
        return body.get("list") or []

    def get_portfolio(self, scheme_common_name: str,
                      year: int, month: str) -> List[Dict]:
        """
        One fund's holdings for one month.

        Returns the raw rows; `api_source.to_records()` converts them to the
        enricher's record contract. An empty list means the API had no
        portfolio for that fund/month — common for a month not yet published,
        or a fund that did not exist yet.
        """
        month = month.strip().upper()
        if month not in MONTHS:
            raise AdvisorkhojError(
                f"Invalid month '{month}'. Expected one of: {', '.join(MONTHS)}")

        body = self._get("getMutualfundHistoricalPortfolio", {
            "scheme_amfi_common": scheme_common_name,
            "year": str(year),
            "month": month,
        })
        return body.get("historicalSchemePortfolioList") or []
