import datetime as dt
import logging
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

HOST = "https://www.opcom.ro"
EXPORT_XML_URL = f"{HOST}/rapoarte-pzu-raportMarketResults-export-xml/{{day}}/{{month}}/{{year}}/en"

RETRY_ATTEMPTS = 2  # 1 initial try + 1 retry
RETRY_BACKOFF_SECONDS = 10

# opcom.ro's WAF 403s the literal "python-requests" default User-Agent (confirmed live:
# curl with no UA and with a browser UA both get 200, python-requests' default gets 403) -
# any non-default UA clears it, this isn't a real auth wall.
HEADERS = {"User-Agent": "Mozilla/5.0"}


def fetch(date: dt.date, timeout: int = 30) -> Optional[bytes]:
    """GET one day's PZU (day-ahead) market results XML report - public, unauthenticated.

    returns raw XML bytes for the given delivery date, or None on request failure. a date
    with no published report (not yet published, or older than OPCOM's retained history)
    still comes back HTTP 200 with an empty <resultset/> rather than an error - callers
    treat that the same as "no data" further downstream, not distinguished here.
    """
    url = EXPORT_XML_URL.format(day=date.day, month=date.month, year=date.year)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=timeout)
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            if attempt < RETRY_ATTEMPTS:
                logger.warning(
                    "OPCOM request to %s failed (attempt %d/%d): %s - retrying in %ds",
                    url, attempt, RETRY_ATTEMPTS, exc, RETRY_BACKOFF_SECONDS,
                )
                time.sleep(RETRY_BACKOFF_SECONDS)
            else:
                logger.error("OPCOM request to %s failed after %d attempt(s)", url, RETRY_ATTEMPTS, exc_info=True)
                return None
    return None
