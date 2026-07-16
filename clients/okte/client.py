import logging
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

HOST = "https://isot.okte.sk/api/v1"

RETRY_ATTEMPTS = 2  # 1 initial try + 1 retry
RETRY_BACKOFF_SECONDS = 10


def fetch(path: str, params: dict, timeout: int = 30) -> Optional[list]:
    """GET one OKTE ISOT REST endpoint, shared by all endpoints/*.py modules.

    public, unauthenticated API - no key/header needed (confirmed live with a plain
    default-UA request, unlike OPCOM's WAF). returns the response parsed as JSON, or
    None if the request failed after retrying once. a query with no published data for
    the range still comes back HTTP 200 with an empty list rather than an error.
    """
    url = f"{HOST}/{path}"
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            if attempt < RETRY_ATTEMPTS:
                logger.warning(
                    "OKTE request to %s failed (attempt %d/%d): %s - retrying in %ds",
                    url, attempt, RETRY_ATTEMPTS, exc, RETRY_BACKOFF_SECONDS,
                )
                time.sleep(RETRY_BACKOFF_SECONDS)
            else:
                logger.error("OKTE request to %s failed after %d attempt(s)", url, RETRY_ATTEMPTS, exc_info=True)
                return None
    return None
