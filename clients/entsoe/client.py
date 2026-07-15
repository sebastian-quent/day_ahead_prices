import ast
import logging
import time
from functools import lru_cache
from typing import Optional

import requests
from quent_core.utils.settings import load_setting

logger = logging.getLogger(__name__)

RETRY_ATTEMPTS = 2  # 1 initial try + 1 retry
RETRY_BACKOFF_SECONDS = 10


@lru_cache(maxsize=1)
def _get_host() -> str:
    return load_setting("entsoe.host", resolve_secret=False)


@lru_cache(maxsize=1)
def _get_api_key() -> str:
    raw_api_keys = load_setting("entsoe.api_key", resolve_secret=True)
    return ast.literal_eval(raw_api_keys)[0]


def fetch(params: dict) -> Optional[bytes]:
    """GET request against the ENTSO-E Transparency Platform API.

    shared by all endpoints/*.py modules - each builds its own params dict
    (documentType, processType, domain, etc.) and passes it here. returns raw
    response bytes so callers can parse the document shape relevant to them.
    retries once after a fixed backoff on any request failure, then returns None
    so callers can skip/continue instead of crashing the run.
    """
    request_params = {"securityToken": _get_api_key(), **params}
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(_get_host(), params=request_params, timeout=30)
            response.raise_for_status()
            return response.content
        except requests.RequestException as exc:
            if attempt < RETRY_ATTEMPTS:
                logger.warning(
                    "ENTSO-E request failed for params %s (attempt %d/%d): %s - retrying in %ds",
                    params, attempt, RETRY_ATTEMPTS, exc, RETRY_BACKOFF_SECONDS,
                )
                time.sleep(RETRY_BACKOFF_SECONDS)
            else:
                logger.error("ENTSO-E request failed for params %s after %d attempt(s)", params, RETRY_ATTEMPTS, exc_info=True)
                return None
