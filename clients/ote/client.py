import logging
import time
from functools import lru_cache
from typing import Optional

import requests
import zeep
from requests import Session
from zeep.helpers import serialize_object
from zeep.transports import Transport

logger = logging.getLogger(__name__)

WSDL = "https://www.ote-cr.cz/services/PublicDataService?wsdl"

RETRY_ATTEMPTS = 2  # 1 initial try + 1 retry
RETRY_BACKOFF_SECONDS = 10


@lru_cache(maxsize=1)
def _get_client() -> zeep.Client:
    return zeep.Client(wsdl=WSDL, transport=Transport(session=Session()))


def fetch(operation: str, params: dict) -> Optional[list]:
    """call one SOAP operation against the OTE PublicDataService, shared by all endpoints/*.py modules.

    public, unauthenticated API - no auth to add here. returns the response
    serialized to plain dicts/lists so callers parse the shape relevant to
    them, or None if the source returned nothing or the request failed after
    retrying once.
    """
    method = getattr(_get_client().service, operation)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            return serialize_object(method(**params))
        except (zeep.exceptions.Error, requests.RequestException) as exc:
            if attempt < RETRY_ATTEMPTS:
                logger.warning(
                    "OTE request %s failed for params %s (attempt %d/%d): %s - retrying in %ds",
                    operation, params, attempt, RETRY_ATTEMPTS, exc, RETRY_BACKOFF_SECONDS,
                )
                time.sleep(RETRY_BACKOFF_SECONDS)
            else:
                logger.error("OTE request %s failed for params %s after %d attempt(s)", operation, params, RETRY_ATTEMPTS, exc_info=True)
                return None
