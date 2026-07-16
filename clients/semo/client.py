import logging
import time
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

HOST = "https://reports.semopx.com"
DOCUMENTS_LIST_URL = f"{HOST}/api/v1/documents/static-reports"
DOCUMENT_DOWNLOAD_BASE = f"{HOST}/documents"

RETRY_ATTEMPTS = 2  # 1 initial try + 1 retry
RETRY_BACKOFF_SECONDS = 10

MAX_PAGES = 50  # safety cap; SEMO's static-reports listing is paginated


def _get(url: str, params: Optional[dict[str, Any]] = None, timeout: int = 30) -> Optional[requests.Response]:
    """GET with one retry on failure, shared by list_documents and download_document.

    public, unauthenticated API - no auth to add here.
    """
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            if attempt < RETRY_ATTEMPTS:
                logger.warning(
                    "SEMO request to %s failed (attempt %d/%d): %s - retrying in %ds",
                    url, attempt, RETRY_ATTEMPTS, exc, RETRY_BACKOFF_SECONDS,
                )
                time.sleep(RETRY_BACKOFF_SECONDS)
            else:
                logger.error("SEMO request to %s failed after %d attempt(s)", url, RETRY_ATTEMPTS, exc_info=True)
                return None
    return None


def list_documents(params: dict[str, Any], *, page_size: int = 1000) -> Optional[list[dict]]:
    """page through SEMO's static-reports document listing for the given filter params.

    reused by every endpoint that reads published report documents (day-ahead today,
    intraday auctions later), not just day-ahead - shared here rather than per-endpoint.
    returns all matching document metadata across pages, or None if any page fails.
    """
    items: list[dict] = []
    page = 1
    while page <= MAX_PAGES:
        response = _get(DOCUMENTS_LIST_URL, params={**params, "page": page, "page_size": page_size})
        if response is None:
            return None

        data = response.json()
        page_items = data.get("items", [])
        if not page_items:
            break
        items.extend(page_items)

        total_pages = data.get("pagination", {}).get("totalPages", 1)
        if page >= total_pages:
            break
        page += 1

    return items


def download_document(resource_name: str, timeout: int = 30) -> Optional[bytes]:
    """download the raw content of a published SEMO report document."""
    response = _get(f"{DOCUMENT_DOWNLOAD_BASE}/{resource_name}", timeout=timeout)
    return response.content if response is not None else None
