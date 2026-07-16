import datetime as dt
import logging
import re
import time
from typing import Optional

import requests
from lxml import html

logger = logging.getLogger(__name__)

HOST = "https://www.enexgroup.gr"
LISTING_URL = f"{HOST}/web/guest/markets-publications-el-day-ahead-market"
DOWNLOAD_URL = f"{HOST}/c/document_library/get_file"

# the EL-DAM market-results listing page bundles several separate file series (NDPS,
# Results, AggrCurves, POSNOMs, ...) as distinct Liferay Asset Publisher portlet
# instances, each with its own fixed instance ID. This one is "Results" (the DAM auction
# outcome - MCP etc., what we want) - identified by matching the uuid in the
# user-supplied example file link against this portlet's listing.
PORTLET_INSTANCE = "com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_6eBaUXF5VIb7"
ENTRIES_PER_PAGE = 7  # the portlet ignores a `_delta=` override in the URL (confirmed live), always 7/page
MAX_PAGES = 6  # ~6 weeks back - covers today/tomorrow with headroom, bounds the walk if an old date is requested

RETRY_ATTEMPTS = 2
RETRY_BACKOFF_SECONDS = 10

_TITLE_PATTERN = re.compile(r"(\d{8})_EL-DAM_Results_EN_v\d+")
_UUID_PATTERN = re.compile(r"uuid=([a-f0-9-]+)")


def _get(url: str, params: Optional[dict] = None, timeout: int = 30) -> Optional[requests.Response]:
    """GET with one retry on failure, shared by list_files and download_file.

    public, unauthenticated Liferay site - no auth/UA workaround needed
    """
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            if attempt < RETRY_ATTEMPTS:
                logger.warning(
                    "ENEX request to %s failed (attempt %d/%d): %s - retrying in %ds",
                    url, attempt, RETRY_ATTEMPTS, exc, RETRY_BACKOFF_SECONDS,
                )
                time.sleep(RETRY_BACKOFF_SECONDS)
            else:
                logger.error("ENEX request to %s failed after %d attempt(s)", url, RETRY_ATTEMPTS, exc_info=True)
                return None
    return None


def list_files(oldest_date: dt.date) -> Optional[dict[dt.date, str]]:
    """list published EL-DAM_Results documents, keyed by delivery date, mapped to their document uuid.

    walks the Results listing's pages (newest first, ENTRIES_PER_PAGE per page) via the
    `_cur=N` param until oldest_date is covered or MAX_PAGES is hit. Returns whatever was
    collected so far (possibly None) if a page request fails outright.
    """
    files: dict[dt.date, str] = {}
    for page in range(1, MAX_PAGES + 1):
        response = _get(
            LISTING_URL,
            params={
                "p_p_id": PORTLET_INSTANCE,
                "p_p_lifecycle": 0,
                "p_p_state": "normal",
                "p_p_mode": "view",
                f"_{PORTLET_INSTANCE}_cur": page,
            },
        )
        if response is None:
            return files or None

        tree = html.fromstring(response.content)
        found_this_page = False
        for anchor in tree.xpath("//span[@class='asset-title']/a"):
            title_match = _TITLE_PATTERN.search(anchor.text_content())
            uuid_match = _UUID_PATTERN.search(anchor.get("href", ""))
            if not title_match or not uuid_match:
                continue
            files[dt.datetime.strptime(title_match.group(1), "%Y%m%d").date()] = uuid_match.group(1)
            found_this_page = True

        if not found_this_page or min(files) <= oldest_date:
            break

    return files


def download_file(uuid: str, timeout: int = 30) -> Optional[bytes]:
    """download the raw xlsx content of one published EL-DAM_Results document."""
    response = _get(DOWNLOAD_URL, params={"uuid": uuid, "groupId": "20126"}, timeout=timeout)
    return response.content if response is not None else None
