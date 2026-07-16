import datetime as dt
import logging
import re
import time
from typing import Optional

import pandas as pd
import requests
from lxml import html

logger = logging.getLogger(__name__)

HOST = "https://www.omie.es"
LIST_URL = f"{HOST}/en/file-access-list"
DOWNLOAD_URL = f"{HOST}/en/file-download"

RETRY_ATTEMPTS = 2 
RETRY_BACKOFF_SECONDS = 10


def _filename_pattern(realdir: str) -> re.Pattern:
    return re.compile(rf"^{re.escape(realdir)}_(\d{{8}})\.\d+$")


def _get(url: str, params: dict, timeout: int = 30) -> Optional[requests.Response]:
    """GET with one retry on failure, shared by list_files and download_file.

    public, unauthenticated file browser.
    """
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            if attempt < RETRY_ATTEMPTS:
                logger.warning(
                    "OMIE request to %s failed (attempt %d/%d): %s - retrying in %ds",
                    url, attempt, RETRY_ATTEMPTS, exc, RETRY_BACKOFF_SECONDS,
                )
                time.sleep(RETRY_BACKOFF_SECONDS)
            else:
                logger.error("OMIE request to %s failed after %d attempt(s)", url, RETRY_ATTEMPTS, exc_info=True)
                return None
    return None


def list_files(realdir: str, dir_label: str, parents: str) -> Optional[dict[dt.date, tuple[str, pd.Timestamp]]]:
    """list published files for one OMIE file-access directory, keyed by delivery date.

    reused by every endpoint that reads published daily files - shared here
    rather than per-endpoint, same shape as clients/semo/client.py's list_documents.

    OMIE republishes corrected files under an incremented version suffix
    (marginalpdbcpt_20230121.3 superseding .1/.2) - the file-access-list page
    already resolves to just the current version per date, so this returns
    that listing as-is rather than guessing/incrementing version suffixes ourselves.
    """
    response = _get(LIST_URL, params={"parents": parents, "dir": dir_label, "realdir": realdir})
    if response is None:
        return None

    pattern = _filename_pattern(realdir)
    tree = html.fromstring(response.content)
    files: dict[dt.date, tuple[str, pd.Timestamp]] = {}
    for row in tree.xpath("//tr[td]"):
        cells = row.xpath("./td/@data-val")
        if len(cells) < 3:
            continue
        filename, _size, mtime = cells[0], cells[1], cells[2]
        match = pattern.match(filename)
        if match is None:
            continue
        date = dt.datetime.strptime(match.group(1), "%Y%m%d").date()
        files[date] = (filename, pd.Timestamp(int(mtime), unit="s", tz="UTC"))
    return files


def download_file(realdir: str, filename: str, timeout: int = 30) -> Optional[bytes]:
    """download the raw content of one published OMIE file."""
    response = _get(DOWNLOAD_URL, params={"parents": realdir, "filename": filename}, timeout=timeout)
    return response.content if response is not None else None
