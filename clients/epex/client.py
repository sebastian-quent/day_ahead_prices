import logging
import socket
import time
from functools import lru_cache
from typing import Optional

import pandas as pd
import paramiko
from quent_core.utils.settings import load_setting

logger = logging.getLogger(__name__)

HOST = "sftp.marketdata.epexspot.com"
PORT = 22

RETRY_ATTEMPTS = 2
RETRY_BACKOFF_SECONDS = 10
CONNECT_TIMEOUT_SECONDS = 30  # paramiko.Transport applies no timeout to the raw TCP connect on its own

_sftp: Optional[paramiko.SFTPClient] = None


@lru_cache(maxsize=1)
def _get_credentials() -> dict:
    return load_setting("epex.sftp_server", resolve_secret=True)


def get_connection() -> paramiko.SFTPClient:
    """shared SFTP connection, reused across calls instead of reconnecting per file"""
    global _sftp
    if _sftp is not None:
        try:
            if _sftp.get_channel().get_transport().is_active():
                return _sftp
        except Exception:
            pass
    credentials = _get_credentials()
    sock = socket.create_connection((HOST, PORT), timeout=CONNECT_TIMEOUT_SECONDS)
    sock.settimeout(None)  # back to blocking for the SSH session itself - only the connect is bounded
    transport = paramiko.Transport(sock)
    transport.auth_timeout = 120
    transport.connect(username=credentials["username"], password=credentials["password"])
    _sftp = paramiko.SFTPClient.from_transport(transport)
    return _sftp


def _with_retry(op, remote_path: str):
    """run op(sftp), retrying once after a fixed backoff on any SFTP failure -
    same one-retry policy as the HTTP clients. resets the cached connection before
    the retry attempt in case it's the connection itself that's gone bad.

    FileNotFoundError is not retried - a missing file (e.g. a resolution that
    doesn't exist for a given year, see fetch_day_ahead_file's fallback) is a
    permanent result, not a transient failure, so retrying it only wastes a
    full backoff sleep for a guaranteed second miss.
    """
    global _sftp
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            return op(get_connection())
        except FileNotFoundError:
            return None
        except (paramiko.SSHException, IOError) as exc:
            if attempt < RETRY_ATTEMPTS:
                logger.warning(
                    "EPEX SFTP request failed for %s (attempt %d/%d): %s - retrying in %ds",
                    remote_path, attempt, RETRY_ATTEMPTS, exc, RETRY_BACKOFF_SECONDS,
                )
                _sftp = None
                time.sleep(RETRY_BACKOFF_SECONDS)
            else:
                logger.error("EPEX SFTP request failed for %s after %d attempt(s)", remote_path, RETRY_ATTEMPTS, exc_info=True)
                return None


def fetch_file(remote_path: str) -> Optional[bytes]:
    """download one file from the EPEX SFTP server, returning its raw bytes."""
    return _with_retry(lambda sftp: sftp.open(remote_path, "rb").read(), remote_path)


def stat_mtime(remote_path: str) -> Optional[pd.Timestamp]:
    """last-modified time of a remote file, used as forecasttime for the data it holds."""
    return _with_retry(lambda sftp: pd.to_datetime(sftp.stat(remote_path).st_mtime, unit="s", utc=True), remote_path)
