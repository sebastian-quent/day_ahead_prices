import datetime as dt
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import pytz
import xmltodict
from prefect import flow

from clients.opcom.client import fetch as opcom_fetch
from core import PriceStore, setup_logging  # noqa: E402 (must precede Database import, see core/dev_paths.py)
from Database.db_connect import engine

logger = logging.getLogger(__name__)

price_store = PriceStore(engine)

SOURCE = "OPCOM"
PRODUCT = "DAY_AHEAD"
MARKET = "SDAC"
BIDDING_ZONE = "RO"
DEFAULT_CURRENCY = "EUR"  # PriceRO is published in EUR/MWh, no currency field in the response to read instead

OUTPUT_DIR = Path("output/opcom/day_ahead")

# Romania's own civil time is EET/EEST, but - like every other SDAC zone in this repo -
# the PZU delivery day runs midnight-to-midnight CET/CEST, not local time. Live-confirmed
# 2026-07-16: OPCOM's Pos=1 price for delivery date 2026-07-15 (158.34) matched ENTSO-E's
# RO position=1 price for the same date exactly, where ENTSO-E's period start was
# 2026-07-14T22:00Z (CEST midnight) - an EET/EEST boundary would have been one hour
# (4 quarter-hour positions) earlier and produced a different Pos=1 price.
DELIVERY_DAY_TZ = pytz.timezone("Europe/Copenhagen")


def _day_bounds_utc(date: dt.date) -> tuple[dt.datetime, dt.datetime]:
    start = DELIVERY_DAY_TZ.localize(dt.datetime.combine(date, dt.time.min)).astimezone(dt.timezone.utc)
    end = DELIVERY_DAY_TZ.localize(dt.datetime.combine(date + dt.timedelta(days=1), dt.time.min)).astimezone(dt.timezone.utc)
    return start, end


def parse_response(raw: bytes, date: dt.date, forecasttime: pd.Timestamp) -> pd.DataFrame:
    """parse one day's OPCOM PZU market-results XML into prod.prices-shaped rows for RO.

    the response has no per-row timestamp, just a 1-based `Pos` sequence number -
    resolution is derived from the actual number of `Detail` entries against the true
    CET/CEST UTC span of the delivery day (same approach as ENTSO-E/OTE/OMIE), so DST
    transition days (23/25h) and the 2025 hourly->15-min switchover both fall out
    correctly without any special-casing. Prices are comma-thousands-formatted above
    999 on this site (confirmed live on other numeric fields in the same document, e.g.
    "2,701.0"), so commas are stripped before the float conversion even though no live
    PriceRO value above 999 has been observed yet.
    """
    document = xmltodict.parse(raw)
    details = document.get("resultset", {}).get("Detail") or []
    if not isinstance(details, list):
        details = [details]
    if not details:
        return pd.DataFrame()

    day_start, day_end = _day_bounds_utc(date)
    resolution_minutes = round((day_end - day_start).total_seconds() / 60 / len(details))

    rows = []
    for detail in details:
        position = int(detail["Pos"])
        valuetime = pd.Timestamp(day_start) + pd.Timedelta(minutes=(position - 1) * resolution_minutes)
        rows.append(
            {
                "valuetime": valuetime,
                "forecasttime": forecasttime,
                "bidding_zone": BIDDING_ZONE,
                "product": PRODUCT,
                "market": MARKET,
                "source": SOURCE,
                "resolution": resolution_minutes,
                "currency": DEFAULT_CURRENCY,
                "price": float(detail["PriceRO"].replace(",", "")),
            }
        )
    return pd.DataFrame(rows)


def fetch_and_parse(from_date: dt.date, to_date: dt.date) -> pd.DataFrame:
    forecasttime = pd.Timestamp.now(tz="UTC")

    dfs = []
    for date in pd.date_range(from_date, to_date, freq="D"):
        date = date.date()
        raw = opcom_fetch(date)
        if raw is None:
            logger.warning("skipping OPCOM %s: fetch failed", date)
            continue
        try:
            df = parse_response(raw, date, forecasttime)
        except (KeyError, ValueError, TypeError):
            logger.error("skipping OPCOM %s: failed to parse response", date, exc_info=True)
            continue
        if df.empty:
            logger.warning("skipping OPCOM %s: no published report for this date", date)
            continue
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def dump(df: pd.DataFrame) -> None:
    """write day-ahead prices to prod.prices via PriceStore, plus a local CSV per bidding zone for cross-checking."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for bidding_zone, zone_df in df.groupby("bidding_zone"):
        zone_df.to_csv(OUTPUT_DIR / f"{bidding_zone}.csv", index=False)

    written = price_store.dump(df)
    logger.info("PriceStore.dump: wrote %d row(s) for OPCOM day-ahead", written)


@flow
def run(from_date: Optional[dt.date] = None, to_date: Optional[dt.date] = None) -> pd.DataFrame:
    """fetch OPCOM (Romania PZU day-ahead auction) prices and dump to prod.prices.

    from_date/to_date optional for historical backfill; defaults to today+tomorrow.
    """
    setup_logging()
    today = dt.date.today()
    from_date = from_date or today
    to_date = to_date or today + dt.timedelta(days=1)

    df = fetch_and_parse(from_date=from_date, to_date=to_date)
    if df.empty:
        logger.warning("no OPCOM day-ahead data fetched for %s to %s", from_date, to_date)
        return df

    dump(df)
    return df


if __name__ == "__main__":
    run()
