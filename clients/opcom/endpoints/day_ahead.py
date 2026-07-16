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
DEFAULT_CURRENCY = "EUR"

OUTPUT_DIR = Path("output/opcom/day_ahead")

DELIVERY_DAY_TZ = pytz.timezone("Europe/Copenhagen")


def _day_bounds_utc(date: dt.date) -> tuple[dt.datetime, dt.datetime]:
    start = DELIVERY_DAY_TZ.localize(dt.datetime.combine(date, dt.time.min)).astimezone(dt.timezone.utc)
    end = DELIVERY_DAY_TZ.localize(dt.datetime.combine(date + dt.timedelta(days=1), dt.time.min)).astimezone(dt.timezone.utc)
    return start, end


def parse_response(raw: bytes, date: dt.date, forecasttime: pd.Timestamp) -> pd.DataFrame:
    """parse one day's OPCOM PZU market-results XML into prod.prices-shaped rows for RO.

    the response has no per-row timestamp, just a 1-based `Pos` sequence number -
    resolution is derived from the actual number of `Detail` entries against the true
    CET/CEST UTC span of the delivery day. Prices are comma-thousands-formatted above
    999 on this site
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
    """write day-ahead prices to prod.prices via PriceStore."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for bidding_zone, zone_df in df.groupby("bidding_zone"):
        zone_df.to_csv(OUTPUT_DIR / f"{bidding_zone}.csv", index=False)

    written = price_store.dump(df)
    logger.info("PriceStore.dump: wrote %d row(s) for OPCOM day-ahead", written)


# cron: */15 13-14 * * *  (CET/CEST; SDAC clears ~12:55 CET/CEST, catch-up starts 13:00)
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
