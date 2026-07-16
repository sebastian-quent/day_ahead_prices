import datetime as dt
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import pytz
from prefect import flow

from clients.ote.client import fetch as ote_fetch
from core import PriceStore, setup_logging  # noqa: E402 (must precede Database import, see core/dev_paths.py)
from Database.db_connect import engine

logger = logging.getLogger(__name__)

price_store = PriceStore(engine)

SOURCE = "OTE"
PRODUCT = "DAY_AHEAD"
MARKET = "SDAC"
BIDDING_ZONE = "CZ"
DEFAULT_CURRENCY = "EUR"  # GetDamPricePeriodE has no currency field/InEur param, unlike the legacy GetDamPriceE - CZ's SDAC clearing price is EUR-native so there is nothing else to read

OUTPUT_DIR = Path("output/ote/day_ahead")

# request the finest resolution OTE publishes; the response's own PeriodResolution
# still drives the stored `resolution` value below rather than this constant, in
# case OTE ever answers with something coarser
REQUEST_RESOLUTION = "PT15M"
RESOLUTION_MINUTES = {"PT15M": 15, "PT30M": 30, "PT60M": 60}

# SDAC delivery days run midnight-to-midnight CET/CEST regardless of a zone's own local time
DELIVERY_DAY_TZ = pytz.timezone("Europe/Prague")


def fetch_day_ahead_prices(from_date: dt.date, to_date: dt.date) -> Optional[list]:
    """fetch OTE day-ahead auction prices (GetDamPricePeriodE) for CZ across a whole date range in one call."""
    return ote_fetch(
        "GetDamPricePeriodE",
        {
            "StartDate": from_date.isoformat(),
            "EndDate": to_date.isoformat(),
            "PeriodResolution": REQUEST_RESOLUTION,
        },
    )


def parse_response(items: list, forecasttime: pd.Timestamp) -> pd.DataFrame:
    """parse OTE day-ahead price items into prod.prices-shaped rows.

    valuetime is derived from each item's local delivery date + PeriodIndex,
    with the local-midnight-to-UTC conversion done once per day and all
    further offsets applied in UTC, so spring-forward/fall-back days (which
    OTE already reflects in how many periods it returns for that date) land
    on the correct UTC instants without any DST-specific handling here.
    """
    rows = []
    day_starts_utc = {}
    for item in items:
        resolution_minutes = RESOLUTION_MINUTES.get(item["PeriodResolution"])
        if resolution_minutes is None:
            logger.warning("OTE: unknown PeriodResolution %r, skipping row", item["PeriodResolution"])
            continue

        date = item["Date"]
        day_start_utc = day_starts_utc.get(date)
        if day_start_utc is None:
            day_start_utc = DELIVERY_DAY_TZ.localize(dt.datetime.combine(date, dt.time.min)).astimezone(dt.timezone.utc)
            day_starts_utc[date] = day_start_utc

        valuetime = pd.Timestamp(day_start_utc) + pd.Timedelta(minutes=(item["PeriodIndex"] - 1) * resolution_minutes)
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
                "price": float(item["Price"]),
            }
        )
    return pd.DataFrame(rows)


def fetch_and_parse(from_date: dt.date, to_date: dt.date) -> pd.DataFrame:
    forecasttime = pd.Timestamp.now(tz="UTC")

    items = fetch_day_ahead_prices(from_date, to_date)
    if not items:
        logger.warning("skipping OTE %s to %s: no data returned", from_date, to_date)
        return pd.DataFrame()

    try:
        return parse_response(items, forecasttime)
    except (KeyError, ValueError, TypeError):
        logger.error("skipping OTE %s to %s: failed to parse response", from_date, to_date, exc_info=True)
        return pd.DataFrame()


def dump(df: pd.DataFrame) -> None:
    """write day-ahead prices to prod.prices via PriceStore, plus a local CSV per bidding zone for cross-checking."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for bidding_zone, zone_df in df.groupby("bidding_zone"):
        zone_df.to_csv(OUTPUT_DIR / f"{bidding_zone}.csv", index=False)

    written = price_store.dump(df)
    logger.info("PriceStore.dump: wrote %d row(s) for OTE day-ahead", written)


@flow
def run(from_date: Optional[dt.date] = None, to_date: Optional[dt.date] = None) -> pd.DataFrame:
    """fetch OTE day-ahead prices and dump to prod.prices.

    from_date/to_date optional for historical backfill; defaults to today+tomorrow.
    note: OTE's GetDamPricePeriodE only has data from delivery date 2025-10-01
    (Czech 15-min go-live) onward - earlier dates return no data.
    """
    setup_logging()
    today = dt.date.today()
    from_date = from_date or today
    to_date = to_date or today + dt.timedelta(days=1)

    df = fetch_and_parse(from_date=from_date, to_date=to_date)
    if df.empty:
        logger.warning("no OTE day-ahead data fetched for %s to %s", from_date, to_date)
        return df

    dump(df)
    return df


if __name__ == "__main__":
    run()
