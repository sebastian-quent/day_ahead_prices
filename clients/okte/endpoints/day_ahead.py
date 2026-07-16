import datetime as dt
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from prefect import flow

from clients.okte.client import fetch as okte_fetch
from core import PriceStore, setup_logging  # noqa: E402 (must precede Database import, see core/dev_paths.py)
from Database.db_connect import engine

logger = logging.getLogger(__name__)

price_store = PriceStore(engine)

SOURCE = "OKTE"
PRODUCT = "DAY_AHEAD"
MARKET = "SDAC"
BIDDING_ZONE = "SK"
# DAM results' `price` field is documented as EUR/MWh with no currency field to read
# instead, same situation as OTE/OPCOM. Cross-checked live 2026-07-16: SK's period-1
# price for delivery date 2026-07-15 (158.34) matches OPCOM's RO Pos=1 price for the
# same date exactly - SK/CZ/HU/RO clear on the same 4M Market Coupling price, so this
# also confirms the parsing (`priceRo`/`priceHu`/`priceCz` in the response are null in
# every observed response and appear unused - not read here).
DEFAULT_CURRENCY = "EUR"

OUTPUT_DIR = Path("output/okte/day_ahead")


def fetch_day_ahead_prices(from_date: dt.date, to_date: dt.date) -> Optional[list]:
    """fetch OKTE DAM (day-ahead) results for SK across a whole date range in one call."""
    return okte_fetch(
        "dam/results",
        {
            "deliveryDayFrom": from_date.isoformat(),
            "deliveryDayTo": to_date.isoformat(),
        },
    )


def parse_response(items: list, forecasttime: pd.Timestamp) -> pd.DataFrame:
    """parse OKTE DAM results into prod.prices-shaped rows for SK.

    unlike OTE/OPCOM/OMIE, OKTE's `deliveryStart`/`deliveryEnd` are already full UTC
    timestamps in the response, so valuetime and per-row resolution both fall out
    directly from the two fields - no local-time day-boundary math needed here, and
    DST transition days (23/25h) and the hourly-to-15-min switchover both fall out
    correctly without any special-casing.
    """
    rows = []
    for item in items:
        delivery_start = pd.Timestamp(item["deliveryStart"])
        delivery_end = pd.Timestamp(item["deliveryEnd"])
        resolution_minutes = round((delivery_end - delivery_start).total_seconds() / 60)
        rows.append(
            {
                "valuetime": delivery_start,
                "forecasttime": forecasttime,
                "bidding_zone": BIDDING_ZONE,
                "product": PRODUCT,
                "market": MARKET,
                "source": SOURCE,
                "resolution": resolution_minutes,
                "currency": DEFAULT_CURRENCY,
                "price": float(item["price"]),
            }
        )
    return pd.DataFrame(rows)


def fetch_and_parse(from_date: dt.date, to_date: dt.date) -> pd.DataFrame:
    forecasttime = pd.Timestamp.now(tz="UTC")

    items = fetch_day_ahead_prices(from_date, to_date)
    if not items:
        logger.warning("skipping OKTE %s to %s: no data returned", from_date, to_date)
        return pd.DataFrame()

    try:
        return parse_response(items, forecasttime)
    except (KeyError, ValueError, TypeError):
        logger.error("skipping OKTE %s to %s: failed to parse response", from_date, to_date, exc_info=True)
        return pd.DataFrame()


def dump(df: pd.DataFrame) -> None:
    """write day-ahead prices to prod.prices via PriceStore."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for bidding_zone, zone_df in df.groupby("bidding_zone"):
        zone_df.to_csv(OUTPUT_DIR / f"{bidding_zone}.csv", index=False)

    written = price_store.dump(df)
    logger.info("PriceStore.dump: wrote %d row(s) for OKTE day-ahead", written)


@flow
def run(from_date: Optional[dt.date] = None, to_date: Optional[dt.date] = None) -> pd.DataFrame:
    """fetch OKTE (Slovakia DAM day-ahead auction) prices and dump to prod.prices.

    from_date/to_date optional for historical backfill; defaults to today+tomorrow.
    """
    setup_logging()
    today = dt.date.today()
    from_date = from_date or today
    to_date = to_date or today + dt.timedelta(days=1)

    df = fetch_and_parse(from_date=from_date, to_date=to_date)
    if df.empty:
        logger.warning("no OKTE day-ahead data fetched for %s to %s", from_date, to_date)
        return df

    dump(df)
    return df


if __name__ == "__main__":
    run()
