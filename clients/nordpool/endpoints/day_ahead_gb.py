import datetime as dt
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from prefect import flow

from clients.nordpool.client import fetch as nordpool_fetch
from core import PriceStore, setup_logging  # noqa: E402 (must precede Database import, see core/dev_paths.py)
from Database.db_connect import engine

logger = logging.getLogger(__name__)

price_store = PriceStore(engine)

SOURCE = "Nord Pool"
PRODUCT = "DAY_AHEAD"
BIDDING_ZONE = "GB"
NORDPOOL_AREA = "UK"

# GB runs two separate, currently-live day-ahead auctions on Nord Pool - not a
# single SDAC-style market like the other zones in day_ahead.py - so both are
# landed as distinct `market` rows for the same bidding_zone (same pattern as
# AT's SDAC + EXAA_EARLY).
MARKETS = ["N2EX", "HalfHourly"]

OUTPUT_DIR = Path("output/nordpool/day_ahead_gb")


def fetch_day_ahead_prices(date: dt.date, market: str) -> Optional[dict]:
    """fetch one day of GB day-ahead auction prices for the given Nord Pool market."""
    params = {
        "market": market,
        "date": date.strftime("%Y-%m-%d"),
        "deliveryArea": NORDPOOL_AREA,
        "currency": "GBP",
    }
    return nordpool_fetch("DayAheadPrices", params)


def parse_response(raw: dict, forecasttime: pd.Timestamp, market: str) -> pd.DataFrame:
    """parse one day's Nord Pool GB DayAheadPrices response into prod.prices-shaped rows."""
    currency = raw.get("currency")
    rows = []
    for entry in raw.get("multiAreaEntries", []):
        delivery_start = pd.Timestamp(entry["deliveryStart"]).tz_convert("UTC")
        delivery_end = pd.Timestamp(entry["deliveryEnd"]).tz_convert("UTC")
        resolution_minutes = round((delivery_end - delivery_start).total_seconds() / 60)

        price = entry.get("entryPerArea", {}).get(NORDPOOL_AREA)
        if price is None:
            continue
        rows.append(
            {
                "valuetime": delivery_start,
                "forecasttime": forecasttime,
                "bidding_zone": BIDDING_ZONE,
                "product": PRODUCT,
                "market": market,
                "source": SOURCE,
                "resolution": resolution_minutes,
                "currency": currency,
                "price": price,
            }
        )
    return pd.DataFrame(rows)


def fetch_and_parse(from_date: dt.date, to_date: dt.date) -> pd.DataFrame:
    forecasttime = pd.Timestamp.now(tz="UTC")

    frames = []
    for date in pd.date_range(from_date, to_date, freq="D"):
        for market in MARKETS:
            raw = fetch_day_ahead_prices(date=date.date(), market=market)
            if raw is None:
                logger.warning("skipping %s (%s): Nord Pool GB fetch failed", date.date(), market)
                continue
            try:
                frames.append(parse_response(raw, forecasttime=forecasttime, market=market))
            except (KeyError, ValueError):
                logger.error("skipping %s (%s): failed to parse Nord Pool GB response", date.date(), market, exc_info=True)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def dump(df: pd.DataFrame) -> None:
    """write GB day-ahead prices to prod.prices via PriceStore."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for market, market_df in df.groupby("market"):
        market_df.to_csv(OUTPUT_DIR / f"{market}.csv", index=False)

    written = price_store.dump(df)
    logger.info("PriceStore.dump: wrote %d row(s) for Nord Pool GB day-ahead", written)


@flow
def run(from_date: Optional[dt.date] = None, to_date: Optional[dt.date] = None) -> pd.DataFrame:
    """fetch Nord Pool GB day-ahead prices and dump to prod.prices.

    from_date/to_date optional for historical backfill; defaults to today+tomorrow.
    """
    setup_logging()
    today = dt.date.today()
    from_date = from_date or today
    to_date = to_date or today + dt.timedelta(days=1)

    df = fetch_and_parse(from_date=from_date, to_date=to_date)
    if df.empty:
        logger.warning("no Nord Pool GB day-ahead data fetched for %s to %s", from_date, to_date)
        return df

    dump(df)
    return df


if __name__ == "__main__":
    run()
