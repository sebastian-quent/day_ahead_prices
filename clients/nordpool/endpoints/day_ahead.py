import datetime as dt
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from prefect import flow

from clients.nordpool.client import fetch as nordpool_fetch
from clients.nordpool.config import BIDDING_ZONE_TO_NORDPOOL_AREA
from core import PriceStore, setup_logging  # noqa: E402 (must precede Database import, see core/dev_paths.py)
from Database.db_connect import engine

logger = logging.getLogger(__name__)

price_store = PriceStore(engine)

SOURCE = "Nord Pool"
PRODUCT = "DAY_AHEAD"
MARKET = "SDAC"
CURRENCY = "EUR"

OUTPUT_DIR = Path("output/nordpool/day_ahead")

NORDPOOL_AREA_TO_BIDDING_ZONE = {v: k for k, v in BIDDING_ZONE_TO_NORDPOOL_AREA.items()}


def fetch_day_ahead_prices(date: dt.date, delivery_areas: list, currency: str = "EUR") -> Optional[dict]:
    """fetch one day of day-ahead auction prices for the given Nord Pool delivery areas."""
    params = {
        "market": "DayAhead",
        "date": date.strftime("%Y-%m-%d"),
        "deliveryArea": ",".join(delivery_areas),
        "currency": currency,
    }
    return nordpool_fetch("DayAheadPrices", params)


def parse_response(raw: dict, forecasttime: pd.Timestamp) -> pd.DataFrame:
    """parse one day's Nord Pool DayAheadPrices response into prod.prices-shaped rows."""
    rows = []
    for entry in raw.get("multiAreaEntries", []):
        delivery_start = pd.Timestamp(entry["deliveryStart"]).tz_convert("UTC")
        delivery_end = pd.Timestamp(entry["deliveryEnd"]).tz_convert("UTC")
        resolution_minutes = round((delivery_end - delivery_start).total_seconds() / 60)

        for nordpool_area, price in entry.get("entryPerArea", {}).items():
            bidding_zone = NORDPOOL_AREA_TO_BIDDING_ZONE.get(nordpool_area)
            if bidding_zone is None:
                continue  # area not in our tracked bidding zones (e.g. legacy SE, non-EU areas)
            rows.append(
                {
                    "valuetime": delivery_start,
                    "forecasttime": forecasttime,
                    "bidding_zone": bidding_zone,
                    "product": PRODUCT,
                    "market": MARKET,
                    "source": SOURCE,
                    "resolution": resolution_minutes,
                    "currency": CURRENCY,
                    "price": price,
                }
            )
    return pd.DataFrame(rows)


def fetch_and_parse(from_date: dt.date, to_date: dt.date) -> pd.DataFrame:
    forecasttime = pd.Timestamp.now(tz="UTC")
    delivery_areas = list(BIDDING_ZONE_TO_NORDPOOL_AREA.values())

    frames = []
    for date in pd.date_range(from_date, to_date, freq="D"):
        raw = fetch_day_ahead_prices(date=date.date(), delivery_areas=delivery_areas)
        if raw is None:
            logger.warning("skipping %s: Nord Pool fetch failed", date.date())
            continue
        try:
            frames.append(parse_response(raw, forecasttime=forecasttime))
        except (KeyError, ValueError):
            logger.error("skipping %s: failed to parse Nord Pool response", date.date(), exc_info=True)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def dump(df: pd.DataFrame) -> None:
    """write day-ahead prices to prod.prices via PriceStore."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for bidding_zone, zone_df in df.groupby("bidding_zone"):
        zone_df.to_csv(OUTPUT_DIR / f"{bidding_zone}.csv", index=False)

    written = price_store.dump(df)
    logger.info("PriceStore.dump: wrote %d row(s) for Nord Pool day-ahead", written)


# cron: */15 13-14 * * *  (CET/CEST; SDAC clears ~12:55 CET/CEST, catch-up starts 13:00)
@flow
def run(from_date: Optional[dt.date] = None, to_date: Optional[dt.date] = None) -> pd.DataFrame:
    """fetch Nord Pool day-ahead prices and dump to prod.prices.

    from_date/to_date optional for historical backfill; defaults to tomorrow only.
    """
    setup_logging()
    tomorrow = dt.date.today() + dt.timedelta(days=1)
    from_date = from_date or tomorrow
    to_date = to_date or tomorrow

    df = fetch_and_parse(from_date=from_date, to_date=to_date)
    if df.empty:
        logger.warning("no Nord Pool day-ahead data fetched for %s to %s", from_date, to_date)
        return df

    dump(df)
    return df


if __name__ == "__main__":
    run()
