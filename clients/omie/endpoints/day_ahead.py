import datetime as dt
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import pytz
from prefect import flow

from clients.omie.client import download_file, list_files
from core import PriceStore, setup_logging  # noqa: E402 (must precede Database import, see core/dev_paths.py)
from Database.db_connect import engine

logger = logging.getLogger(__name__)

price_store = PriceStore(engine)

SOURCE = "OMIE"
PRODUCT = "DAY_AHEAD"
MARKET = "SDAC"
DEFAULT_CURRENCY = "EUR"

REALDIR = "marginalpdbcpt"
DIR_LABEL = " Day-ahead market hourly price in Portugal"
PARENTS = "/Day-ahead Market/1. Prices"

# marginalpdbcpt carries the joint MIBEL auction result: column index 4 (0-based) is
# Spain's clearing price, column 5 is Portugal's - identical whenever the interconnector
# isn't congested, but they diverge during congestion
PRICE_COLUMN_TO_ZONE = {4: "ES", 5: "PT"}

DELIVERY_DAY_TZ = pytz.timezone("Europe/Madrid")

OUTPUT_DIR = Path("output/omie/day_ahead")


def _day_bounds_utc(date: dt.date) -> tuple[dt.datetime, dt.datetime]:
    start = DELIVERY_DAY_TZ.localize(dt.datetime.combine(date, dt.time.min)).astimezone(dt.timezone.utc)
    end = DELIVERY_DAY_TZ.localize(dt.datetime.combine(date + dt.timedelta(days=1), dt.time.min)).astimezone(dt.timezone.utc)
    return start, end


def parse_file(content: bytes, date: dt.date, forecasttime: pd.Timestamp) -> pd.DataFrame:
    """parse one marginalpdbcpt daily file into prod.prices-shaped rows for ES and PT.

    file shape: a "MARGINALPDBCPT;" header line, then one
    "year;month;day;period;price_es;price_pt;" row per delivery period, terminated by a
    bare "*" line - both header and terminator are skipped by the row-shape check below.
    resolution is derived from the actual number of periods in the file against the true
    CET/CEST UTC span of the delivery day
    """
    rows_raw = []
    for line in content.decode("latin-1").splitlines():
        fields = line.strip().rstrip(";").split(";")
        if len(fields) < 6 or not fields[3].isdigit():
            continue
        rows_raw.append(fields)

    if not rows_raw:
        return pd.DataFrame()

    day_start, day_end = _day_bounds_utc(date)
    resolution_minutes = round((day_end - day_start).total_seconds() / 60 / len(rows_raw))

    rows = []
    for fields in rows_raw:
        position = int(fields[3])
        valuetime = pd.Timestamp(day_start) + pd.Timedelta(minutes=(position - 1) * resolution_minutes)
        for column, bidding_zone in PRICE_COLUMN_TO_ZONE.items():
            rows.append(
                {
                    "valuetime": valuetime,
                    "forecasttime": forecasttime,
                    "bidding_zone": bidding_zone,
                    "product": PRODUCT,
                    "market": MARKET,
                    "source": SOURCE,
                    "resolution": resolution_minutes,
                    "currency": DEFAULT_CURRENCY,
                    "price": float(fields[column]),
                }
            )
    return pd.DataFrame(rows)


def fetch_and_parse(from_date: dt.date, to_date: dt.date) -> pd.DataFrame:
    files = list_files(REALDIR, DIR_LABEL, PARENTS)
    if files is None:
        logger.warning("skipping OMIE %s to %s: could not list published files", from_date, to_date)
        return pd.DataFrame()

    dfs = []
    for date in pd.date_range(from_date, to_date, freq="D"):
        date = date.date()
        entry = files.get(date)
        if entry is None:
            logger.warning("skipping OMIE %s: no published file for this date", date)
            continue

        filename, forecasttime = entry
        content = download_file(REALDIR, filename)
        if content is None:
            logger.warning("skipping OMIE %s: failed to download %s", date, filename)
            continue

        try:
            df = parse_file(content, date, forecasttime)
        except (ValueError, IndexError, UnicodeDecodeError):
            logger.error("skipping OMIE %s: failed to parse %s", date, filename, exc_info=True)
            continue
        if not df.empty:
            dfs.append(df)

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def dump(df: pd.DataFrame) -> None:
    """write day-ahead prices to prod.prices via PriceStore."""
    # OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # for bidding_zone, zone_df in df.groupby("bidding_zone"):
    #     zone_df.to_csv(OUTPUT_DIR / f"{bidding_zone}.csv", index=False)

    written = price_store.dump(df)
    logger.info("PriceStore.dump: wrote %d row(s) for OMIE day-ahead", written)


# cron: */15 13-14 * * *  (CET/CEST; SDAC clears ~12:55 CET/CEST, catch-up starts 13:00)
@flow
def run(from_date: Optional[dt.date] = None, to_date: Optional[dt.date] = None) -> pd.DataFrame:
    """fetch OMIE (MIBEL day-ahead auction) prices for ES and PT and dump to prod.prices.

    from_date/to_date optional for historical backfill; defaults to tomorrow only.
    """
    setup_logging()
    tomorrow = dt.date.today() + dt.timedelta(days=1)
    from_date = from_date or tomorrow
    to_date = to_date or tomorrow

    df = fetch_and_parse(from_date=from_date, to_date=to_date)
    if df.empty:
        logger.warning("no OMIE day-ahead data fetched for %s to %s", from_date, to_date)
        return df

    dump(df)
    return df


if __name__ == "__main__":
    run()
