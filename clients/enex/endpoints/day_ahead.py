import datetime as dt
import io
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import pytz
from prefect import flow

from clients.enex.client import download_file, list_files
from core import PriceStore, setup_logging  # noqa: E402 (must precede Database import, see core/dev_paths.py)
from Database.db_connect import engine

logger = logging.getLogger(__name__)

price_store = PriceStore(engine)

SOURCE = "ENEX"
PRODUCT = "DAY_AHEAD"
MARKET = "SDAC"
BIDDING_ZONE = "GR"
# MCP has no currency field in the file - HEnEx's EL-DAM clears in EUR (confirmed live
# 2026-07-16: MCP for every quarter-hour of delivery date 2026-07-16 matches ENTSO-E's
# own GR feed for the same date to the cent, which reports EUR).
DEFAULT_CURRENCY = "EUR"

OUTPUT_DIR = Path("output/enex/day_ahead")

# the Results file's DELIVERY_MTU column looks like Greek local wall-clock (it runs
# 00:00:00 to 23:45:00 for DDAY) but isn't - live cross-check 2026-07-16 against
# ENTSO-E's own GR feed for delivery date 2026-07-16 confirmed DELIVERY_MTU is CET/CEST,
# not Athens' own EET/EEST civil time: all 96 quarter-hours matched exactly on both
# valuetime and price once DELIVERY_MTU was localized as CET/CEST - same "auction time,
# not zone time" rule as AT/EXAA, CZ/OTE, IE/SEMO, RO/OPCOM, ES+PT/OMIE. Rather than
# parse DELIVERY_MTU's wall-clock string directly (ambiguous on a fall-back DST day),
# valuetime is reconstructed from the file's own 1-based SORT position against this true
# UTC day span, same position-based approach as OPCOM/OMIE - sidesteps the ambiguity
# entirely and falls out correctly on DST transition days without special-casing.
DELIVERY_DAY_TZ = pytz.timezone("Europe/Copenhagen")


def _day_bounds_utc(date: dt.date) -> tuple[dt.datetime, dt.datetime]:
    start = DELIVERY_DAY_TZ.localize(dt.datetime.combine(date, dt.time.min)).astimezone(dt.timezone.utc)
    end = DELIVERY_DAY_TZ.localize(dt.datetime.combine(date + dt.timedelta(days=1), dt.time.min)).astimezone(dt.timezone.utc)
    return start, end


def parse_response(raw: bytes, date: dt.date, forecasttime: pd.Timestamp) -> pd.DataFrame:
    """parse one day's EL-DAM_Results xlsx into prod.prices-shaped rows for GR.

    the sheet repeats every delivery period's MCP once per ASSET_DESCR/CLASSIFICATION
    supply-demand breakdown row (exports, imports, load, generation mix, ...) - those
    columns are a volume split, not a price split (confirmed live: MCP is identical
    across all rows sharing a SORT position), so rows are deduped down to one per SORT
    before use. DELIVERY_DURATION is the period length in minutes, read per row rather
    than hardcoded, so an eventual hourly-era backfill or a future resolution change
    falls out correctly without special-casing.
    """
    df = pd.read_excel(io.BytesIO(raw), sheet_name=0, usecols=["SORT", "DELIVERY_DURATION", "MCP"])
    if df.empty:
        return pd.DataFrame()

    periods = df.drop_duplicates(subset="SORT").sort_values("SORT").reset_index(drop=True)
    day_start, _ = _day_bounds_utc(date)
    valuetime = pd.Timestamp(day_start) + pd.to_timedelta(
        (periods["SORT"] - 1) * periods["DELIVERY_DURATION"], unit="m"
    )

    return pd.DataFrame(
        {
            "valuetime": valuetime,
            "forecasttime": forecasttime,
            "bidding_zone": BIDDING_ZONE,
            "product": PRODUCT,
            "market": MARKET,
            "source": SOURCE,
            "resolution": periods["DELIVERY_DURATION"].astype(int),
            "currency": DEFAULT_CURRENCY,
            "price": periods["MCP"].astype(float),
        }
    )


def fetch_and_parse(from_date: dt.date, to_date: dt.date) -> pd.DataFrame:
    files = list_files(oldest_date=from_date)
    if not files:
        logger.warning("skipping ENEX %s to %s: could not list published Results files", from_date, to_date)
        return pd.DataFrame()

    forecasttime = pd.Timestamp.now(tz="UTC")

    dfs = []
    for date in pd.date_range(from_date, to_date, freq="D"):
        date = date.date()
        uuid = files.get(date)
        if uuid is None:
            logger.warning("skipping ENEX %s: no published Results file for this date", date)
            continue

        raw = download_file(uuid)
        if raw is None:
            logger.warning("skipping ENEX %s: failed to download Results file", date)
            continue

        try:
            df = parse_response(raw, date, forecasttime)
        except (KeyError, ValueError, TypeError):
            logger.error("skipping ENEX %s: failed to parse Results file", date, exc_info=True)
            continue
        if df.empty:
            logger.warning("skipping ENEX %s: no MCP rows in Results file", date)
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
    logger.info("PriceStore.dump: wrote %d row(s) for ENEX day-ahead", written)


@flow
def run(from_date: Optional[dt.date] = None, to_date: Optional[dt.date] = None) -> pd.DataFrame:
    """fetch ENEX (Greece EL-DAM day-ahead auction) prices and dump to prod.prices.

    from_date/to_date optional for historical backfill; defaults to today+tomorrow. note:
    the Results listing only exposes cur=N pagination back through MAX_PAGES worth of
    pages (clients/enex/client.py) - fine for today/tomorrow, but a deeper backfill needs
    that constant raised first.
    """
    setup_logging()
    today = dt.date.today()
    from_date = from_date or today
    to_date = to_date or today + dt.timedelta(days=1)

    df = fetch_and_parse(from_date=from_date, to_date=to_date)
    if df.empty:
        logger.warning("no ENEX day-ahead data fetched for %s to %s", from_date, to_date)
        return df

    dump(df)
    return df


if __name__ == "__main__":
    run()
