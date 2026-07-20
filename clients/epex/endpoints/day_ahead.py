import datetime as dt
import io
import logging
from functools import partial
from pathlib import Path
from typing import NamedTuple, Optional

import pandas as pd
import pytz
from prefect import flow

import clients.epex.client as epex_client
from core import PriceStore, setup_logging  # noqa: E402 (must precede Database import, see core/dev_paths.py)
from Database.db_connect import engine

logger = logging.getLogger(__name__)

price_store = PriceStore(engine)

SOURCE = "EPEX"
PRODUCT = "DAY_AHEAD"
DEFAULT_CURRENCY = "EUR"

OUTPUT_DIR = Path("output/epex/day_ahead")

DELIVERY_DAY_TZ = pytz.timezone("Europe/Copenhagen")


class ZoneFile(NamedTuple):
    folder: str  # SFTP top-level folder, e.g. "austria"
    filename_slug: str  # filename segment, e.g. "germany_luxembourg" for DE
    resolution_minutes: int
    market: str = "SDAC"  # GB isn't in SDAC - its two EPEX auctions need their own codes


# per-zone SFTP file layout.
# path is derived by _remote_path(): resolution decides the period folder and filename
# convention (EPEX isn't consistent across resolutions - 15-min uses an "_15" filename
# infix, GB's half-hourly uses an "hh_" prefix instead), year decides Current vs Historical.
ZONE_FILE_CONFIG = {
    "AT": [ZoneFile("austria", "austria", 15)],
    "BE": [ZoneFile("belgium", "belgium", 15)],
    "DK1": [ZoneFile("denmark 1", "denmark 1", 15)],
    "DK2": [ZoneFile("denmark 2", "denmark 2", 15)],
    "FI": [ZoneFile("finland", "finland", 15)],
    "FR": [ZoneFile("france", "france", 15)],
    "DE": [ZoneFile("germany", "germany_luxembourg", 15)],
    "GB": [
        ZoneFile("great-britain", "great-britain", 60, market="Hourly"),
        ZoneFile("great-britain", "great-britain", 30, market="HalfHourly"),
    ],
    "NL": [ZoneFile("netherlands", "netherlands", 15)],
    "NO1": [ZoneFile("norway 1", "norway 1", 15)],
    "NO2": [ZoneFile("norway 2", "norway 2", 15)],
    "NO3": [ZoneFile("norway 3", "norway 3", 15)],
    "NO4": [ZoneFile("norway 4", "norway 4", 15)],
    "NO5": [ZoneFile("norway 5", "norway 5", 15)],
    "PL": [ZoneFile("poland", "poland", 15)],
    "SE1": [ZoneFile("sweden 1", "sweden 1", 15)],
    "SE2": [ZoneFile("sweden 2", "sweden 2", 15)],
    "SE3": [ZoneFile("sweden 3", "sweden 3", 15)],
    "SE4": [ZoneFile("sweden 4", "sweden 4", 15)],
    "CH": [ZoneFile("switzerland", "switzerland", 60)],
}

_RESOLUTION_FILE_CONVENTION = {
    # resolution_minutes: (period folder, filename prefix, filename infix)
    15: ("Quarter-hourly", "", "_15"),
    30: ("Half-hourly", "hh_", ""),
    60: ("Hourly", "", ""),
}


def _day_bounds_utc(date: dt.date) -> tuple:
    start = DELIVERY_DAY_TZ.localize(dt.datetime.combine(date, dt.time.min)).astimezone(dt.timezone.utc)
    end = DELIVERY_DAY_TZ.localize(dt.datetime.combine(date + dt.timedelta(days=1), dt.time.min)).astimezone(dt.timezone.utc)
    return start, end


def _convert_subhour_to_timestamp(date: pd.Timestamp, slot: str, resolution_minutes: int) -> pd.Timestamp:
    """"Hour 3 Q2" -> that sub-hourly slot's start, handling the DST-ambiguous "Hour 3A"/"3B" columns.

    Covers both quarter-hourly (15 min/slot) and half-hourly (30 min/slot) files - the slot
    index within the hour (Q1, Q2, ...) is scaled by resolution_minutes.
    """
    _, hour_str, quarter_str = slot.split(" ")
    if hour_str == "3A":
        hour, ambiguous = 2, True  # CEST (spring/summer)
    elif hour_str == "3B":
        hour, ambiguous = 2, False  # CET (winter)
    else:
        hour, ambiguous = int(hour_str) - 1, "raise"

    slot_index = int(quarter_str[1]) - 1
    naive = pd.Timestamp(date) + pd.Timedelta(hours=hour) + pd.Timedelta(minutes=slot_index * resolution_minutes)
    return naive.tz_localize(tz="Europe/Copenhagen", ambiguous=ambiguous)


def _convert_hour_to_timestamp(date: pd.Timestamp, slot: str) -> pd.Timestamp:
    """"Hour 3" -> that hour's start, handling the DST-ambiguous "Hour 3A"/"3B" columns."""
    hour_str = slot.split()[1]
    if hour_str == "3A":
        hour, ambiguous = 2, True  # CEST (spring/summer)
    elif hour_str == "3B":
        hour, ambiguous = 2, False  # CET (winter)
    else:
        hour, ambiguous = int(hour_str) - 1, "raise"

    naive = pd.Timestamp(date) + pd.Timedelta(hours=hour)
    return naive.tz_localize(tz="Europe/Copenhagen", ambiguous=ambiguous)


def _remote_path(zone: ZoneFile, year: int) -> str:
    period, filename_prefix, filename_infix = _RESOLUTION_FILE_CONVENTION[zone.resolution_minutes]
    freshness = "Current" if year == dt.date.today().year else "Historical"
    return (
        f"/{zone.folder}/Day-Ahead Auction/{period}/{freshness}/Prices_Volumes/"
        f"{filename_prefix}auction_spot{filename_infix}_prices_{zone.filename_slug}_{year}.csv"
    )


def fetch_day_ahead_file(zone: ZoneFile, year: int) -> tuple:
    """fetch one zone file's rolling annual day-ahead auction price file."""
    remote_path = _remote_path(zone, year)
    logger.info("fetching EPEX file %s", remote_path)
    content = epex_client.fetch_file(remote_path)
    if content is None:
        return None, None
    forecasttime = epex_client.stat_mtime(remote_path)
    return content, forecasttime


def _extract_currency(content: bytes) -> str:
    """EPEX's skipped first line reads like "...Prices - EPEX Spot Market Auction - austria - Currency: EUR"."""
    first_line = content.split(b"\n", 1)[0].decode("utf-8", errors="replace")
    if "Currency:" in first_line:
        return first_line.rsplit("Currency:", 1)[1].strip()
    return DEFAULT_CURRENCY


def parse_csv(content: bytes, bidding_zone: str, zone: ZoneFile, forecasttime: pd.Timestamp) -> pd.DataFrame:
    """parse one zone's annual day-ahead auction CSV into prod.prices-shaped rows."""
    currency = _extract_currency(content)

    df = pd.read_csv(io.BytesIO(content), skiprows=1)
    hour_cols = [c for c in df.columns if c.startswith("Hour ")]
    df = df.assign(Date=pd.to_datetime(df["Delivery day"], dayfirst=True)).set_index("Date")[hour_cols]
    df.columns.name = "slot"
    df = df.unstack().rename("price").reset_index()
    df = df.loc[df["price"].notnull()]

    if zone.resolution_minutes == 60:
        convert = _convert_hour_to_timestamp
    else:
        convert = partial(_convert_subhour_to_timestamp, resolution_minutes=zone.resolution_minutes)
    valuetime = df.apply(lambda row: convert(row["Date"], row["slot"]), axis=1).dt.tz_convert("UTC")

    df = df.assign(
        valuetime=valuetime,
        forecasttime=forecasttime,
        bidding_zone=bidding_zone,
        product=PRODUCT,
        market=zone.market,
        source=SOURCE,
        resolution=zone.resolution_minutes,
        currency=currency,
    )

    columns = ["valuetime", "forecasttime", "bidding_zone", "product", "market", "source", "resolution", "currency", "price"]
    return df[columns].reset_index(drop=True)


def fetch_and_parse(bidding_zones: list, from_date: dt.date, to_date: dt.date) -> pd.DataFrame:
    years = sorted(set(range(from_date.year, to_date.year + 1)))
    window_start, _ = _day_bounds_utc(from_date)
    _, window_end = _day_bounds_utc(to_date)

    frames = []
    for bidding_zone in bidding_zones:
        for zone in ZONE_FILE_CONFIG[bidding_zone]:
            for year in years:
                content, forecasttime = fetch_day_ahead_file(zone, year)
                if content is None:
                    logger.warning("skipping %s %s (%s) %s: EPEX fetch failed", bidding_zone, zone.market, zone.resolution_minutes, year)
                    continue
                try:
                    frames.append(parse_csv(content, bidding_zone, zone, forecasttime))
                except (KeyError, ValueError):
                    logger.error(
                        "skipping %s %s (%s) %s: failed to parse EPEX file",
                        bidding_zone, zone.market, zone.resolution_minutes, year, exc_info=True,
                    )

    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    return combined.loc[(combined["valuetime"] >= window_start) & (combined["valuetime"] < window_end)].reset_index(
        drop=True
    )


def dump(df: pd.DataFrame) -> None:
    """write day-ahead prices to prod.prices via PriceStore."""
    # OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # for bidding_zone, zone_df in df.groupby("bidding_zone"):
    #     zone_df.to_csv(OUTPUT_DIR / f"{bidding_zone}.csv", index=False)

    written = price_store.dump(df)
    logger.info("PriceStore.dump: wrote %d row(s) for EPEX day-ahead", written)


# cron: */15 13-14 * * *  (CET/CEST; SDAC clears ~12:55 CET/CEST, catch-up starts 13:00)
@flow
def run(from_date: Optional[dt.date] = None, to_date: Optional[dt.date] = None) -> pd.DataFrame:
    """fetch EPEX SDAC day-ahead prices and dump to prod.prices.

    GB is excluded - separate (non-SDAC) auction, own schedule, see run_gb().
    from_date/to_date optional for historical backfill; defaults to tomorrow only.
    """
    setup_logging()
    tomorrow = dt.date.today() + dt.timedelta(days=1)
    from_date = from_date or tomorrow
    to_date = to_date or tomorrow

    zones = [zone for zone in ZONE_FILE_CONFIG if zone != "GB"]
    df = fetch_and_parse(zones, from_date=from_date, to_date=to_date)
    if df.empty:
        logger.warning("no EPEX day-ahead data fetched for %s to %s", from_date, to_date)
        return df

    dump(df)
    return df


# fetches both Hourly (N2EX-equivalent) and HalfHourly GB markets - needs two catch-up schedules, not one.
# Prefect runs in CET/CEST, so these are written in CET/CEST wall-clock, not UK local time:
# cron (Hourly):     */15 11-12 * * *  (CET/CEST; UK gate closure 09:50 / results by 10:00 UK = 10:50/11:00 CET)
# cron (HalfHourly): */15 15-16 * * *  (CET/CEST; UK gate closure 14:30 UK = 15:30 CET, results shortly after)
# NB: assumes UK and EU keep changing clocks on the same date - if that ever diverges, these drift by up to a week around the transition
@flow
def run_gb(from_date: Optional[dt.date] = None, to_date: Optional[dt.date] = None) -> pd.DataFrame:
    """fetch EPEX GB day-ahead prices and dump to prod.prices.

    GB isn't in SDAC - separate auction, separate (earlier) publish time, own schedule.
    Split out from run() so its schedule doesn't wait on SDAC's later clearing, and run()
    doesn't waste a call on GB while its own auction is still pending.
    from_date/to_date optional for historical backfill; defaults to tomorrow only.
    """
    setup_logging()
    tomorrow = dt.date.today() + dt.timedelta(days=1)
    from_date = from_date or tomorrow
    to_date = to_date or tomorrow

    df = fetch_and_parse(["GB"], from_date=from_date, to_date=to_date)
    if df.empty:
        logger.warning("no EPEX GB day-ahead data fetched for %s to %s", from_date, to_date)
        return df

    dump(df)
    return df


if __name__ == "__main__":
    run()
