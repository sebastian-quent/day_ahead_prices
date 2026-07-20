import datetime as dt
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import pytz
import xmltodict
from dateutil import parser as date_parser
from prefect import flow

from clients.entsoe.client import fetch as entsoe_fetch
from clients.entsoe.config import BIDDING_ZONE_TO_ENTSOE_AREA
from core import PriceStore, setup_logging  # noqa: E402 (must precede Database import, see core/dev_paths.py)
from Database.db_connect import engine

logger = logging.getLogger(__name__)

price_store = PriceStore(engine)

SOURCE = "ENTSO-E"
PRODUCT = "DAY_AHEAD"
MARKET = "SDAC"
MARKET_IE = "SEM_DA"  # IE isn't SDAC - I-SEM's own day-ahead auction, see run_ie()
DEFAULT_CURRENCY = "EUR"

OUTPUT_DIR = Path("output/entsoe/day_ahead")

DELIVERY_DAY_TZ = pytz.timezone("Europe/Copenhagen")


def _day_bounds_utc(date: dt.date) -> tuple:
    start = DELIVERY_DAY_TZ.localize(dt.datetime.combine(date, dt.time.min)).astimezone(dt.timezone.utc)
    end = DELIVERY_DAY_TZ.localize(dt.datetime.combine(date + dt.timedelta(days=1), dt.time.min)).astimezone(dt.timezone.utc)
    return start, end


def fetch_day_ahead_prices(bidding_zone: str, date: dt.date) -> Optional[bytes]:
    """fetch one zone's one day of ENTSO-E day-ahead auction prices (documentType A44)."""
    domain = BIDDING_ZONE_TO_ENTSOE_AREA[bidding_zone]
    period_start, period_end = _day_bounds_utc(date)
    params = {
        "documentType": "A44",
        "In_Domain": domain,
        "Out_Domain": domain,
        "periodStart": period_start.strftime("%Y%m%d%H%M"),
        "periodEnd": period_end.strftime("%Y%m%d%H%M"),
        "contract_MarketAgreement.type": "A01",
    }
    return entsoe_fetch(params)


def parse_response(raw: bytes, bidding_zone: str, forecasttime: pd.Timestamp, market: str = MARKET) -> pd.DataFrame:
    """parse one zone/day's ENTSO-E day-ahead price document into prod.prices-shaped rows."""
    document = xmltodict.parse(raw)
    market_document = document.get("Publication_MarketDocument")
    if market_document is None:
        reason = document.get("Acknowledgement_MarketDocument", {}).get("Reason", {}).get("text")
        logger.warning("ENTSO-E returned no price document for %s: %s", bidding_zone, reason)
        return pd.DataFrame()

    series_list = market_document["TimeSeries"]
    if not isinstance(series_list, list):
        series_list = [series_list]

    rows = []
    for series in series_list:
        sequence_position = series.get("classificationSequence_AttributeInstanceComponent.position")
        if sequence_position not in (None, "1"):
            continue  # not the SDAC auction (e.g. EXAA or another parallel sequence)
        currency = series.get("currency_Unit.name", DEFAULT_CURRENCY)
        periods = series["Period"]
        if not isinstance(periods, list):
            periods = [periods]
        for period in periods:
            start = date_parser.parse(period["timeInterval"]["start"])
            end = date_parser.parse(period["timeInterval"]["end"])
            resolution_minutes = int(period["resolution"][2:-1])  # "PT60M" -> 60, "PT15M" -> 15
            num_positions = round((end - start).total_seconds() / 60 / resolution_minutes)

            points = period["Point"]
            if not isinstance(points, list):
                points = [points]
            # ENTSO-E only emits a Point when the price changes - a position with no
            # Point holds the last published price, so gaps must be forward-filled
            position_to_price = {int(point["position"]): float(point["price.amount"]) for point in points}

            last_price = None
            for position in range(1, num_positions + 1):
                if position in position_to_price:
                    last_price = position_to_price[position]
                if last_price is None:
                    continue  # no price published yet for this position
                valuetime = pd.Timestamp(start + dt.timedelta(minutes=(position - 1) * resolution_minutes))
                rows.append(
                    {
                        "valuetime": valuetime.tz_convert("UTC"),
                        "forecasttime": forecasttime,
                        "bidding_zone": bidding_zone,
                        "product": PRODUCT,
                        "market": market,
                        "source": SOURCE,
                        "resolution": resolution_minutes,
                        "currency": currency,
                        "price": last_price,
                    }
                )
    return pd.DataFrame(rows)


def fetch_and_parse(bidding_zones: list, from_date: dt.date, to_date: dt.date, market: str = MARKET) -> pd.DataFrame:
    forecasttime = pd.Timestamp.now(tz="UTC")

    frames = []
    for bidding_zone in bidding_zones:
        for date in pd.date_range(from_date, to_date, freq="D"):
            raw = fetch_day_ahead_prices(bidding_zone, date.date())
            if raw is None:
                logger.warning("skipping %s %s: ENTSO-E fetch failed", bidding_zone, date.date())
                continue
            try:
                frames.append(parse_response(raw, bidding_zone, forecasttime, market=market))
            except (KeyError, ValueError):
                logger.error(
                    "skipping %s %s: failed to parse ENTSO-E response", bidding_zone, date.date(), exc_info=True
                )

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def dump(df: pd.DataFrame) -> None:
    """write day-ahead prices to prod.prices via PriceStore."""
    # OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # for bidding_zone, zone_df in df.groupby("bidding_zone"):
    #     zone_df.to_csv(OUTPUT_DIR / f"{bidding_zone}.csv", index=False)

    written = price_store.dump(df)
    logger.info("PriceStore.dump: wrote %d row(s) for ENTSO-E day-ahead", written)


# cron: */15 13-14 * * *  (CET/CEST; SDAC clears ~12:55 CET/CEST, catch-up starts 13:00)
@flow
def run(from_date: Optional[dt.date] = None, to_date: Optional[dt.date] = None) -> pd.DataFrame:
    """fetch ENTSO-E SDAC day-ahead prices and dump to prod.prices.

    IE is excluded - separate (non-SDAC) auction, own schedule, see run_ie().
    from_date/to_date optional for historical backfill; defaults to tomorrow only.
    """
    setup_logging()
    tomorrow = dt.date.today() + dt.timedelta(days=1)
    from_date = from_date or tomorrow
    to_date = to_date or tomorrow

    zones = [zone for zone in BIDDING_ZONE_TO_ENTSOE_AREA if zone != "IE"]
    df = fetch_and_parse(zones, from_date=from_date, to_date=to_date)
    if df.empty:
        logger.warning("no ENTSO-E day-ahead data fetched for %s to %s", from_date, to_date)
        return df

    dump(df)
    return df


# cron: */15 12-13 * * *  (CET/CEST; SEM-DA gate closure firm at 11:00 Irish time = 12:00 CET, results confirmed available same-day shortly after - unlike SEMO's static-reports catalog, ENTSO-E's IE feed has no publish lag, see project-overview.md SEMO section)
@flow
def run_ie(from_date: Optional[dt.date] = None, to_date: Optional[dt.date] = None) -> pd.DataFrame:
    """fetch ENTSO-E IE day-ahead prices and dump to prod.prices.

    IE isn't SDAC - I-SEM's separate SEM-DA auction, separate (earlier) publish time, own
    schedule. Split out from run() so its schedule doesn't wait on SDAC's later clearing,
    and run() doesn't waste a call on IE while its own auction is still pending. Labeled
    market=MARKET_IE ("SEM_DA"), not MARKET ("SDAC") - see project-overview.md Known gaps.
    from_date/to_date optional for historical backfill; defaults to tomorrow only.
    """
    setup_logging()
    tomorrow = dt.date.today() + dt.timedelta(days=1)
    from_date = from_date or tomorrow
    to_date = to_date or tomorrow

    df = fetch_and_parse(["IE"], from_date=from_date, to_date=to_date, market=MARKET_IE)
    if df.empty:
        logger.warning("no ENTSO-E IE day-ahead data fetched for %s to %s", from_date, to_date)
        return df

    dump(df)
    return df


if __name__ == "__main__":
    run()
