import csv
import datetime as dt
import io
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from prefect import flow

from clients.semo.client import download_document, list_documents
from core import PriceStore, setup_logging  # noqa: E402 (must precede Database import, see core/dev_paths.py)
from Database.db_connect import engine

logger = logging.getLogger(__name__)

price_store = PriceStore(engine)

SOURCE = "SEMO"
PRODUCT = "DAY_AHEAD"
MARKET = "SDAC"  # SEM-DA runs under the "PWR-MRC-D+1" multi-regional coupling auction, the same coupled algorithm ENTSO-E's IE feed already reports as SDAC
BIDDING_ZONE = "IE"
DEFAULT_CURRENCY = "EUR"  # the auction's native clearing currency; GBP is only published as an FX-converted reference series

REPORT_ID = "EA-001"  # DPuG_ID shared by all SEMO "Market Results" auction reports (DA + IDA1-3); filtered down to SEM-DA below
RESOURCE_PREFIX = "MarketResult_SEM-DA_"
DELIVERY_MARKET = "ROI-DA"  # the all-island SEM-DA auction publishes identical NI-DA/ROI-DA prices under separate sections; ROI-DA maps to bidding_zone IE

# SEM-DA is coupled via MRC/SDAC alongside the rest of Europe, so - like every other SDAC
# zone in this repo - its delivery day runs midnight-to-midnight CET/CEST, not Irish local
# time; confirmed live (2026-07-15) against a document's own valuetimes, which start/end on
# the CEST day boundary rather than Ireland's UTC/UTC+1 one

OUTPUT_DIR = Path("output/semo/day_ahead")


def fetch_day_ahead_documents(from_date: dt.date, to_date: dt.date) -> Optional[list[dict]]:
    """list SEM-DA auction result documents delivering into [from_date, to_date].

    SEM-DA is a D+1 auction: DateRetention on a listed document is the auction date, one day
    before the CET/CEST delivery day (see DELIVERY_MARKET note above) it actually prices - so
    the DateRetention filter is queried one day earlier than the requested delivery range.
    """
    documents = list_documents({
        "DPuG_ID": REPORT_ID,
        "DateRetention": f">={(from_date - dt.timedelta(days=1)).isoformat()}<={(to_date - dt.timedelta(days=1)).isoformat()}",
    })
    if documents is None:
        return None
    return [d for d in documents if d.get("ResourceName", "").startswith(RESOURCE_PREFIX)]


def _parse_market_result(content: bytes) -> tuple[Optional[pd.Timestamp], Optional[int], dict[str, float]]:
    """parse a SEM-DA MarketResult csv, returning (publication time, resolution minutes,
    {iso timestamp: price}) for the ROI-DA/EUR index price series.

    the file is a Euphemia auction result dump: a few metadata rows, then one section per
    market (NI-DA, ROI-DA, ...), each holding several label/timestamp-header/value blocks
    (index prices in EUR and GBP, volumes, net position, ...) followed by thousands of rows
    of per-participant order data we don't need - parsing stops as soon as the target block
    is found instead of reading the rest of the file.
    """
    rows = csv.reader(io.StringIO(content.decode("utf-8")), delimiter=";")

    publication_time = None
    current_market = None
    for row in rows:
        if not row or not row[0]:
            continue
        label = row[0]

        if label == "Publication date time" and len(row) > 1:
            publication_time = pd.Timestamp(row[1])
        elif label == "Market" and len(row) > 1:
            current_market = row[1]
        elif (
            label == "Index prices"
            and current_market == DELIVERY_MARKET
            and len(row) > 2
            and row[2] == DEFAULT_CURRENCY
        ):
            resolution_minutes = int(row[1])
            timestamps = next(rows)
            values = next(rows)
            prices = {
                ts: float(val.replace(",", "."))
                for ts, val in zip(timestamps, values)
                if ts
            }
            return publication_time, resolution_minutes, prices

    return publication_time, None, {}


def parse_document(doc: dict) -> pd.DataFrame:
    """download and parse one SEM-DA document into prod.prices-shaped rows."""
    resource_name = doc.get("ResourceName")
    content = download_document(resource_name)
    if content is None:
        return pd.DataFrame()

    publication_time, resolution_minutes, prices = _parse_market_result(content)
    if not prices or resolution_minutes is None:
        logger.warning("SEMO: no %s/%s index price series found in %s", DELIVERY_MARKET, DEFAULT_CURRENCY, resource_name)
        return pd.DataFrame()

    forecasttime = publication_time if publication_time is not None else pd.Timestamp.now(tz="UTC")
    return pd.DataFrame(
        {
            "valuetime": pd.Timestamp(ts),
            "forecasttime": forecasttime,
            "bidding_zone": BIDDING_ZONE,
            "product": PRODUCT,
            "market": MARKET,
            "source": SOURCE,
            "resolution": resolution_minutes,
            "currency": DEFAULT_CURRENCY,
            "price": price,
        }
        for ts, price in prices.items()
    )


def fetch_and_parse(from_date: dt.date, to_date: dt.date) -> pd.DataFrame:
    documents = fetch_day_ahead_documents(from_date, to_date)
    if not documents:
        logger.warning("skipping SEMO %s to %s: no day-ahead documents found", from_date, to_date)
        return pd.DataFrame()

    dfs = []
    for doc in documents:
        try:
            df = parse_document(doc)
        except (KeyError, ValueError, TypeError, UnicodeDecodeError):
            logger.error("skipping SEMO document %s: failed to parse", doc.get("ResourceName"), exc_info=True)
            continue
        if not df.empty:
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
    logger.info("PriceStore.dump: wrote %d row(s) for SEMO day-ahead", written)


@flow
def run(from_date: Optional[dt.date] = None, to_date: Optional[dt.date] = None) -> pd.DataFrame:
    """fetch SEMO (SEM day-ahead auction) prices and dump to prod.prices.

    from_date/to_date optional for historical backfill; defaults to today+tomorrow.
    note: SEMO's static-reports API only retains roughly the last 12 months of published
    documents - DateRetention-filtered listings return nothing older than that, so this
    cannot backfill beyond that window.
    """
    setup_logging()
    today = dt.date.today()
    from_date = from_date or today
    to_date = to_date or today + dt.timedelta(days=1)

    df = fetch_and_parse(from_date=from_date, to_date=to_date)
    if df.empty:
        logger.warning("no SEMO day-ahead data fetched for %s to %s", from_date, to_date)
        return df

    dump(df)
    return df


if __name__ == "__main__":
    run()
