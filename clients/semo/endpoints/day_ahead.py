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
MARKET = "SEM_DA"  # I-SEM's own day-ahead auction, not SDAC - see project-overview.md Known gaps
BIDDING_ZONE = "IE"
DEFAULT_CURRENCY = "EUR"

REPORT_ID = "EA-001"
RESOURCE_PREFIX = "MarketResult_SEM-DA_"
DELIVERY_MARKET = "ROI-DA"  # the all-island SEM-DA auction publishes identical NI-DA/ROI-DA prices under separate sections; ROI-DA maps to bidding_zone IE


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
    # OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    # for bidding_zone, zone_df in df.groupby("bidding_zone"):
    #     zone_df.to_csv(OUTPUT_DIR / f"{bidding_zone}.csv", index=False)

    written = price_store.dump(df)
    logger.info("PriceStore.dump: wrote %d row(s) for SEMO day-ahead", written)


# cron: 5,20,35,50 1-2 * * *  (CET/CEST; SEMOpx's static-reports catalog batch-publishes each
# document at Irish midnight ~= 01:00 CET/CEST the day after its "Date" field - confirmed via
# the API's own PublishTime field, see run()'s docstring. Catch-up starts 01:05, every 15 min
# for ~2h, per the project's standard catch-up pattern.)
@flow
def run(from_date: Optional[dt.date] = None, to_date: Optional[dt.date] = None) -> pd.DataFrame:
    """fetch SEMO (SEM day-ahead auction) prices and dump to prod.prices.

    from_date/to_date optional for historical backfill; defaults to yesterday only.
    yesterday, not tomorrow: SEMOpx's static-reports catalog only lists a document at Irish
    midnight the day *after* its "Date" field (confirmed via the API's PublishTime field -
    e.g. a SEM-DA document with Date=2026-07-19 carries PublishTime=2026-07-20T00:00). For
    SEM-DA, Date is the delivery day, so on any given run day the newest delivery day actually
    published is yesterday's - targeting tomorrow (as other day-ahead sources do) always
    returns nothing.
    note: SEMO's static-reports API only retains roughly the last 12 months of published
    documents - DateRetention-filtered listings return nothing older than that, so this
    cannot backfill beyond that window.
    """
    setup_logging()
    yesterday = dt.date.today() - dt.timedelta(days=1)
    from_date = from_date or yesterday
    to_date = to_date or yesterday

    df = fetch_and_parse(from_date=from_date, to_date=to_date)
    if df.empty:
        logger.warning("no SEMO day-ahead data fetched for %s to %s", from_date, to_date)
        return df

    dump(df)
    return df


if __name__ == "__main__":
    run()
