"""One-off historical backfill: 2024-01-01 (or each source's own documented floor) through
yesterday, across every day-ahead source. Dumps to prod.prices only - every dump() call below
passes publish=False explicitly, so none of this replays onto the live NATS stream.

Run manually, not scheduled: `poetry run python scripts/backfill_2024.py`.
See project-overview.md > Cross-cutting > Historical backfill for the per-source floor reasoning.
"""
import datetime as dt
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # repo root, so `core`/`clients` resolve regardless of invocation style

from core import setup_logging
from verify_backfill import build_reports, print_report  # sibling module in scripts/, see verify_backfill.py

logger = logging.getLogger(__name__)

YESTERDAY = dt.date.today() - dt.timedelta(days=1)


def iter_year_chunks(from_date: dt.date, to_date: dt.date):
    """split [from_date, to_date] at calendar-year boundaries - ENTSO-E/EPEX's practical per-request range cap."""
    start = from_date
    while start <= to_date:
        end = min(dt.date(start.year, 12, 31), to_date)
        yield start, end
        start = end + dt.timedelta(days=1)


def iter_month_chunks(from_date: dt.date, to_date: dt.date):
    """split [from_date, to_date] at calendar-month boundaries, for incremental dump/progress logging."""
    start = from_date
    while start <= to_date:
        next_month = dt.date(start.year + 1, 1, 1) if start.month == 12 else dt.date(start.year, start.month + 1, 1)
        end = min(next_month - dt.timedelta(days=1), to_date)
        yield start, end
        start = end + dt.timedelta(days=1)


def dump_chunk(price_store, df: pd.DataFrame, label: str) -> None:
    if df.empty:
        logger.info("%s: no rows fetched", label)
        return
    try:
        written = price_store.dump(df, publish=False)
        logger.info("%s: wrote %d/%d row(s)", label, written, len(df))
    except Exception:
        logger.error("%s: dump failed", label, exc_info=True)


def backfill_okte():
    from clients.okte.endpoints.day_ahead import fetch_and_parse, price_store

    # OKTE's API caps a single request at one calendar year (confirmed live: a 2024-01-01..
    # 2025-12-31 call 400s, so unlike OTE it does need year-chunking for a multi-year backfill).
    from_date, to_date = dt.date(2024, 1, 1), YESTERDAY
    for chunk_start, chunk_end in iter_year_chunks(from_date, to_date):
        df = fetch_and_parse(chunk_start, chunk_end)
        dump_chunk(price_store, df, f"OKTE {chunk_start}..{chunk_end}")


def backfill_ote():
    from clients.ote.endpoints.day_ahead import fetch_and_parse, price_store

    from_date, to_date = dt.date(2025, 10, 1), YESTERDAY  # documented floor: CZ 15-min go-live
    df = fetch_and_parse(from_date, to_date)
    dump_chunk(price_store, df, f"OTE {from_date}..{to_date}")


def backfill_opcom():
    from clients.opcom.endpoints.day_ahead import fetch_and_parse, price_store

    from_date, to_date = dt.date(2024, 1, 1), YESTERDAY
    for chunk_start, chunk_end in iter_month_chunks(from_date, to_date):
        df = fetch_and_parse(chunk_start, chunk_end)
        dump_chunk(price_store, df, f"OPCOM {chunk_start}..{chunk_end}")


def backfill_omie():
    from clients.omie.endpoints.day_ahead import fetch_and_parse, price_store

    from_date, to_date = dt.date(2024, 1, 1), YESTERDAY
    for chunk_start, chunk_end in iter_month_chunks(from_date, to_date):
        df = fetch_and_parse(chunk_start, chunk_end)
        dump_chunk(price_store, df, f"OMIE {chunk_start}..{chunk_end}")


def backfill_enex():
    from clients.enex.endpoints.day_ahead import fetch_and_parse, price_store

    from_date, to_date = dt.date(2026, 1, 1), YESTERDAY  # documented listing floor
    for chunk_start, chunk_end in iter_month_chunks(from_date, to_date):
        df = fetch_and_parse(chunk_start, chunk_end)
        dump_chunk(price_store, df, f"ENEX {chunk_start}..{chunk_end}")


def backfill_semo():
    from clients.semo.endpoints.day_ahead import fetch_and_parse, price_store

    from_date, to_date = YESTERDAY - dt.timedelta(days=365), YESTERDAY  # ~12-month retention floor
    for chunk_start, chunk_end in iter_month_chunks(from_date, to_date):
        df = fetch_and_parse(chunk_start, chunk_end)
        dump_chunk(price_store, df, f"SEMO {chunk_start}..{chunk_end}")


def backfill_nordpool():
    from clients.nordpool.endpoints.day_ahead import fetch_and_parse, price_store

    # deliberately not 2024-01-01: anything older than the ~2-month rolling window is a
    # guaranteed 401 that still burns a 10s retry sleep per request for no benefit.
    from_date, to_date = YESTERDAY - dt.timedelta(days=60), YESTERDAY
    df = fetch_and_parse(from_date, to_date)
    dump_chunk(price_store, df, f"NORDPOOL {from_date}..{to_date}")


def backfill_nordpool_gb():
    from clients.nordpool.endpoints.day_ahead_gb import fetch_and_parse, price_store

    from_date, to_date = YESTERDAY - dt.timedelta(days=60), YESTERDAY
    df = fetch_and_parse(from_date, to_date)
    dump_chunk(price_store, df, f"NORDPOOL GB {from_date}..{to_date}")


def backfill_epex_run():
    from clients.epex.endpoints.day_ahead import ZONE_FILE_CONFIG, fetch_and_parse, price_store

    zones = [zone for zone in ZONE_FILE_CONFIG if zone != "GB"]
    from_date, to_date = dt.date(2024, 1, 1), YESTERDAY
    for chunk_start, chunk_end in iter_year_chunks(from_date, to_date):
        df = fetch_and_parse(zones, chunk_start, chunk_end)
        dump_chunk(price_store, df, f"EPEX {chunk_start}..{chunk_end}")


def backfill_epex_gb():
    from clients.epex.endpoints.day_ahead import fetch_and_parse, price_store

    from_date, to_date = dt.date(2024, 1, 1), YESTERDAY
    for chunk_start, chunk_end in iter_year_chunks(from_date, to_date):
        df = fetch_and_parse(["GB"], chunk_start, chunk_end)
        dump_chunk(price_store, df, f"EPEX GB {chunk_start}..{chunk_end}")


def _entsoe_fetch_range(bidding_zone: str, from_date: dt.date, to_date: dt.date, market: str) -> pd.DataFrame:
    """fetch one ENTSO-E zone across an arbitrary date range in a single request, instead of
    clients/entsoe's normal per-day loop - backfill-only, since the live daily run only ever
    needs a single day. Reuses the existing single-day building blocks unchanged: _day_bounds_utc
    for the request window, parse_response for the response (already handles a list of Period
    elements per document, so a multi-day response needs no new parsing logic)."""
    from clients.entsoe.client import fetch as entsoe_fetch
    from clients.entsoe.config import BIDDING_ZONE_TO_ENTSOE_AREA
    from clients.entsoe.endpoints.day_ahead import _day_bounds_utc, parse_response

    domain = BIDDING_ZONE_TO_ENTSOE_AREA[bidding_zone]
    period_start, _ = _day_bounds_utc(from_date)
    _, period_end = _day_bounds_utc(to_date)
    params = {
        "documentType": "A44",
        "In_Domain": domain,
        "Out_Domain": domain,
        "periodStart": period_start.strftime("%Y%m%d%H%M"),
        "periodEnd": period_end.strftime("%Y%m%d%H%M"),
        "contract_MarketAgreement.type": "A01",
    }
    raw = entsoe_fetch(params)
    if raw is None:
        logger.warning("skipping ENTSO-E %s %s..%s: fetch failed", bidding_zone, from_date, to_date)
        return pd.DataFrame()
    try:
        return parse_response(raw, bidding_zone, pd.Timestamp.now(tz="UTC"), market=market)
    except (KeyError, ValueError):
        logger.error("skipping ENTSO-E %s %s..%s: failed to parse response", bidding_zone, from_date, to_date, exc_info=True)
        return pd.DataFrame()


def _run_entsoe_tasks(tasks, price_store, max_workers=5):
    """fetch+dump a list of (zone, chunk_start, chunk_end, market) ENTSO-E tasks concurrently.

    ENTSO-E's own response time for a wide periodStart/periodEnd range turns out to dominate
    (observed live: a single zone-year request can take ~10+ minutes), not our parsing/DB
    write - so with 34 zones x 3 year-chunks fully sequential, a full run is many hours. These
    are independent I/O-bound requests, so a modest thread pool lets wall-clock time track the
    slowest single request instead of their sum. Kept conservative (5 workers) to avoid
    tripping ENTSO-E rate limits on one API key.
    """
    import concurrent.futures

    def _run_one(zone, chunk_start, chunk_end, market):
        df = _entsoe_fetch_range(zone, chunk_start, chunk_end, market)
        dump_chunk(price_store, df, f"ENTSO-E {zone} {chunk_start}..{chunk_end}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_run_one, zone, chunk_start, chunk_end, market) for zone, chunk_start, chunk_end, market in tasks]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception:
                logger.error("ENTSO-E backfill task failed unexpectedly", exc_info=True)


def backfill_entsoe_run():
    from clients.entsoe.config import BIDDING_ZONE_TO_ENTSOE_AREA
    from clients.entsoe.endpoints.day_ahead import MARKET, price_store

    zones = [zone for zone in BIDDING_ZONE_TO_ENTSOE_AREA if zone != "IE"]
    from_date, to_date = dt.date(2024, 1, 1), YESTERDAY
    tasks = [(zone, chunk_start, chunk_end, MARKET) for zone in zones for chunk_start, chunk_end in iter_year_chunks(from_date, to_date)]
    _run_entsoe_tasks(tasks, price_store)


def backfill_entsoe_ie():
    from clients.entsoe.endpoints.day_ahead import MARKET_IE, price_store

    from_date, to_date = dt.date(2024, 1, 1), YESTERDAY
    tasks = [("IE", chunk_start, chunk_end, MARKET_IE) for chunk_start, chunk_end in iter_year_chunks(from_date, to_date)]
    _run_entsoe_tasks(tasks, price_store)


BACKFILLS = [
    ("OKTE", backfill_okte),
    ("OTE", backfill_ote),
    ("OPCOM", backfill_opcom),
    ("OMIE", backfill_omie),
    ("ENEX", backfill_enex),
    ("SEMO", backfill_semo),
    ("NORDPOOL", backfill_nordpool),
    ("NORDPOOL GB", backfill_nordpool_gb),
    ("EPEX", backfill_epex_run),
    ("EPEX GB", backfill_epex_gb),
    ("ENTSO-E", backfill_entsoe_run),
    ("ENTSO-E IE", backfill_entsoe_ie),
]


def main():
    setup_logging()
    for name, fn in BACKFILLS:
        logger.info("=== starting backfill: %s ===", name)
        try:
            fn()
        except Exception:
            logger.error("=== %s backfill aborted by unexpected error ===", name, exc_info=True)
        else:
            logger.info("=== finished backfill: %s ===", name)
    logger.info("=== all backfills complete - verifying against prod.prices (not just trusting exit code 0) ===")

    # each backfill call above catches and logs its own fetch failures per chunk rather than
    # raising, so a clean run through BACKFILLS is NOT proof of complete data - a transient
    # fetch failure (SFTP hiccup, ENTSO-E timeout/throttling under concurrency) can silently
    # drop a whole zone/year with no non-zero exit code. Found live 2026-07-22: NO1/NO2 lost
    # ~9 months this way and the run still "succeeded" - see project-overview.md > Known gaps.
    # verify_backfill.py's gap scan (existence + full settlement-period count per day, not
    # MIN/MAX) is the only reliable completeness signal.
    zone_reports, correctness_issues = build_reports()
    all_clean = print_report(zone_reports, correctness_issues)
    if all_clean:
        logger.info("=== verification: all in-scope zones complete, no gaps found ===")
    else:
        logger.warning("=== verification: gaps or correctness issues found - see report above, re-run affected source/zone/date combinations ===")


if __name__ == "__main__":
    main()
