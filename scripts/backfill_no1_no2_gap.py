"""One-off targeted backfill: closes the NO1/NO2 data hole found by scripts/verify_backfill.py
on 2026-07-22 (see project-overview.md > Historical backfill / Known gaps).

Both EPEX and ENTSO-E independently have zero DAY_AHEAD rows for NO1 and NO2 for
2025-01-01..2025-09-30 (EPEX's own gap actually spans all five Norway zones, but NO3/4/5
were saved by ENTSO-E covering them there - only NO1/NO2 lost both sources at once). This
re-fetches just that zone/date window from both sources.

Dumps to prod.prices with publish=False - this is historical data, must not replay onto the
live NATS stream. Run manually: `poetry run python scripts/backfill_no1_no2_gap.py`.
"""
import datetime as dt
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core import setup_logging

logger = logging.getLogger(__name__)

ZONES = ["NO1", "NO2"]
FROM_DATE = dt.date(2025, 1, 1)
TO_DATE = dt.date(2025, 9, 30)


def dump_chunk(price_store, df, label: str) -> None:
    if df.empty:
        logger.info("%s: no rows fetched", label)
        return
    written = price_store.dump(df, publish=False)
    logger.info("%s: wrote %d/%d row(s)", label, written, len(df))


def backfill_epex():
    from clients.epex.endpoints.day_ahead import fetch_and_parse, price_store

    df = fetch_and_parse(ZONES, FROM_DATE, TO_DATE)
    dump_chunk(price_store, df, f"EPEX {ZONES} {FROM_DATE}..{TO_DATE}")


def backfill_entsoe():
    from clients.entsoe.endpoints.day_ahead import MARKET, fetch_and_parse, price_store

    df = fetch_and_parse(ZONES, FROM_DATE, TO_DATE, market=MARKET)
    dump_chunk(price_store, df, f"ENTSO-E {ZONES} {FROM_DATE}..{TO_DATE}")


def main():
    setup_logging()
    for name, fn in [("EPEX", backfill_epex), ("ENTSO-E", backfill_entsoe)]:
        logger.info("=== starting gap-fill: %s ===", name)
        try:
            fn()
        except Exception:
            logger.error("=== %s gap-fill aborted by unexpected error ===", name, exc_info=True)
        else:
            logger.info("=== finished gap-fill: %s ===", name)


if __name__ == "__main__":
    main()
