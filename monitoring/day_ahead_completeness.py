import datetime as dt
import logging
from typing import Optional

import pandas as pd
import pytz
from prefect import flow

from core import PriceStore, setup_logging  # noqa: E402 (must precede Database import, see core/dev_paths.py)
from Database.db_connect import engine

logger = logging.getLogger(__name__)

price_store = PriceStore(engine)

MARKET_TYPE = "DAY_AHEAD"

DELIVERY_DAY_TZ = pytz.timezone("Europe/Copenhagen")

# every bidding_zone from project-overview.md's matrix with >=1 live source today. static
# list, not shared via core/, since scrapers may split into their own repos later and a shared
# constant would complicate that split; revisit as a core/ constant if this needs to be reused.
IN_SCOPE_ZONES = [
    "AT", "BE", "BG", "CH", "CZ", "DE", "DK1", "DK2", "EE", "ES", "FI", "FR", "GB", "GR",
    "HR", "HU", "IE", "IT_NORD", "IT_CNOR", "IT_CSUD", "IT_SUD", "IT_SICI", "IT_SARD",
    "IT_CALA", "LT", "LV", "NL", "NO1", "NO2", "NO3", "NO4", "NO5", "PL", "PT", "RO",
    "SE1", "SE2", "SE3", "SE4", "SI", "SK",
]


def _day_bounds_utc(date: dt.date) -> tuple[dt.datetime, dt.datetime]:
    start = DELIVERY_DAY_TZ.localize(dt.datetime.combine(date, dt.time.min)).astimezone(dt.timezone.utc)
    end = DELIVERY_DAY_TZ.localize(dt.datetime.combine(date + dt.timedelta(days=1), dt.time.min)).astimezone(dt.timezone.utc)
    return start, end


def check_completeness(target_date: dt.date) -> list[str]:
    """return the sorted list of in-scope bidding zones with zero DAY_AHEAD rows for target_date."""
    start, end = _day_bounds_utc(target_date)
    df = price_store.get(market_type=MARKET_TYPE, from_valuetime=pd.Timestamp(start), to_valuetime=pd.Timestamp(end))
    zones_with_data = set(df["bidding_zone"].unique())
    return sorted(set(IN_SCOPE_ZONES) - zones_with_data)


def send_alert(missing_zones: list[str], target_date: dt.date) -> None:
    """notify that one or more in-scope zones have no DAY_AHEAD data for target_date.

    TODO: wire up an actual channel (email vs Teams - not decided yet). logging only for now.
    """
    logger.warning(
        "day-ahead completeness check: %d zone(s) missing data for %s: %s",
        len(missing_zones),
        target_date,
        ", ".join(missing_zones),
    )


# cron: 0 17 * * *  (CET/CEST; runs after every live source's catch-up window for tomorrow's
# delivery day has closed - GB HalfHourly is the latest at ~15:30 CET)
@flow
def run(target_date: Optional[dt.date] = None) -> list[str]:
    """check that every in-scope bidding zone has at least one DAY_AHEAD row for target_date.

    never raises/fails the Prefect run on missing data - a zone with zero rows is a
    monitorable outcome (logged + alerted), not a code error. target_date defaults to
    tomorrow's delivery day.
    """
    setup_logging()
    target_date = target_date or dt.date.today() + dt.timedelta(days=1)

    missing = check_completeness(target_date)
    if missing:
        send_alert(missing, target_date)
    else:
        logger.info(
            "day-ahead completeness check: all %d in-scope zones have data for %s",
            len(IN_SCOPE_ZONES),
            target_date,
        )

    return missing


if __name__ == "__main__":
    run()
