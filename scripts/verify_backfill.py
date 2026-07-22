"""One-off diagnostic: verify the 2024-01-01 historical backfill (see scripts/backfill_2024.py)
actually landed complete, gap-free data for every in-scope bidding zone - not just that
MIN(valuetime)/MAX(valuetime) look right, since days can be missing in between those two points.

Read-only: only SELECTs against prod.prices, never writes. Run manually:
`poetry run python scripts/verify_backfill.py`.

Checks, per (bidding_zone, market):
  1. Existence - every delivery day from 2024-01-01 to yesterday has at least one row from
     ANY source (redundancy/2-source coverage is a live-operation goal, not a backfill
     requirement - see project-overview.md > Goal).
  2. Completeness - days that do have rows have the FULL expected settlement-period count for
     that day (span of the delivery day in UTC, DST-aware, divided by that day's resolution),
     not just a partial scrape.
  3. Correctness sanity - no NULL price/resolution/currency, resolution in the known-valid set,
     no wildly-out-of-range prices.

Delivery-day boundary uses Europe/Copenhagen, same anchor as every other cross-cutting script
in this repo (monitoring/day_ahead_completeness.py, monitoring/coverage.py).
"""
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import pytz
from sqlalchemy import text

from core import dev_paths  # noqa: F401  (adds sibling Production repo to sys.path for Database.*)
from Database.db_connect import engine
from monitoring.day_ahead_completeness import IN_SCOPE_ZONES, _day_bounds_utc

MARKET_TYPE = "DAY_AHEAD"
FROM_DATE = dt.date(2024, 1, 1)
TO_DATE = dt.date.today() - dt.timedelta(days=1)
DELIVERY_DAY_TZ = pytz.timezone("Europe/Copenhagen")

VALID_RESOLUTIONS = {15, 30, 60}
# EUR/MWh day-ahead prices occasionally go deeply negative (oversupply) or spike during scarcity,
# but a value outside this band is more likely a parsing/unit bug than a real clearing price.
PRICE_SANITY_MIN, PRICE_SANITY_MAX = -1000, 5000

AGGREGATE_SQL = text(
    """
    WITH local_days AS (
        SELECT bidding_zone, market, source, valuetime, resolution, currency, price,
               (valuetime AT TIME ZONE 'Europe/Copenhagen')::date AS local_day
        FROM prod.prices
        WHERE market_type = :market_type
          AND valuetime >= :from_valuetime
          AND valuetime < :to_valuetime
    )
    SELECT bidding_zone, market, local_day,
           COUNT(DISTINCT valuetime) AS actual_periods,
           MODE() WITHIN GROUP (ORDER BY resolution) AS resolution,
           COUNT(*) FILTER (WHERE resolution IS NULL OR price IS NULL OR currency IS NULL) AS null_count,
           COUNT(*) FILTER (WHERE resolution NOT IN (15, 30, 60)) AS bad_resolution_count,
           COUNT(*) FILTER (WHERE price < :price_min OR price > :price_max) AS bad_price_count,
           array_agg(DISTINCT source) AS sources
    FROM local_days
    GROUP BY bidding_zone, market, local_day
    ORDER BY bidding_zone, market, local_day
    """
)


def fetch_daily_aggregates(from_date: dt.date, to_date: dt.date) -> pd.DataFrame:
    from_valuetime, _ = _day_bounds_utc(from_date)
    _, to_valuetime = _day_bounds_utc(to_date)
    df = pd.read_sql(
        AGGREGATE_SQL,
        engine,
        params={
            "market_type": MARKET_TYPE,
            "from_valuetime": from_valuetime,
            "to_valuetime": to_valuetime,
            "price_min": PRICE_SANITY_MIN,
            "price_max": PRICE_SANITY_MAX,
        },
    )
    df["local_day"] = pd.to_datetime(df["local_day"]).dt.date
    return df


def expected_periods_for_day(day: dt.date, resolution: int) -> int:
    start, end = _day_bounds_utc(day)
    span_minutes = (end - start).total_seconds() / 60
    return round(span_minutes / resolution)


def full_calendar(from_date: dt.date, to_date: dt.date) -> list[dt.date]:
    days = []
    d = from_date
    while d <= to_date:
        days.append(d)
        d += dt.timedelta(days=1)
    return days


def build_reports(from_date: dt.date = FROM_DATE, to_date: dt.date = TO_DATE) -> tuple[dict, list]:
    """query prod.prices and build a per-zone gap/correctness report.

    returns (zone_reports, correctness_issues) - reusable by anything that wants the raw
    result (e.g. backfill_2024.py auto-verifying itself), not just this script's own CLI report.
    """
    df = fetch_daily_aggregates(from_date, to_date)
    calendar_set = set(full_calendar(from_date, to_date))

    zone_reports = {}
    correctness_issues = []

    for zone in IN_SCOPE_ZONES:
        zone_df = df[df["bidding_zone"] == zone]

        if zone_df.empty:
            zone_reports[zone] = {
                "status": "MISSING",
                "first_day": None,
                "last_day": None,
                "missing_days": sorted(calendar_set),
                "partial_days": [],
                "markets": [],
            }
            continue

        markets = sorted(zone_df["market"].unique())
        # union of days covered by ANY market/source for this zone - existence check is
        # zone-level, not per-market, so one market filling a gap left by another still counts.
        zone_days_present = set(zone_df["local_day"])
        missing_days = sorted(calendar_set - zone_days_present)

        partial_days = []
        for _, row in zone_df.iterrows():
            expected = expected_periods_for_day(row["local_day"], int(row["resolution"]))
            if row["actual_periods"] < expected:
                partial_days.append((row["local_day"], row["market"], int(row["actual_periods"]), expected))

            if row["null_count"] or row["bad_resolution_count"] or row["bad_price_count"]:
                correctness_issues.append(
                    (zone, row["market"], row["local_day"], int(row["null_count"]),
                     int(row["bad_resolution_count"]), int(row["bad_price_count"]))
                )

        first_day = min(zone_days_present)
        last_day = max(zone_days_present)

        if missing_days or partial_days:
            status = "GAPS"
        elif first_day > from_date:
            status = "LATE_START"
        elif last_day < to_date:
            status = "STALE"
        else:
            status = "OK"

        zone_reports[zone] = {
            "status": status,
            "first_day": first_day,
            "last_day": last_day,
            "missing_days": missing_days,
            "partial_days": partial_days,
            "markets": markets,
        }

    return zone_reports, correctness_issues


def print_report(
    zone_reports: dict,
    correctness_issues: list,
    from_date: dt.date = FROM_DATE,
    to_date: dt.date = TO_DATE,
) -> bool:
    """print the same human-readable report main() always has, return True iff fully clean
    (every zone OK, no correctness issues) - the pass/fail signal callers can act on.
    """
    ok_zones = [z for z, r in zone_reports.items() if r["status"] == "OK"]
    problem_zones = [z for z, r in zone_reports.items() if r["status"] != "OK"]

    print(f"=== SUMMARY: {len(ok_zones)}/{len(zone_reports)} zones fully complete {from_date}..{to_date} ===\n")

    if ok_zones:
        print(f"OK ({len(ok_zones)}): {', '.join(sorted(ok_zones))}\n")

    for zone in sorted(problem_zones):
        r = zone_reports[zone]
        print(f"--- {zone}: {r['status']} ---")
        print(f"  markets: {r['markets']}")
        print(f"  coverage: {r['first_day']} .. {r['last_day']}")
        if r["missing_days"]:
            print(f"  fully missing days ({len(r['missing_days'])}): {_summarize_days(r['missing_days'])}")
        if r["partial_days"]:
            print(f"  partial days ({len(r['partial_days'])}):")
            for day, market, actual, expected in r["partial_days"][:20]:
                print(f"    {day} [{market}]: {actual}/{expected} periods")
            if len(r["partial_days"]) > 20:
                print(f"    ... and {len(r['partial_days']) - 20} more")
        print()

    if correctness_issues:
        print(f"=== CORRECTNESS ISSUES ({len(correctness_issues)} zone/market/day rows) ===")
        for zone, market, day, nulls, bad_res, bad_price in correctness_issues[:30]:
            print(f"  {zone} [{market}] {day}: nulls={nulls} bad_resolution={bad_res} bad_price={bad_price}")
        if len(correctness_issues) > 30:
            print(f"  ... and {len(correctness_issues) - 30} more")
    else:
        print("=== CORRECTNESS: no NULLs, invalid resolutions, or out-of-range prices found ===")

    return not problem_zones and not correctness_issues


def main() -> None:
    print(f"Fetching daily aggregates for {len(IN_SCOPE_ZONES)} zones, {FROM_DATE} .. {TO_DATE} ...")
    zone_reports, correctness_issues = build_reports()
    print_report(zone_reports, correctness_issues)


def _summarize_days(days: list[dt.date]) -> str:
    """collapse a sorted list of dates into contiguous ranges for compact printing."""
    if not days:
        return ""
    ranges = []
    start = prev = days[0]
    for day in days[1:]:
        if (day - prev).days > 1:
            ranges.append((start, prev))
            start = day
        prev = day
    ranges.append((start, prev))
    return ", ".join(f"{a}" if a == b else f"{a}..{b}" for a, b in ranges)


if __name__ == "__main__":
    main()
