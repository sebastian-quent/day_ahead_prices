"""per-zone DAY_AHEAD price summary for a delivery day, for the map dashboard's /api/prices.

same query/grouping approach as monitoring/coverage.py's build_source_coverage(), extended with:
- a headline "baseload" price per zone (mean price across the day's settlement periods - same
  thing EPEX's own market-results map calls "Baseload"), averaged across sources rather than a
  straight row-mean, for the same GB mixed-resolution reason coverage.py already accounts for.
- a per-period price curve from whichever source landed the most periods that day ("primary"),
  for the hover detail table.
"""

import datetime as dt

import pandas as pd
import pytz

from core import PriceStore
from Database.db_connect import engine

MARKET_TYPE = "DAY_AHEAD"
DELIVERY_DAY_TZ = pytz.timezone("Europe/Copenhagen")

# same 41-zone list as monitoring/day_ahead_completeness.py and monitoring/coverage.py,
# duplicated rather than shared via core/ - consistent with those modules' own note to only
# promote it once a need for real sharing (not just avoiding a 5th copy) shows up.
IN_SCOPE_ZONES = [
    "AT", "BE", "BG", "CH", "CZ", "DE", "DK1", "DK2", "EE", "ES", "FI", "FR", "GB", "GR",
    "HR", "HU", "IE", "IT_NORD", "IT_CNOR", "IT_CSUD", "IT_SUD", "IT_SICI", "IT_SARD",
    "IT_CALA", "LT", "LV", "NL", "NO1", "NO2", "NO3", "NO4", "NO5", "PL", "PT", "RO",
    "SE1", "SE2", "SE3", "SE4", "SI", "SK",
]

price_store = PriceStore(engine)


def _day_bounds_utc(date: dt.date) -> tuple[dt.datetime, dt.datetime]:
    start = DELIVERY_DAY_TZ.localize(dt.datetime.combine(date, dt.time.min)).astimezone(dt.timezone.utc)
    end = DELIVERY_DAY_TZ.localize(dt.datetime.combine(date + dt.timedelta(days=1), dt.time.min)).astimezone(dt.timezone.utc)
    return start, end


def build_zone_summary(target_date: dt.date) -> dict[str, dict]:
    """one entry per IN_SCOPE_ZONES, keyed by bidding_zone.

    headline `avg_price` ("baseload") is the mean of each (source, market)'s own average price,
    not a straight row-mean - GB lands two markets at different resolutions (N2EX hourly,
    GbHalfHour half-hourly, see project-overview.md), and a plain row-mean would let the
    half-hourly market's 2x row count silently outweigh the hourly one. `curve` is the raw
    per-period prices from the single (source, market) that landed the most periods that day.
    """
    start, end = _day_bounds_utc(target_date)
    df = price_store.get(
        market_type=MARKET_TYPE, from_valuetime=pd.Timestamp(start), to_valuetime=pd.Timestamp(end)
    )

    summary = {
        zone: {"has_data": False, "avg_price": None, "currency": None, "sources": [], "curve_source": None, "curve": []}
        for zone in IN_SCOPE_ZONES
    }
    if df.empty:
        return summary

    span_minutes = (end - start).total_seconds() / 60
    by_market = (
        df.groupby(["bidding_zone", "source", "market"])
        .agg(actual=("valuetime", "size"), resolution=("resolution", "first"),
             avg_price=("price", "mean"), currency=("currency", "first"))
        .reset_index()
    )
    by_market["expected"] = (span_minutes / by_market["resolution"]).round().astype(int)

    for zone, rows in by_market.groupby("bidding_zone"):
        if zone not in summary:
            continue  # zone not in our in-scope list (shouldn't happen, but don't blow up on it)
        sources = [
            {
                "source": row.source,
                "market": row.market,
                "actual": int(row.actual),
                "expected": int(row.expected),
                "avg_price": round(float(row.avg_price), 2),
            }
            for row in rows.itertuples()
        ]

        # "primary" source for the hover curve: whichever (source, market) landed the most
        # settlement periods for this zone/day - no per-zone primary/backup assignment exists
        # yet (see project-overview.md Scheduling), so this is a per-request, per-day pick
        # rather than a fixed table. ties broken alphabetically for determinism.
        primary = rows.sort_values(["actual", "source", "market"], ascending=[False, True, True]).iloc[0]
        curve_df = df[
            (df["bidding_zone"] == zone) & (df["source"] == primary["source"]) & (df["market"] == primary["market"])
        ].sort_values("valuetime")
        curve = [
            {"time": row.valuetime.astimezone(DELIVERY_DAY_TZ).strftime("%H:%M"), "price": round(float(row.price), 2)}
            for row in curve_df.itertuples()
        ]

        summary[zone] = {
            "has_data": True,
            "avg_price": round(float(rows["avg_price"].mean()), 2),
            "currency": rows["currency"].iloc[0],
            "sources": sources,
            "curve_source": f"{primary['source']} ({primary['market']})",
            "curve": curve,
        }

    return summary
