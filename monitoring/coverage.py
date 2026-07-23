"""prototype: which bidding zones have DAY_AHEAD data landed for a given delivery day, grouped
by scraper/source, with actual vs. expected settlement-period counts per zone.

reads prod.prices directly via PriceStore.get(). no run/error history yet (see
project-overview.md Monitoring section) - that's a separate, later system once logs are
wired up; this only answers "is data in or not (and how much of it)", not "why is it missing".

run with: poetry run streamlit run monitoring/coverage.py
"""

import datetime as dt
import sys
from pathlib import Path

# streamlit runs this file directly, which puts dashboard/ (not the project root) on sys.path -
# every other entry point in this repo is invoked as a module instead, so this is the first
# place that needs the project root added explicitly to import core/Database.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import pytz
import streamlit as st

from core import PriceStore
from Database.db_connect import engine

MARKET_TYPE = "DAY_AHEAD"
DELIVERY_DAY_TZ = pytz.timezone("Europe/Copenhagen")

# same 41-zone list as monitoring/day_ahead_completeness.py, duplicated rather than shared via
# core/ - consistent with that module's own note to only promote it once a third consumer needs it.
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


def build_source_coverage(target_date: dt.date) -> pd.DataFrame:
    """one row per (source, bidding_zone) that actually has data for target_date.

    `expected` is the delivery day's UTC span (already 23h/25h-correct on DST transition days,
    since it comes straight out of _day_bounds_utc()) divided by that row's own resolution - not
    a flat 24h assumption. sources/zones with zero rows simply don't appear here; the "fully
    missing" list is computed separately against IN_SCOPE_ZONES.
    """
    start, end = _day_bounds_utc(target_date)
    df = price_store.get(market_type=MARKET_TYPE, from_valuetime=pd.Timestamp(start), to_valuetime=pd.Timestamp(end))
    if df.empty:
        return pd.DataFrame(columns=["source", "bidding_zone", "actual", "expected"])

    span_minutes = (end - start).total_seconds() / 60
    # group by market first, not just source+zone: GB lands as two separate market rows per
    # source (N2EX hourly + GbHalfHour half-hourly, see project-overview.md), each with its own
    # resolution - collapsing straight to source+zone would pick one arbitrary resolution and
    # miscompute `expected`. each market's own expected is correct before summing them into one
    # actual/expected pair per zone.
    by_market = (
        df.groupby(["source", "bidding_zone", "market"])
        .agg(actual=("valuetime", "size"), resolution=("resolution", "first"))
        .reset_index()
    )
    by_market["expected"] = (span_minutes / by_market["resolution"]).round().astype(int)
    return by_market.groupby(["source", "bidding_zone"], as_index=False)[["actual", "expected"]].sum()


# fixed status colors (never themed - same hex on light and dark surfaces), matched to the
# team's data-viz palette. status rides on a dot + text label, never on color alone, and the
# count text itself always stays in normal ink rather than the status color (low-contrast on a
# tinted background otherwise, especially for warning's yellow).
GOOD = "#0ca30c"
WARNING = "#fab219"
CRITICAL = "#d03b3b"

STYLE = """
<style>
.source-block { margin-bottom: 16px; }
.source-name {
  font-weight: 600;
  margin-bottom: 6px;
  color: var(--text-color, inherit);
}
.source-count { font-weight: 400; opacity: 0.6; font-size: 0.8rem; margin-left: 6px; }
.chip-row { display: flex; flex-wrap: wrap; gap: 6px; }
.chip {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 3px 10px;
  border-radius: 999px;
  font-size: 0.8rem;
  font-variant-numeric: tabular-nums;
  background: var(--secondary-background-color, rgba(128, 128, 128, 0.08));
  border: 1px solid rgba(128, 128, 128, 0.18);
  color: var(--text-color, inherit);
}
.chip .dot { width: 8px; height: 8px; border-radius: 50%; flex: 0 0 auto; }
.chip.good .dot { background: __GOOD__; }
.chip.warning .dot { background: __WARNING__; }
.chip.critical .dot { background: __CRITICAL__; }
</style>
""".replace("__GOOD__", GOOD).replace("__WARNING__", WARNING).replace("__CRITICAL__", CRITICAL)


def _chip(status: str, label: str) -> str:
    return f'<span class="chip {status}"><span class="dot"></span>{label}</span>'


def render_source_section(source: str, rows: pd.DataFrame) -> str:
    chips = []
    for _, row in rows.sort_values("bidding_zone").iterrows():
        zone, actual, expected = row["bidding_zone"], int(row["actual"]), int(row["expected"])
        status = "good" if actual >= expected else "warning"
        chips.append(_chip(status, f"{zone} ({actual}/{expected})"))
    return (
        '<div class="source-block">'
        f'<div class="source-name">{source}<span class="source-count">{len(rows)} zone(s)</span></div>'
        f'<div class="chip-row">{"".join(chips)}</div>'
        "</div>"
    )


st.set_page_config(page_title="Day-ahead coverage", layout="wide")
st.title("DAY-AHEAD PRICE COVERAGE")

st.markdown(STYLE, unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    target_date = st.date_input("Delivery day", value=dt.date.today() + dt.timedelta(days=1))
    
    coverage = build_source_coverage(target_date)
    covered_zones = coverage["bidding_zone"].nunique() if not coverage.empty else 0

with col2:
    st.metric("Zones covered", f"{covered_zones}/{len(IN_SCOPE_ZONES)}")


if coverage.empty:
    st.warning("No DAY_AHEAD rows found for this delivery day yet.")
else:
    for source in sorted(coverage["source"].unique()):
        st.markdown(render_source_section(source, coverage[coverage["source"] == source]), unsafe_allow_html=True)

missing = sorted(set(IN_SCOPE_ZONES) - set(coverage["bidding_zone"].unique())) if not coverage.empty else IN_SCOPE_ZONES
if missing:
    st.subheader(f"Zones with zero sources ({len(missing)})")
    chips = "".join(_chip("critical", zone) for zone in missing)
    st.markdown(f'<div class="chip-row">{chips}</div>', unsafe_allow_html=True)
