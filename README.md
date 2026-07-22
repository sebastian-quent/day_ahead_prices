# day-ahead-prices

Scrapers that collect European day-ahead electricity prices from multiple
sources and land them in a single, consistent Postgres table, so trading
tooling has one place to query instead of per-source formats.

Each bidding zone is covered by at least two independent sources for
redundancy. Intraday prices are out of scope for now but the schema already
supports them.

## Layout

- `core/` - shared `PriceStore` (dump/retrieve, plus publish to `quent-data-stream`), logging, utils
- `clients/<source>/client.py` - auth + generic request function for that source
- `clients/<source>/endpoints/<name>.py` - fetch, parse, dump, `@flow`-decorated `run()`
- `monitoring/` - `day_ahead_completeness.py` (Prefect flow, zone-level data-completeness
  check, separate from flow health) and `coverage.py` (Streamlit dashboard, per-source
  coverage for a given delivery day)
- `db/migrations/` - DDL for `prod.prices`
- `scripts/` - one-off backfill/verification drivers, not scheduled

Every row `PriceStore.dump()` writes is also published to `quent-data-stream` (NATS
JetStream, stream `PRICES`) - see `project-overview.md` for details. `publish=False`
disables this per instance or per call, e.g. for backfills.

## Sources

Live and landing rows in `prod.prices`:

- **Nordpool** - all zones except GB's batch call, plus a separate GB endpoint (`N2EX_DayAhead` + `GbHalfHour_DayAhead`); free API only serves a rolling ~2-month history
- **EPEX** - 20 zones incl. GB and DK2
- **ENTSO-E** - 34 of 35 zones (GB excluded, see `project-overview.md`)
- **OTE** (Czech Republic) - CZ, SOAP/zeep
- **SEMO** (Ireland) - IE
- **OPCOM** (Romania) - RO
- **OMIE** (Spain/Portugal) - ES, PT (joint MIBEL auction)
- **ENEX** (Greece) - GR
- **OKTE** (Slovakia) - SK

Not started: CROPEX (HR), HUPX (HU), GME (IT), BSP Southpool (SI) - all gated
behind paid access, see `project-overview.md`.

31 of 35 in-scope zones have ≥2 live sources. HR, HU and SI are still on a
single source (their local scraper isn't built yet); IT also has just one
(ENTSO-E, split into 7 bidding-zone rows - GME would be its second, not built).

## Data

Target table: `prod.prices`, keyed on
`valuetime, forecasttime, bidding_zone, market_type, market, source`. See
`project-overview.md` for the full schema and column descriptions.

## Dependencies

Poetry-managed (`pyproject.toml`/`poetry.lock`), own independent venv - not
merged into Production's, see `project-overview.md`.

## Status

Historical backfill to 2024-01-01 is done and verified (day-by-day gap scan,
not just MIN/MAX per zone) for every zone that can reach that far back;
Nordpool, OTE, SEMO and ENEX are floor-limited by source-side retention
windows instead. No Prefect deployment/schedule is wired up yet - see
`project-overview.md` for full scope, architecture, current implementation
status per zone, and the iteration/to-do list.
