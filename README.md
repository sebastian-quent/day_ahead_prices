# day-ahead-prices

Scrapers that collect European day-ahead electricity prices from multiple
sources and land them in a single, consistent Postgres table, so trading
tooling has one place to query instead of per-source formats.

Each bidding zone is covered by at least two independent sources for
redundancy. Intraday prices are out of scope for now but the schema already
supports them.

## Layout

- `core/` - shared `PriceStore` (dump/retrieve), logging, utils
- `clients/<source>/client.py` - auth + generic request function for that source
- `clients/<source>/endpoints/<name>.py` - fetch, parse, dump, `@flow`-decorated `run()`
- `monitoring/` - `day_ahead_completeness.py`, a separate Prefect flow that checks every
  in-scope zone landed data for a given delivery day (data completeness, not flow health)
- `db/migrations/` - DDL for `prod.prices`

## Sources

Live and landing rows in `prod.prices`:

- **Nordpool** - all zones except GB's batch call, plus a separate GB endpoint (`N2EX_DayAhead` + `GbHalfHour_DayAhead`); free API only serves a rolling ~2-month history
- **EPEX** - 20 zones incl. GB and DK2
- **ENTSO-E** - 33 of 35 zones (GB and IT excluded, see `project-overview.md`)
- **OTE** (Czech Republic) - CZ, SOAP/zeep
- **SEMO** (Ireland) - IE
- **OPCOM** (Romania) - RO
- **OMIE** (Spain/Portugal) - ES, PT (joint MIBEL auction)
- **ENEX** (Greece) - GR
- **OKTE** (Slovakia) - SK

Not started: CROPEX (HR), HUPX (HU), GME (IT), BSP Southpool (SI) - all gated
behind paid/paperwork access, see `project-overview.md`.

31 of 34 in-scope zones have ≥2 live sources. HR, HU and SI are still on a
single source (their local scraper isn't built yet); IT has zero (ENTSO-E
excludes it due to its ~7-way sub-zone split, no other source built yet).

## Data

Target table: `prod.prices`, keyed on
`valuetime, forecasttime, bidding_zone, product, market, source`. See
`project-overview.md` for the full schema and column descriptions.

## Dependencies

Poetry-managed (`pyproject.toml`/`poetry.lock`), own independent venv - not
merged into Production's, see `project-overview.md`.

## Status

See `project-overview.md` for full scope, architecture, current
implementation status per zone, and the iteration/to-do list.
