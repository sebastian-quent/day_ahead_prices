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

## Sources

- **Nordpool** - all zones except GB batch call, plus a separate GB endpoint
- **EPEX** - 20 zones incl. GB
- **ENTSO-E** - 33 of 35 zones (GB and IT excluded, see `project-overview.md`)

## Data

Target table: `prod.prices`, keyed on
`valuetime, forecasttime, bidding_zone, product, market, source`. See
`project-overview.md` for the full schema and column descriptions.

Until `prod.prices` is live, each endpoint's `dump()` writes a local CSV per
bidding zone instead, but is named/structured exactly as the eventual DB
write so swapping it in later is a one-line change.

## Status

See `project-overview.md` for full scope, architecture, current
implementation status per zone, and the iteration/to-do list.
