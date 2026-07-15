# European Day-Ahead Price Scrapers

## Goal

Scrape day-ahead (and later intraday) electricity prices for "all" European bidding zones, and land them in Postgres in a single, consistent table so trading tooling has one place to query from — instead of per-source formats scattered across scrapers.

Redundancy requirement: at least **two independent sources per bidding zone**, so a single source outage doesn't create a data gap.

## Scope

- In scope: day-ahead auction prices (`DAY_AHEAD`), all bidding zones listed below, historical backfill capability, Prefect-scheduled runs with logging.
- In scope later, not now: intraday (`INTRADAY`) scrapers. The schema already supports it (see Data model) so nothing needs to change when that work starts.
- In scope later, not now: direct NATS publishing alongside the DB write.
- Out of scope for this project: anything not price-related (e.g. volumes, nominations, flows, imbalance prices) — those stay in their existing scraper setups.

## Architecture

- **Monorepo**, not one repo per scraper. Structured so a client (e.g. EPEX) can be split into its own repo later if it grows enough to justify it.
- `core/` — shared library: DB dump/retrieve (`PriceStore`), logging setup, common utils. Used by every client. Will be added to QUENT Core lib once tested and approved.
- `clients/<source>/` — one folder per data source (nordpool, epex, entsoe, cropex, ote, ...). Each has:
  - `client.py` — auth + HTTP request handling only, no parsing logic.
  - `config.py` — source-specific config/secrets.
  - `endpoints/<endpoint>.py` — one file per endpoint: fetch, parse, dump. Also where `@flow`-decorated `run()` lives, and where historical backfill is exposed via an optional date/range param on `run()`.
- **Prefect**: `@flow` decorator sits directly on each endpoint's `run()`. Logs go to Prefect so failures are visible without digging through server logs.
- **Storage**: write directly to Postgres via `core.PriceStore` for now. Built so publishing to NATS (matching the existing Empire pattern) can be added alongside the DB write later without restructuring.
- **DB engine**: `from Database.db_connect import engine` — same shared engine as `ImbalancePriceHandler`. `core.PriceStore` takes this as a constructor arg rather than building its own connection.

## Data model

Table: **`prod.prices`**.

| Column       | Type              | PK  | Not Null | Description                                               |
| ------------ | ----------------- | :-: | :------: | ----------------------------------------------------------- |
| valuetime    | `timestamptz`     |  ✓  |    ✓     | Start of delivery period (UTC)                              |
| forecasttime | `timestamptz`     |  ✓  |    ✓     | Timestamp when the data was scraped (UTC)                   |
| bidding_zone | `varchar(20)`     |  ✓  |    ✓     | Delivery area (DE, DK1, NO2, GB, ...)                        |
| product      | `varchar(20)`     |  ✓  |    ✓     | Coarse bucket: `DAY_AHEAD` or `INTRADAY`                     |
| market       | `varchar(20)`     |  ✓  |    ✓     | The actual price series identity (see below)                |
| source       | `varchar(20)`     |  ✓  |    ✓     | Data source (`EPEX`, `Nord Pool`, `ENTSO-E`, `EXAA`, ...)    |
| resolution   | `smallint`        |     |    ✓     | Delivery resolution in minutes (`60`, `30`, `15`)            |
| currency     | `varchar(10)`     |     |    ✓     | Native currency (`EUR`, `GBP`, `CHF`, `NOK`, ...)            |
| price        | `numeric(10,2)`   |     |    ✓     | Market clearing price / VWAP                                |

`bidding_zone`/`product`/`market`/`source` are capped at 20 chars, `currency` at 10 - might needs to get extended eventually.

**`product` vs `market`**: `product` is a coarse filter, `market` is what actually disambiguates. `DAY_AHEAD` does not always mean SDAC — GB isn't part of SDAC at all, and AT has both SDAC and EXAA's early auction for the same delivery day. `market` covers auction codes (`SDAC`, `EXAA_EARLY`, `IDA1`, `IDA2`, `IDA3`) and intraday VWAP series (`ID1`, `ID3`, `FULL`) under the same column, treated as open text — no enum or lookup table for now. Revisit only if invalid/misspelled market codes actually become a problem.

**Resolution note**: most bidding zones have moved to 15-minute settlement, but a number are still on 30 or 60 minutes. `resolution` is stored as plain integer minutes (not an ISO 8601 duration string) and must be read per-response, not hardcoded per zone or globally — a zone can also change resolution over time, so historical rows and current rows for the same zone may legitimately differ.

`core.PriceStore.get()` collapses to the latest `forecasttime` per `valuetime`/zone/product/market/source by default, so consumers get the current known price curve rather than every scrape snapshot.

## Countries / sources / bidding zones

This table tracks **implementation status**, not just source availability — ✓ means the zone is actually wired up and landing rows in `test.prices` today, not just that the source could theoretically cover it. Verified 2026-07-15 by reading each client's zone config directly (`clients/nordpool/config.py` + `day_ahead_gb.py`, `clients/epex/endpoints/day_ahead.py`'s `ZONE_FILE_CONFIG`, `clients/entsoe/config.py`), not from memory of the old table.

Legend: **✓** implemented and landing data · **○** source could cover this zone but isn't built yet (local provider not started, or a specific technical blocker) · **–** not applicable, no source

| Country Code | Country Name   | Nordpool | EPEX | ENTSO-E | Local          | Live sources | Iteration |
| ------------ | -------------- | :------: | :--: | :-----: | -------------- | :----------: | :-------: |
| AT           | Austria        | ✓        | ✓    | ✓       | –              | 3            | 1         |
| BE           | Belgium        | ✓        | ✓    | ✓       | –              | 3            | 1         |
| BG           | Bulgaria       | ✓        | –    | ✓       | –              | 2            | 1         |
| HR           | Croatia        | –        | –    | ✓       | ○ CROPEX       | 1            | 2         |
| CZ           | Czech Republic | –        | –    | ✓       | ○ OTE          | 1            | 2         |
| DE           | Germany        | ✓        | ✓    | ✓       | –              | 3            | 1         |
| DK1          | Denmark (West) | ✓        | ✓    | ✓       | –              | 3            | 1         |
| DK2          | Denmark (East) | ✓        | ✓    | ✓       | –              | 3            | 1         |
| EE           | Estonia        | ✓        | –    | ✓       | –              | 2            | 1         |
| ES           | Spain          | –        | –    | ✓       | ○ OMIE         | 1            | 3         |
| FI           | Finland        | ✓        | ✓    | ✓       | –              | 3            | 1         |
| FR           | France         | ✓        | ✓    | ✓       | –              | 3            | 1         |
| GR           | Greece         | –        | –    | ✓       | ○ ENEX         | 1            | 3         |
| HU           | Hungary        | –        | –    | ✓       | ○ HUPX         | 1            | 2         |
| IE           | Ireland        | –        | –    | ✓       | ○ SEMO         | 1            | 2         |
| IT           | Italy          | –        | –    | ○*      | ○ GME          | 0            | 3         |
| LT           | Lithuania      | ✓        | –    | ✓       | –              | 2            | 1         |
| LV           | Latvia         | ✓        | –    | ✓       | –              | 2            | 1         |
| NL           | Netherlands    | ✓        | ✓    | ✓       | –              | 3            | 1         |
| NO1          | Norway 1       | ✓        | ✓    | ✓       | –              | 3            | 1         |
| NO2          | Norway 2       | ✓        | ✓    | ✓       | –              | 3            | 1         |
| NO3          | Norway 3       | ✓        | ✓    | ✓       | –              | 3            | 1         |
| NO4          | Norway 4       | ✓        | ✓    | ✓       | –              | 3            | 1         |
| NO5          | Norway 5       | ✓        | ✓    | ✓       | –              | 3            | 1         |
| PL           | Poland         | ✓        | ✓    | ✓       | –              | 3            | 1         |
| PT           | Portugal       | –        | –    | ✓       | ○ OMIE         | 1            | 3         |
| RO           | Romania        | –        | –    | ✓       | ○ OPCOM        | 1            | 2         |
| SE1          | Sweden 1       | ✓        | ✓    | ✓       | –              | 3            | 1         |
| SE2          | Sweden 2       | ✓        | ✓    | ✓       | –              | 3            | 1         |
| SE3          | Sweden 3       | ✓        | ✓    | ✓       | –              | 3            | 1         |
| SE4          | Sweden 4       | ✓        | ✓    | ✓       | –              | 3            | 1         |
| SI           | Slovenia       | –        | –    | ✓       | ○ Southpool    | 1            | 3         |
| SK           | Slovakia       | –        | –    | ✓       | ○ OKTE         | 1            | 3         |
| CH           | Switzerland    | –        | ✓    | ✓       | –              | 2            | 1         |
| GB           | Great Britain  | ✓        | ✓    | –       | –              | 2            | 1         |

\* IT's ENTSO-E gap isn't "not built yet" like the others — `clients/entsoe/config.py` explicitly excludes it because ENTSO-E splits Italy into ~7 price sub-zones with no single EIC matching our one `IT` bidding_zone. Needs a mapping decision (which sub-zone(s) count as "IT"?), not just config work.

- **Iteration 1**: Nordpool, EPEX, ENTSO-E — 24 zones, all now at ≥2 live sources (verified 2026-07-15, see To do below).
- **Iteration 2**: local providers with straightforward APIs — CROPEX, OTE, HUPX, SEMO, OPCOM. All currently at 1 live source (ENTSO-E); building these adds the 2nd.
- **Iteration 3**: remaining local providers — OMIE (ES, PT), ENEX, GME, Southpool, OKTE. Same logic, lower priority / assume harder integrations. IT is the only zone with 0 live sources today.

GB confirmed as ENTSO-E-only gap (no GB data via ENTSO-E) — Nordpool + EPEX now both implemented, giving it 2 live sources. GB isn't reachable via Nordpool's normal SDAC `market=DayAhead` batch call (confirmed live: returns no data for `deliveryArea=GB`/`UK`) — it runs under two separate Nord Pool markets instead, `N2EX_DayAhead` (hourly) and `GbHalfHour_DayAhead` (half-hourly), both still on the same free/unauthenticated API host, no need for Nord Pool's gated v2 data portal.

## To do

**Core**
- [x] `PriceStore.dump()` / `.get()` — implemented in `core/price_store.py`, append-only change-detected writes; currently targets `test.prices`, a one-line rename to `prod.prices` at go-live
- [x] `prod.prices` DDL / migration — `db/migrations/0001_create_prices.sql`, matches the live `test.prices` schema; free-text columns, no FK/dimension tables (see Known gaps below)
- [x] shared logging setup — `core/logging.py`, stdlib config only; **not yet wired into Prefect** — see Known gaps below

**Iteration 1 (Nordpool, EPEX, ENTSO-E)**
- [x] Nordpool client + first endpoint — `clients/nordpool/`, dumps live to `test.prices`; full 22-zone `BIDDING_ZONE_TO_NORDPOOL_AREA` mapping active (the "scoped to AT only" gap noted below has been resolved)
- [x] Nordpool GB day-ahead endpoint — `clients/nordpool/endpoints/day_ahead_gb.py`; GB isn't part of Nordpool's SDAC batch call, so it's a separate endpoint hitting two distinct Nord Pool markets (`N2EX_DayAhead` hourly + `GbHalfHour_DayAhead` half-hourly) under `deliveryArea=UK`, landed as two `market` rows for `bidding_zone=GB`; currency read off the response rather than hardcoded. Verified live end-to-end 2026-07-15: 96 rows written to `test.prices`
- [x] EPEX client + first endpoint — `clients/epex/`, dumps live to `test.prices`; `ZONE_FILE_CONFIG` now covers 20 zones including GB (`market="N2EX"`, half-hourly) — the "6 of ~19 zones" gap noted below has been resolved. GB rows verified live 2026-07-15: 48 rows written to `test.prices`
- [x] ENTSO-E client + first endpoint — `clients/entsoe/`, dumps live to `test.prices`; `BIDDING_ZONE_TO_ENTSOE_AREA` covers 33 of 35 zones (every zone except GB and IT — IT excluded due to ENTSO-E's ~7-way sub-zone split, see Countries table above)
- [x] `@flow` decorator on all three endpoints' `run()`
- [x] cross-check: at least 2 sources landing per zone in iteration 1 — **verified 2026-07-15: true for all 24 iteration-1 zones** (see Countries table above for the per-zone breakdown; GB was the last one, closed by the two entries above)
- [x] smoke-test every implemented scraper (Nordpool incl. GB, EPEX, ENTSO-E — all zones and markets) against one historical past date — **done 2026-07-15, date tested: 2025-11-15, all rows written live to `test.prices`.** EPEX: 1824 rows across all 20 zones/markets. ENTSO-E: 3024 rows across all 33 zones. Nordpool (both the SDAC batch endpoint and the GB endpoint) failed with `401 Unauthorized` for this date — see new Known gaps entry below, this is a source-side access limit, not a parsing bug. Spot-checked EPEX vs ENTSO-E DE prices for the same hour: identical to the cent. No null prices, no naive timestamps, resolution/currency read correctly per zone.
- [ ] historical backfill run for iteration 1 zones — deferred, not started on purpose: holding off until current setup/progress has been presented, so nothing gets backfilled twice if the approach changes based on feedback.
- [x] verify DST transition handling (23-hour spring-forward / 25-hour fall-back days) across Nordpool, EPEX, and ENTSO-E scrapers — **closed 2026-07-15**, see breakdown below.
  - **ENTSO-E confirmed live**: live-tested DE against 2026-03-29 (spring forward, 92 rows @ 15-min) and 2025-10-26 (fall back, 100 rows @ 15-min) — `valuetime` sequence was gap-free, duplicate-free UTC in both cases. Correct by construction: `_day_bounds_utc()` uses `pytz.localize()` (not naive `replace(tzinfo=)`) to get the true UTC span for local midnight-to-midnight, and `num_positions` is derived from that actual UTC span rather than an assumed 24h, so ENTSO-E's per-day point count is picked up automatically. No code change needed for this source.
  - **EPEX confirmed live**: live-tested AT against the same two dates (92 rows / 100 rows @ 15-min, gap-free, duplicate-free). EPEX's CSVs carry static `Hour 3A`/`Hour 3B` columns year-round; both are populated only on the actual fall-back day, and `_convert_subhour_to_timestamp`'s `ambiguous=True/False` (day_ahead.py:95-97) correctly disambiguates them via pytz. On the spring-forward day both are all-null and get filtered out (day_ahead.py:157) before timestamp conversion ever runs, so the nonexistent local hour never reaches `tz_localize`. No code change needed for this source. Noted but not a live bug: plain `Hour N` columns use `ambiguous="raise"` with no `nonexistent=` handling, and pytz's `NonExistentTimeError`/`AmbiguousTimeError` aren't caught by `fetch_and_parse`'s `except (KeyError, ValueError)` — harmless only because EPEX always pre-splits the ambiguous hour into "3A"/"3B" rather than ever publishing an ambiguous/nonexistent plain hour.
  - **Nordpool reviewed statically, live verification deferred** — the free API's rolling ~2-month window (see Known gaps below) means neither the last DST day (2026-03-29) nor the next one (2026-10-25) is currently fetchable. Code review gives reasonable confidence without live data: unlike EPEX/ENTSO-E, `day_ahead.py`/`day_ahead_gb.py` never reconstruct local-time day boundaries themselves — `deliveryStart`/`deliveryEnd` come back from the API already as tz-aware UTC-convertible timestamps per entry, and `resolution_minutes` is derived per-entry from the actual delta, same pattern as ENTSO-E. There's no local-midnight arithmetic on our side to get wrong; the open question is purely whether Nord Pool's API itself returns the correct 92/100-count entry list for a transition day, which can't be confirmed until real data is reachable. Live verification deferred to the window **2026-10-24 through 2026-12-24** (once the 2026-10-25 fall-back day is published and still within the rolling window), or sooner if v2 portal access is obtained.

**Iteration 2**
- [ ] CROPEX, OTE, HUPX, SEMO, OPCOM clients

**Iteration 3**
- [ ] OMIE, ENEX, GME, Southpool, OKTE clients

**Later / not scoped yet**
- [ ] intraday scrapers (IDA1-3 auctions, ID1/ID3/FULL VWAPs) — schema already supports this via `market`
- [ ] NATS publish alongside DB write
- [ ] migrate `clients/nordpool/` from the current free/unauthenticated API to Nord Pool's gated v2 data portal — **now higher priority**: confirmed 2026-07-15 the free API only serves roughly the last two months of history (`DayAheadPrices` returns `401 Unauthorized` for dates older than that, e.g. 2025-11-15 fails while 2026-05-15 succeeds); Nordpool cannot backfill beyond that window at all today. Blocked on getting v2 access, revisit once granted.
- [ ] market code reference/lookup table — only if free-text `market` values start causing validation issues; `id-tables-design.drawio` sketches an FK-based alternative, archived as a future idea, see Known gaps below
- [ ] fuller normalized table design (`id-tables-design.drawio`) — the drawio sketch is more than just a fix for the market-typo problem above: it's a standing idea for a proper dimensional model (`dim_bidding_zone`/`dim_product`/`dim_market`/`dim_source` with FKs into `prod.prices`) that could be worth building for its own sake one day, not only as a reaction to bad data. No plan yet, keeping this bullet so the idea doesn't get lost even if the typo problem never materializes.
- [ ] day-ahead volumes alongside prices — could be an interesting future scope, no plan yet. Would need a schema decision (extend `prod.prices` vs a separate table) before any work starts; currently `volumes` is explicitly out of scope (see Scope section above)
- [ ] dependency manifest — no `requirements.txt`/`pyproject.toml` checked in; `prefect`, `pandas`, `sqlalchemy`, `paramiko`, `xmltodict`, `pytz`, `requests`, `quent_core` are all unpinned
- [ ] consistent retry policy across clients — `clients/epex/client.py` retries once on a dropped SFTP connection, `clients/nordpool/client.py` and `clients/entsoe/client.py` fail immediately on any request exception with no retry
- [ ] failure alerting — once flows are deployed/scheduled, nothing notifies anyone on a failed run beyond the log line; consider a Prefect automation/notification
- [ ] Nordpool currency handling — `clients/nordpool/endpoints/day_ahead.py` still hardcodes `CURRENCY = "EUR"` on every row instead of reading it off the response like EPEX/ENTSO-E do; harmless since every zone it covers settles in EUR. `day_ahead_gb.py` (new) reads currency off the response instead of repeating this, since GB settles in GBP — this to-do is only about the original SDAC endpoint, left unchanged for now

## Known gaps / architecture review (2026-07-15)

Findings from a full pass over the current code and docs. Flagging rather than silently fixing, since several involve a tradeoff or a decision the team should make.

**Nordpool's free API only serves a rolling ~2-month window, not full history.** Found while smoke-testing the historical spot-check above: `GET DayAheadPrices` returns `200` for dates within roughly the last two months (e.g. 2026-05-15) and `401 Unauthorized` for anything older (e.g. 2025-11-15, 2025-01-01) — confirmed via direct `curl` against the API, independent of this repo's code, so it's not a bug in `clients/nordpool/client.py`. This affects both `day_ahead.py` (SDAC batch) and `day_ahead_gb.py` (GB), since both hit the same host/endpoint. Practical effect: Nordpool cannot contribute to any historical backfill older than ~2 months; EPEX and ENTSO-E are unaffected and covered every iteration-1 zone fine for 2025-11-15. Raises the priority of the already-tracked "migrate to gated v2 portal" to-do above, and means the "historical backfill run for iteration 1 zones" to-do below will land with only 2 of 3 sources for older dates unless v2 access is obtained first.

**~~Nordpool zone config is scoped down to one zone.~~ Resolved** — `clients/nordpool/config.py`'s full 22-zone `BIDDING_ZONE_TO_NORDPOOL_AREA` mapping is active (no longer wrapped in a dead triple-quoted string). This unblocked 2-source coverage for BG, DK2, EE, LT, LV. GB is not in this mapping and never will be — it isn't part of Nordpool's SDAC batch call at all, see the GB entry below instead.

**~~EPEX zone coverage is partial.~~ Resolved** — `ZONE_FILE_CONFIG` in `clients/epex/endpoints/day_ahead.py` now covers 20 zones (including GB and DK2, both added since this gap was first flagged), not just the original 6.

**~~Real 2-source coverage today is much lower than the checklist implies.~~ Resolved 2026-07-15** — re-verified by reading each client's zone config directly rather than relying on this doc: all 24 iteration-1 zones now have ≥2 live sources (see the Countries table above). GB was the last gap, closed by adding both the Nordpool GB endpoint and EPEX's GB zone. The only zone left below the redundancy target for the *whole* project is IT (0 sources — iteration 3, not iteration 1), and iteration-2/3 zones are still at 1 source (ENTSO-E) each by design, pending their local-provider clients.

**Flow logs aren't wired into Prefect.** All three endpoints still call `logging.getLogger(__name__)` / `setup_logging()` rather than `prefect.logging.get_run_logger()`. `@flow` is now on `run()`, but log lines won't show up as flow-run logs in the Prefect UI — only wherever stdout is captured at the deployment layer. Worth deciding whether to switch to `get_run_logger()`, add a stdout-forwarding handler, or accept incomplete UI logs.

**No deployment/schedule exists yet — out of scope for this repo.** `@flow` alone doesn't run anything on a cadence — there's no `prefect.yaml`, `flow.serve()`, or `flow.deploy()` anywhere, and no work pool. Decided 2026-07-15: scheduling isn't this repo's responsibility — it'll be set up once this code lands in the production repo/infra, by whoever operates that. `@flow` decorators stay (so `run()` is deployment-ready), but no deployment config is being added here.

**~~No DDL checked into the repo.~~ Resolved 2026-07-15** — `db/migrations/0001_create_prices.sql` now matches the live `test.prices` schema.

**`id-tables-design.drawio` is archived — free text stands, not a pending plan.** The diagram sketches `dim_bidding_zone`/`dim_product`/`dim_market`/`dim_source` tables with FK constraints into `prod.prices`, to stop typos like `"day ahead"` vs `"DAY_AHEAD"` landing silently. Decided 2026-07-15: keep free text as documented in Resolved Decisions below; the diagram is kept only as a future idea to revisit if invalid/misspelled `market` values actually become a problem, not an in-progress design.

**Inconsistent retry behavior across clients.** `clients/epex/client.py` retries once on a dropped SFTP connection; `clients/nordpool/client.py` and `clients/entsoe/client.py` fail immediately on any `requests.RequestException`, relying entirely on the next scheduled run to recover. Tolerable today since `dump()`'s change-detection makes rescrapes cheap, but worth a deliberate policy once these run unattended on a schedule.

**No dependency manifest.** No `requirements.txt` or `pyproject.toml` in the repo — `prefect`, `pandas`, `sqlalchemy`, `paramiko`, `xmltodict`, `pytz`, `requests`, and the internal `quent_core` package are all unpinned. Not urgent solo, but will matter once this deploys to a Prefect work pool on a different machine.

## Lessons from the existing `ImbalancePriceHandler` pattern

Not being carried forward as-is into `core.PriceStore` — noting why, so the divergence is intentional rather than accidental:

- **Naming**: `<Domain>Handler` doesn't generalize well once there's a second table. `PriceStore` (or `<Domain>Store`) reads more clearly as "the thing that stores and retrieves X."
- **`self.engine = engine or engine`** — this is a no-op fallback (references itself), effectively dead code. `PriceStore` should take the shared `Database.db_connect.engine` explicitly as a constructor arg, same source, cleaner fallback.
- **Dedup via correlated `NOT EXISTS` subquery, one row at a time via `executemany`** — works, but doesn't batch, which conflicts with "pipelines should keep running on partial failure" below.
- **No chunking** — a single `conn.execute(sql_query, records)` for the whole DataFrame means one failing row fails the entire batch (full rollback), which conflicts with "pipelines should keep running on partial failure."
- **`get_imb_prices` returns `valuetime` as the index** — fine standalone, but makes it harder to concat/merge results across multiple stores/tables later. Keeping it as a plain column is more composable.

## Resolved decisions

| Question | Decision |
| --- | --- |
| DB engine source | `from Database.db_connect import engine` — same shared engine as `ImbalancePriceHandler` |
| Schema / table name | `prod.prices` |
| `product` vs `market` | `product` = coarse bucket (`DAY_AHEAD`/`INTRADAY`); `market` = actual price series, free text, no enum |
| ENTSO-E gap for GB | Confirmed correct, no action needed |
| Repo name | Proposed: `day-ahead-prices`, awaiting creation |
| Rescrape dedup strategy | Append-only, not upsert: `PriceStore.dump()` looks up the latest known price per key (one query per batch, not per row) and inserts a new row (new `forecasttime`) only when price actually changed; unchanged rescrapes are skipped. `forecasttime` therefore means "when this price last changed", not "when we last checked" — true for both source-native forecasttime (e.g. EPEX file mtime) and `utcnow()` fallback sources alike. `ON CONFLICT DO NOTHING` on the full PK is kept only as a safety net against exact re-inserts (e.g. a retried failed run), not as the change-detection mechanism. Comparison is price-only — resolution/currency changes alone don't trigger a new row. |
| Column types | `bidding_zone`/`product`/`market`/`source` are `varchar(20)`, `currency` is `varchar(10)` (not open `text`); `resolution` is `smallint` (minutes) rather than an ISO 8601 duration string like `PT60M`. All columns are `NOT NULL`. Verified against the draft DDL for the table. |
| Dimension tables / FKs (`id-tables-design.drawio`) | Not adopted (2026-07-15). Free-text columns stand per the `product` vs `market` decision above. Diagram kept only as a future idea to revisit if invalid/misspelled `market` values actually cause problems. |
| GB day-ahead via Nord Pool | Land both `N2EX_DayAhead` (hourly) and `GbHalfHour_DayAhead` (half-hourly) as separate `market` rows for `bidding_zone=GB` — same one-zone-two-markets pattern as AT's `SDAC`/`EXAA_EARLY`, not a choice between them. |
