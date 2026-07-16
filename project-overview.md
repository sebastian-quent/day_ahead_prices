# European Day-Ahead Price Scrapers

## Goal

Scrape day-ahead (and later intraday) electricity prices for all European bidding zones into a single Postgres table, replacing per-source formats scattered across scrapers.

Redundancy requirement: at least **two independent sources per bidding zone**, so a single source outage doesn't create a data gap.

## Scope

- In scope: day-ahead auction prices (`DAY_AHEAD`), all bidding zones listed below, historical backfill, Prefect-scheduled runs with logging.
- Later, not now: intraday (`INTRADAY`) scrapers. Schema already supports it (see Data model).
- Later, not now: direct NATS publishing alongside the DB write.
- Out of scope: anything not price-related (volumes, nominations, flows, imbalance prices) — stays in existing scraper setups.

## Architecture

- **Monorepo**, not one repo per scraper. A client (e.g. EPEX) can split into its own repo later if it grows enough to justify it.
- `core/` — shared library: DB dump/retrieve (`PriceStore`), logging setup, common utils. Used by every client. Moves to QUENT Core once tested and approved.
- `clients/<source>/` — one folder per data source (nordpool, epex, entsoe, cropex, ote, ...):
  - `client.py` — auth + HTTP request handling only, no parsing.
  - `config.py` — source-specific config/secrets.
  - `endpoints/<endpoint>.py` — fetch, parse, dump per endpoint. Also hosts the `@flow`-decorated `run()`, with backfill exposed via an optional date/range param.
- **Prefect**: `@flow` sits directly on each endpoint's `run()`. Logs go to Prefect so failures are visible without digging through server logs.
- **Storage**: writes directly to Postgres via `core.PriceStore`. Built so NATS publishing (matching the Empire pattern) can be added alongside the DB write later without restructuring.
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

`bidding_zone`/`product`/`market`/`source` capped at 20 chars, `currency` at 10 — may need extending eventually.

**`product` vs `market`**: `product` is a coarse filter, `market` is what actually disambiguates. `DAY_AHEAD` doesn't always mean SDAC — GB isn't part of SDAC at all, and AT has both SDAC and EXAA's early auction for the same delivery day. `market` covers auction codes (`SDAC`, `EXAA_EARLY`, `IDA1-3`) and intraday VWAP series (`ID1`, `ID3`, `FULL`) as open text, no enum. Revisit only if bad market codes actually become a problem.

**Resolution**: most zones have moved to 15-minute settlement, some are still 30 or 60. Stored as plain integer minutes, read per response — never hardcoded per zone, since a zone can change resolution over time.

`core.PriceStore.get()` collapses to the latest `forecasttime` per `valuetime`/zone/product/market/source, so consumers get the current price curve, not every scrape snapshot.

## Countries / sources / bidding zones

Tracks **implementation status**, not just source availability — ✓ means the zone is wired up and landing rows in `test.prices` today. Verified 2026-07-15 by reading each client's zone config directly (`clients/nordpool/config.py` + `day_ahead_gb.py`, `clients/epex/endpoints/day_ahead.py`'s `ZONE_FILE_CONFIG`, `clients/entsoe/config.py`).

Legend: **✓** implemented and landing data · **○** source could cover this zone but isn't built · **–** not applicable

| Country Code | Country Name   | Nordpool | EPEX | ENTSO-E | Local          | Live sources |
| ------------ | -------------- | :------: | :--: | :-----: | -------------- | :----------: |
| AT           | Austria        | ✓        | ✓    | ✓       | –              | 3            |
| BE           | Belgium        | ✓        | ✓    | ✓       | –              | 3            |
| BG           | Bulgaria       | ✓        | –    | ✓       | –              | 2            |
| HR           | Croatia        | –        | –    | ✓       | ○ CROPEX       | 1            |
| CZ           | Czech Republic | –        | –    | ✓       | ✓ OTE          | 2            |
| DE           | Germany        | ✓        | ✓    | ✓       | –              | 3            |
| DK1          | Denmark (West) | ✓        | ✓    | ✓       | –              | 3            |
| DK2          | Denmark (East) | ✓        | ✓    | ✓       | –              | 3            |
| EE           | Estonia        | ✓        | –    | ✓       | –              | 2            |
| ES           | Spain          | –        | –    | ✓       | ✓ OMIE         | 2            |
| FI           | Finland        | ✓        | ✓    | ✓       | –              | 3            |
| FR           | France         | ✓        | ✓    | ✓       | –              | 3            |
| GR           | Greece         | –        | –    | ✓       | ✓ ENEX         | 2            |
| HU           | Hungary        | –        | –    | ✓       | ○ HUPX         | 1            |
| IE           | Ireland        | –        | –    | ✓       | ✓ SEMO         | 2            |
| IT           | Italy          | –        | –    | ○*      | ○ GME          | 0            |
| LT           | Lithuania      | ✓        | –    | ✓       | –              | 2            |
| LV           | Latvia         | ✓        | –    | ✓       | –              | 2            |
| NL           | Netherlands    | ✓        | ✓    | ✓       | –              | 3            |
| NO1          | Norway 1       | ✓        | ✓    | ✓       | –              | 3            |
| NO2          | Norway 2       | ✓        | ✓    | ✓       | –              | 3            |
| NO3          | Norway 3       | ✓        | ✓    | ✓       | –              | 3            |
| NO4          | Norway 4       | ✓        | ✓    | ✓       | –              | 3            |
| NO5          | Norway 5       | ✓        | ✓    | ✓       | –              | 3            |
| PL           | Poland         | ✓        | ✓    | ✓       | –              | 3            |
| PT           | Portugal       | –        | –    | ✓       | ✓ OMIE         | 2            |
| RO           | Romania        | –        | –    | ✓       | ✓ OPCOM        | 2            |
| SE1          | Sweden 1       | ✓        | ✓    | ✓       | –              | 3            |
| SE2          | Sweden 2       | ✓        | ✓    | ✓       | –              | 3            |
| SE3          | Sweden 3       | ✓        | ✓    | ✓       | –              | 3            |
| SE4          | Sweden 4       | ✓        | ✓    | ✓       | –              | 3            |
| SI           | Slovenia       | –        | –    | ✓       | ○ BSP Southpool| 1            |
| SK           | Slovakia       | –        | –    | ✓       | ✓ OKTE         | 2            |
| CH           | Switzerland    | –        | ✓    | ✓       | –              | 2            |
| GB           | Great Britain  | ✓        | ✓    | –       | –              | 2            |

\* IT's ENTSO-E gap is a mapping decision, not missing config — `clients/entsoe/config.py` excludes it because ENTSO-E splits Italy into ~7 price sub-zones with no single EIC matching our one `IT` bidding_zone.

GB has no ENTSO-E source; Nordpool + EPEX give it 2 live sources. GB isn't reachable via Nordpool's normal SDAC batch call — it runs under two separate Nord Pool markets instead, `N2EX_DayAhead` (hourly) and `GbHalfHour_DayAhead` (half-hourly), both on the same free/unauthenticated API host.

## To do

One entry per source, checked off once it's actually landing rows live (not just built/parse-verified). Cross-cutting items sit in their own group at the end.

**Core**
- [x] `PriceStore.dump()` / `.get()` — `core/price_store.py`, append-only change-detected writes. Targets `test.prices`; renaming to `prod.prices` at go-live is a one-line change.
- [x] `prod.prices` DDL — `db/migrations/0001_create_prices.sql`, matches the live `test.prices` schema; free-text columns, no FK/dimension tables (see Known gaps).
- [x] shared logging setup — `core/logging.py`, stdlib config only. **Not yet wired into Prefect** — see Known gaps.

**Nordpool**
- [x] client + day-ahead endpoint — `clients/nordpool/`, live to `test.prices`; full 22-zone `BIDDING_ZONE_TO_NORDPOOL_AREA` mapping active.
- [x] GB day-ahead endpoint — `clients/nordpool/endpoints/day_ahead_gb.py`. GB isn't part of Nordpool's SDAC batch call, so it's a separate endpoint hitting two Nord Pool markets (`N2EX_DayAhead` hourly + `GbHalfHour_DayAhead` half-hourly), landed as two `market` rows for `bidding_zone=GB`; currency read off the response. Verified live 2026-07-15: 96 rows written.
- [x] Smoke-tested 2025-11-15 (both endpoints) — failed `401 Unauthorized`; source-side rolling ~2-month window, not a parsing bug (see Known gaps).
- [x] DST reviewed statically (no local-time day-boundary reconstruction). Live verification deferred to **2026-10-24–2026-12-24**, once the fall-back day is inside the rolling window, or sooner with v2 access.
- [ ] Currency handling — `day_ahead.py` still hardcodes `CURRENCY = "EUR"` instead of reading it off the response (harmless, every zone it covers settles EUR). `day_ahead_gb.py` already reads it correctly since GB settles GBP.
- [ ] Migrate to Nord Pool's gated v2 data portal — **higher priority since 2026-07-15**: free API only serves ~2 months of history (`401` for older dates). Blocked on v2 access.

**EPEX**
- [x] client + day-ahead endpoint — `clients/epex/`, live to `test.prices`; `ZONE_FILE_CONFIG` covers 20 zones including GB (`market="N2EX"`, half-hourly) and DK2. GB verified live 2026-07-15: 48 rows.
- [x] Smoke-tested 2025-11-15: 1824 rows across 20 zones/markets. EPEX vs ENTSO-E DE spot-check: identical to the cent.
- [x] DST verified live: AT 2026-03-29 (92 rows) and 2025-10-26 (100 rows), gap/duplicate-free.

**ENTSO-E**
- [x] client + day-ahead endpoint — `clients/entsoe/`, live to `test.prices`; mapping covers 33 of 35 zones (all except GB and IT — IT excluded by ENTSO-E's ~7-way sub-zone split).
- [x] Smoke-tested 2025-11-15: 3024 rows across 33 zones.
- [x] DST verified live: DE 2026-03-29 (92 rows) and 2025-10-26 (100 rows), gap/duplicate-free.

**Nordpool + EPEX + ENTSO-E cross-check**
- [x] `@flow` decorator on all three endpoints' `run()`.
- [x] ≥2 sources per zone — **verified 2026-07-15 for all 24 zones** (GB was the last gap, closed by the Nordpool GB endpoint + EPEX's GB zone).
- [x] DST transition handling — **closed 2026-07-15**:
  - **ENTSO-E**: correct by construction — `_day_bounds_utc()` uses `pytz.localize()`, `num_positions` derived from the actual UTC span, not an assumed 24h.
  - **EPEX**: static `Hour 3A`/`Hour 3B` columns disambiguate fall-back via `ambiguous=True/False`; spring-forward hours are null and filtered out before conversion. Known non-issue: plain `Hour N` columns use `ambiguous="raise"` with no `nonexistent=` handling, uncaught by `fetch_and_parse` — harmless since EPEX always pre-splits ambiguous hours.
  - **Nordpool**: reviewed statically only (see above).
- [ ] Historical backfill for these 24 zones — deferred on purpose, pending review of current progress, so nothing gets backfilled twice.

**OTE (Czech Republic)**
- [x] client + day-ahead endpoint — `clients/ote/`, live to `test.prices`. SOAP via `zeep` (`PublicDataService` WSDL, `GetDamPricePeriodE`), single zone (CZ). Verified 2026-07-15: CZ/2025-11-15, 96 rows, matched ENTSO-E's CZ feed to the cent (confirms EUR — endpoint has no currency field). DST-verified 2026-03-29 (92 rows)/2025-10-26 (100 rows), gap-free. Data only available from **2025-10-01** (CZ 15-min go-live) — Production's `ote_api.py` has a stale comment claiming 2025-06-12, confirmed wrong live. Legacy hourly endpoint (`GetDamPriceE`) not wired up, needs CZK/EUR + hourly handling of its own, deferred with backfill.

**SEMO (Ireland)**
- [x] client + day-ahead endpoint — `clients/semo/`, live to `test.prices`. Lists/downloads SEMOpx static-reports (`DPuG_ID=EA-001`), filtered to `MarketResult_SEM-DA_*`. Single zone (IE) — only `ROI-DA` parsed (`NI-DA` is byte-identical but out of scope). Verified 2026-07-16: IE/2026-07-15, 48 rows; averages to ENTSO-E's hourly IE prices, confirming parsing + EUR assumption. DST-verified 2026-03-29 (46 rows)/2025-10-26 (50 rows) — no special handling needed, SEMO timestamps already carry explicit UTC `Z`. SEM-DA is a **D+1 auction** with delivery day on CET/CEST boundaries, not Irish time — `fetch_day_ahead_documents()` queries one day earlier to account for this. Listing only retains ~12 months of documents (confirmed back to 2020) — same rolling-window limit as Nordpool.

**OPCOM (Romania)**
- [x] client + day-ahead endpoint — `clients/opcom/`, live to `test.prices`. XML export from opcom.ro's report page, no auth wall beyond a User-Agent check (WAF blocked default `python-requests` UA — fixed with a static `Mozilla/5.0` header). Single zone (RO), no per-row timestamp — `valuetime` derived from 1-based `Pos` vs. the true UTC day span (same approach as ENTSO-E/OTE/OMIE). Dates with no report return HTTP 200 with an empty `<resultset/>`. History goes back to at least 2015-01-01 (not fully bisected). Delivery-day boundary confirmed CET/CEST, not RO's own EET/EEST — cross-checked against ENTSO-E. Currency hardcoded EUR (no field to read). DST-verified 2026-03-29 (92 rows)/2025-10-26 (100 rows). Dump test passed 2026-07-16: 96 rows for 2026-07-16.

**OMIE (Spain / Portugal)**
- [x] client + day-ahead endpoint — `clients/omie/`, live to `test.prices`. No API — daily flat files on a Drupal file-browser, one file covers both ES and PT (joint MIBEL auction). `list_files()` scrapes the listing to resolve the current-version filename per date (corrected files get incremented suffixes). Forecasttime from file mtime. Resolution derived per-file; verified across the hourly era, the 15-min go-live, and both DST transitions (100/92 rows). ES and PT price columns aren't always identical — diverge during interconnector congestion, so both are parsed as distinct rows. Delivery-day boundary uses `Europe/Madrid` for both zones — cross-checked 2026-07-16 against ENTSO-E, 96/96 match. Pre-2023 history exists as yearly zip archives, not wired up. Dump test passed 2026-07-16: 192 rows (96 ES + 96 PT) for 2026-07-16.

**CROPEX (Croatia)**
- [ ] Client not started — likely paid API access, unconfirmed (see Known gaps); blocked.

**HUPX (Hungary)**
- [ ] Client not started — paid "HUPX Labs" API, unconfirmed access (see Known gaps); blocked. HUPX Labs also bundles BSP Southpool (SI) and SEEPEX (RS) data — could unlock a second SI source too.

**OKTE (Slovakia)**
- [x] client + day-ahead endpoint — `clients/okte/`, live to `test.prices`. Public unauthenticated REST API (`isot.okte.sk/api/v1/dam/results`), no WAF issue. Single zone (SK). Response timestamps are already full UTC ISO-8601, so no local-time boundary math is needed. Verified across the 15-min era, hourly era (back to 2010), and both DST transitions (92/100 rows). One request accepts a full date range, so `fetch_day_ahead_prices()` does a single bulk call per run, unlike OPCOM/OMIE's per-day loop. Currency hardcoded EUR (no field). Cross-checked 2026-07-16 against ENTSO-E's SK feed: 96/96 match; also confirmed SK/CZ/HU/RO clear on the same 4M Market Coupling price. Dump test passed 2026-07-16: 96 rows for 2026-07-16.

**ENEX (Greece)**
- [x] client + day-ahead endpoint — `clients/enex/`, live to `test.prices`. HEnEx's EL-DAM results xlsx on a Liferay page, no auth wall. Single zone (GR); targets the "Results" portlet specifically (instance `6eBaUXF5VIb7`). Listing ignores `_delta=`, so `list_files()` paginates via `_cur=1,2,3,...`.
- [x] Results sheet repeats each period's MCP once per breakdown row (exports, load, generation mix, ...) — `parse_response` dedupes to one row per `SORT` position. `DELIVERY_DURATION` read per row, not hardcoded.
- [x] Delivery-day boundary confirmed CET/CEST, not Greece's own EET/EEST — cross-checked all 96 quarter-hours of 2026-07-16 against ENTSO-E's GR feed, exact match. `valuetime` reconstructed from the 1-based `SORT` position rather than parsing the ambiguous wall-clock column directly.
- [x] Currency hardcoded EUR (no field; confirmed via the ENTSO-E cross-check).
- [x] `forecasttime` uses `utcnow()` fallback — no reliably-timezoned native publish timestamp.
- [x] DST spring-forward verified 2026-07-16 against 2026-03-29 (92 rows). Fall-back not verifiable yet — listing only retains documents back to **2026-01-01**, so 2025-10-26 is out of range; revisit after 2026-10-25.
- [x] Dump test passed 2026-07-16: 96 rows for 2026-07-16.

**GME / Southpool**
- [ ] Not started. GME (Italy) is free but needs paperwork/registration — lower barrier than CROPEX/HUPX. Southpool (SI) only reachable via the HUPX Labs bundle — no standalone source identified yet.

**Cross-cutting / not scoped to one source**
- [ ] Intraday scrapers (IDA1-3 auctions, ID1/ID3/FULL VWAPs) — schema already supports this via `market`.
- [ ] NATS publish alongside DB write.
- [ ] Market code reference/lookup table — only if free-text `market` values start causing problems; `id-tables-design.drawio` sketches an FK-based alternative.
- [ ] Fuller normalized table design (`id-tables-design.drawio`) — standing idea for a `dim_bidding_zone`/`dim_product`/`dim_market`/`dim_source` model with FKs into `prod.prices`, independent of the typo problem. No plan yet.
- [ ] Day-ahead volumes alongside prices — needs a schema decision (extend `prod.prices` vs. separate table); currently out of scope.
- [ ] Dependency manifest — no `requirements.txt`/`pyproject.toml`; `prefect`, `pandas`, `sqlalchemy`, `paramiko`, `xmltodict`, `pytz`, `requests`, `zeep`, `openpyxl`, `quent_core` all unpinned.
- [ ] Consistent retry policy — EPEX retries once on a dropped SFTP connection; Nordpool and ENTSO-E fail immediately with no retry.
- [ ] Failure alerting — nothing notifies anyone on a failed run beyond the log line; consider a Prefect automation.

## Known gaps / architecture review (2026-07-15)

Findings from a full pass over the current code and docs. Flagged rather than silently fixed, since several involve a decision the team should make.

**Nordpool's free API only serves a rolling ~2-month window, not full history.** `DayAheadPrices` returns `200` for recent dates and `401` for anything older (confirmed via direct `curl`, independent of this repo's code). Affects both `day_ahead.py` and `day_ahead_gb.py`; EPEX and ENTSO-E are unaffected. Raises priority of the v2-portal migration to-do; historical backfill will land with only 2 of 3 sources for older dates unless v2 access is obtained first.

**~~Nordpool zone config scoped to one zone.~~ Resolved** — full 22-zone `BIDDING_ZONE_TO_NORDPOOL_AREA` mapping active, unblocking 2-source coverage for BG, DK2, EE, LT, LV. GB isn't in this mapping and never will be — see the GB entry above instead.

**~~EPEX zone coverage partial.~~ Resolved** — `ZONE_FILE_CONFIG` now covers 20 zones, including GB and DK2.

**~~Real 2-source coverage lower than the checklist implied.~~ Resolved 2026-07-15** — re-verified from each client's zone config directly: all 24 iteration-1 zones now have ≥2 live sources. Only IT (0 sources, iteration 3) remains below target project-wide.

**Flow logs aren't wired into Prefect.** All three endpoints still use `logging.getLogger(__name__)`/`setup_logging()` instead of `prefect.logging.get_run_logger()` — `@flow` is on `run()`, but log lines won't show as flow-run logs in the Prefect UI. Needs a decision: switch to `get_run_logger()`, add a stdout-forwarding handler, or accept incomplete UI logs.

**No deployment/schedule exists yet — out of scope for this repo.** `@flow` alone doesn't run anything on a cadence; no `prefect.yaml`, `flow.serve()`, `flow.deploy()`, or work pool. Decided 2026-07-15: scheduling gets set up once this code lands in production infra, by whoever operates that.

**~~No DDL checked into the repo.~~ Resolved 2026-07-15** — `db/migrations/0001_create_prices.sql` matches the live `test.prices` schema.

**`id-tables-design.drawio` is archived, not a pending plan.** Sketches `dim_bidding_zone`/`dim_product`/`dim_market`/`dim_source` tables with FKs, to stop typos like `"day ahead"` vs `"DAY_AHEAD"` landing silently. Decided 2026-07-15: keep free text (see Resolved Decisions); diagram kept only as a future idea if bad `market` values become a real problem.

**Inconsistent retry behavior across clients.** EPEX retries once on a dropped SFTP connection; Nordpool and ENTSO-E fail immediately on any request exception. Tolerable today since `dump()`'s change-detection makes rescrapes cheap, but worth a deliberate policy once these run unattended.

**No dependency manifest.** No `requirements.txt`/`pyproject.toml` — `prefect`, `pandas`, `sqlalchemy`, `paramiko`, `xmltodict`, `pytz`, `requests`, `zeep`, and `quent_core` are all unpinned. Not urgent solo, but will matter once deployed to a different machine.

**Several remaining local providers are gated behind paid or paperwork-based access — flagged 2026-07-16, unresolved:**
- **HUPX (Hungary)** — paid "HUPX Labs" API, access unconfirmed. Also bundles BSP Southpool (SI) and SEEPEX (RS) — could unlock a second SI source too (RS isn't in scope).
- **CROPEX (Croatia)** — likely paid, same unconfirmed status.
- **GME (Italy)** — free but needs paperwork/registration. Lower barrier than HUPX/CROPEX.
- **OPCOM (Romania)** — no auth wall found so far, picked up first for that reason. Worth re-confirming once the client is built, in case automated (non-browser) access needs auth.

## Lessons from the existing `ImbalancePriceHandler` pattern

Not carried forward as-is into `core.PriceStore` — noting why, so the divergence is intentional:

- **Naming**: `<Domain>Handler` doesn't generalize past one table. `PriceStore` (or `<Domain>Store`) reads clearer.
- **`self.engine = engine or engine`** — a no-op fallback (dead code). `PriceStore` takes the shared engine as a constructor arg instead.
- **Dedup via correlated `NOT EXISTS`, one row at a time via `executemany`** — doesn't batch, conflicts with "pipelines should keep running on partial failure."
- **No chunking** — one `conn.execute()` for the whole DataFrame means one failing row fails the entire batch.
- **`get_imb_prices` returns `valuetime` as the index** — harder to concat/merge across stores later. Keeping it a plain column is more composable.

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
