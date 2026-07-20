# European Day-Ahead Price Scrapers

## Goal

Scrape day-ahead (and later intraday) electricity prices for all European bidding zones into a single Postgres table, replacing per-source formats scattered across scrapers.

Redundancy requirement: at least **two independent sources per bidding zone**, so a single source outage doesn't create a data gap. This is about having a working fallback available, not running every source on an identical real-time schedule — day-to-day, a single primary source landing data per zone is sufficient; secondary sources are backup for when the primary is down (see Scheduling). This requirement is about live operation, not backfill — backfill only needs **one** source per zone/day (see Scope, and To do → Cross-cutting → Historical backfill).

## Scope

- In scope: day-ahead auction prices (`DAY_AHEAD`), all bidding zones listed below, historical backfill (2024-01-01 onward — see To do → Cross-cutting → Historical backfill for per-source readiness), Prefect-scheduled runs with logging.
- Later, not now: intraday (`INTRADAY`) scrapers. Schema already supports it (see Data model).
- Out of scope: anything not price-related (volumes, nominations, flows, imbalance prices) — stays in existing scraper setups.

## Architecture

- **Monorepo** (for now), not one repo per scraper. A client (e.g. EPEX) can split into its own repo later if it grows enough to justify it.
- `core/` — shared library: DB dump/retrieve (`PriceStore`), logging setup, common utils. Used by every client. Moves to QUENT Core once tested and approved.
- `clients/<source>/` — one folder per data source (nordpool, epex, entsoe, cropex, ote, ...):
  - `client.py` — auth + HTTP request handling only, no parsing.
  - `config.py` — source-specific config/secrets.
  - `endpoints/<endpoint>.py` — fetch, parse, dump per endpoint. Also hosts the `@flow`-decorated `run()`, with backfill exposed via an optional date/range param.
- **Prefect**: `@flow` sits directly on each endpoint's `run()`. Logs go to Prefect so failures are visible without digging through server logs.
- **Storage**: writes directly to Postgres via `core.PriceStore`, which also publishes every written row to `quent-data-stream` (see Streaming below).
- **DB engine**: `from Database.db_connect import engine` — same shared engine as `ImbalancePriceHandler`. `core.PriceStore` takes this as a constructor arg rather than building its own connection.

## Streaming (quent-data-stream)

`core/price_store.py` publishes every row `PriceStore.dump()` writes to Postgres onto a NATS stream, so the data is trivially accessible to any future consumer — how anything consumes it is deliberately not decided yet (out of scope, see Scope).

- **Producer only**: publishes via `quent_core.streaming`, doesn't touch the `quent-data-stream` gateway repo itself.
- **Stream**: own dedicated JetStream stream, named **`PRICES`**. Not scoped to "auction" or "day-ahead" in the name because `product`/`market` already carry the day-ahead-vs-intraday and auction-vs-continuous distinctions per-event, and `INTRADAY` will land on the same stream later. Known tradeoff, accepted: this data is **not** reachable via the gateway's `/replay`, `/ws`, `/hybrid` routes unless/until the gateway is separately updated to also watch `PRICES` — consumers need a direct NATS/JetStream connection for now. Created on first publish by `quent_core.streaming.ensure_stream()`, which only sets `name`+`subjects` (no explicit retention/duplicate_window/storage config) — so the stream runs on whatever `nats-py`/JetStream server defaults apply.
- **Subject**: one subject per `product`, not per source/zone — `prices.<product>.updates`, e.g. `prices.day_ahead.updates` today. `INTRADAY` gets its own subject automatically later, no code change needed. Consumers filter by zone/source/market client-side via the event's `data` fields. `NatsConfig.stream_subjects` is set to `prices.>` (a wildcard) precisely because `ensure_stream()` only applies the subjects list passed on first creation — a non-wildcard would have locked the stream to whichever product published first.
- **Connection**: same shared endpoint/certs as the Empire producer (`tls://192.168.1.202:4222`) — team-shared infra, not machine-specific.
- **Integration point**: centralized in `PriceStore.dump()`, not duplicated per endpoint — every `clients/*/endpoints/day_ahead*.py` file publishes "for free" with no changes of its own. `PriceStore(engine, publish=False)` (constructor) or `price_store.dump(df, publish=False)` (per call) disable it — e.g. for local/dev work or a historical backfill that shouldn't replay old rows onto the live stream. Only rows that actually committed to Postgres are published; a NATS outage is caught and logged, never fails the DB write.
- **Dedup key**: `core/price_store.py` defines its own `_build_msg_id()`, building the full natural key (`subject:valuetime:bidding_zone:product:market:source` — the same columns as `PriceStore.KEY_COLUMNS` plus `product`), because one shared subject per product means many rows (different zone/source/market) share subject+valuetime within the same `dump()` batch — anything coarser would collide under any nonzero `duplicate_window`. Publishing goes through `EventPublisher(nats_cfg, logger)` (`connect()` / `publish_many()` / `close()`), always passing this explicit `msg_id`.

## Data model

Table: **`prod.prices`**.

| Column       | Type              | PK  | Not Null | Description                                               |
| ------------ | ----------------- | :-: | :------: | ----------------------------------------------------------- |
| valuetime    | `timestamptz`     |  ✓  |    ✓     | Start of delivery period (UTC)                              |
| forecasttime | `timestamptz`     |  ✓  |    ✓     | Timestamp when the data was scraped (UTC)                   |
| bidding_zone | `varchar(20)`     |  ✓  |    ✓     | Delivery area (DE, DK1, NO2, GB, ...)                        |
| product      | `varchar(20)`     |  ✓  |    ✓     | Coarse bucket: `DAY_AHEAD` or `INTRADAY`                     |
| market       | `varchar(20)`     |  ✓  |    ✓     | The actual price series identity (see below)                |
| source       | `varchar(20)`     |  ✓  |    ✓     | Data source (`EPEX`, `Nord Pool`, `ENTSOE`, `EXAA`, ...)     |
| resolution   | `smallint`        |     |    ✓     | Delivery resolution in minutes (`60`, `30`, `15`)            |
| currency     | `varchar(10)`     |     |    ✓     | Native currency (`EUR`, `GBP`, `CHF`, `NOK`, ...)            |
| price        | `numeric(10,2)`   |     |    ✓     | Market clearing price / VWAP                                |

`bidding_zone`/`product`/`market`/`source` capped at 20 chars, `currency` at 10 — may need extending eventually.

**`product` vs `market`**: `product` is a coarse filter, `market` is what actually disambiguates. `DAY_AHEAD` doesn't always mean SDAC — GB isn't part of SDAC at all, and AT has both SDAC and EXAA's early auction for the same delivery day. `market` covers auction codes (`SDAC`, `EXAA_EARLY`, `IDA1-3`) and intraday VWAP series (`ID1`, `ID3`, `FULL`) as open text, no enum. Revisit only if bad market codes actually become a problem.

**Resolution**: most zones have moved to 15-minute settlement, some are still 30 or 60. Stored as plain integer minutes, read per response — never hardcoded per zone, since a zone can change resolution over time.

`core.PriceStore.get()` collapses to the latest `forecasttime` per `valuetime`/zone/product/market/source, so consumers get the current price curve, not every scrape snapshot.

**Dedup / rescrape strategy**: `PriceStore.dump()` is append-only, not upsert — it looks up the latest known price per key (one query per batch, not per row) and inserts a new row (new `forecasttime`) only when the price actually changed; unchanged rescrapes are skipped. `forecasttime` therefore means "when this price last changed", not "when we last checked" — true for both source-native forecasttime (e.g. EPEX file mtime) and `utcnow()` fallback sources alike. Comparison is price-only — resolution/currency changes alone don't trigger a new row. `ON CONFLICT DO NOTHING` on the full PK is kept only as a safety net against exact re-inserts (e.g. a retried failed run), not as the change-detection mechanism.

## Countries / sources / bidding zones

Tracks **implementation status**, not just source availability — ✓ means the zone is wired up and landing rows in `prod.prices` today.

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
| IT           | Italy          | –        | –    | ✓       | ○ GME          | 1            |
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

GB has no ENTSO-E source; Nordpool + EPEX give it 2 live sources. GB isn't reachable via Nordpool's normal SDAC batch call — it runs under two separate Nord Pool markets instead, `N2EX_DayAhead` (hourly) and `GbHalfHour_DayAhead` (half-hourly), both on the same free/unauthenticated API host.

IT has no single ENTSO-E area — ENTSO-E splits it into 7 price sub-zones, landed as 7 separate `bidding_zone` rows (`IT_NORD`, `IT_CNOR`, `IT_CSUD`, `IT_SUD`, `IT_SICI`, `IT_SARD`, `IT_CALA`, using ENTSO-E's own area naming) rather than one `IT` row. The `IT` row above rolls all 7 up for the country-level overview.

## To do

One entry per source, checked off once it's actually landing rows live (not just built/parse-verified). Cross-cutting items sit in their own group at the end.

**Core**
- [x] `PriceStore.dump()` / `.get()` — `core/price_store.py`, append-only change-detected writes. Targets `prod.prices`.
- [x] `prod.prices` DDL — `db/migrations/0001_create_prices.sql`, matches the live `prod.prices` schema; free-text columns, no FK/dimension tables (see Known gaps).
- [x] shared logging setup — `core/logging.py`, stdlib config only. Wired into the Prefect UI via `PREFECT_LOGGING_EXTRA_LOGGERS` — see Known gaps.

**Nordpool**
- [x] client + day-ahead endpoint — `clients/nordpool/`, live to `prod.prices`; full 22-zone `BIDDING_ZONE_TO_NORDPOOL_AREA` mapping active.
- [x] GB day-ahead endpoint — `clients/nordpool/endpoints/day_ahead_gb.py`. GB isn't part of Nordpool's SDAC batch call, so it's a separate endpoint hitting two Nord Pool markets (`N2EX_DayAhead` hourly + `GbHalfHour_DayAhead` half-hourly), landed as two `market` rows for `bidding_zone=GB`; currency read off the response.
- [x] Free API only serves ~2 months of history — `401` for older dates, source-side rolling window, not a parsing bug (see Known gaps).
- [x] DST reviewed statically (no local-time day-boundary reconstruction). Live verification deferred to **2026-10-24–2026-12-24**, once the fall-back day is inside the rolling window, or sooner with v2 access.
- [x] Currency handling — `day_ahead.py` now reads `currency` off the top-level `raw["currency"]` field (confirmed live against the API: present for the multi-area SDAC call, same as `day_ahead_gb.py` already did), instead of a hardcoded `CURRENCY = "EUR"` constant.
- [ ] Migrate to Nord Pool's gated v2 data portal — free API only serves ~2 months of history (`401` for older dates). Blocked on v2 access.

**EPEX**
- [x] client + day-ahead endpoint — `clients/epex/`, live to `prod.prices`; `ZONE_FILE_CONFIG` covers 19 zones (18 SDAC zones + CH) fetched by `run()`, plus GB (`market="Hourly"`/`"HalfHourly"`) fetched separately by `run_gb()`.
- [x] DST transition handling verified (see Nordpool + EPEX + ENTSO-E cross-check below).
- [x] `run_gb()` — second `@flow` in `day_ahead.py`, so GB (own N2EX-timed schedule, see Scheduling) doesn't ride along on `run()`'s SDAC-anchored schedule or vice versa. Calls the same `fetch_and_parse()`/`dump()` as `run()` with `bidding_zones=["GB"]` — no duplicated fetch/parse logic.
- [x] Historical resolution fallback — `fetch_day_ahead_file()` tries the configured resolution first, falls back to 60 min if that file doesn't exist (needed for pre-Oct-2025 zones now configured at 15 min). Confirmed live for AT/DE 2024-03-15.

**ENTSO-E**
- [x] client + day-ahead endpoint — `clients/entsoe/`, live to `prod.prices`; `BIDDING_ZONE_TO_ENTSOE_AREA` mapping covers 34 of 35 zones (all except GB) — IT is split into its own 7 ENTSO-E sub-zones (`IT_NORD`, `IT_CNOR`, `IT_CSUD`, `IT_SUD`, `IT_SICI`, `IT_SARD`, `IT_CALA`) rather than one `IT` entry, so the mapping has 40 dict entries in total. `run()` fetches 39 of those (excludes IE); IE fetched separately by `run_ie()`.
- [x] DST transition handling verified (see cross-check below).
- [x] `run_ie()` — second `@flow` in `day_ahead.py`, so IE (own SEM-DA-timed schedule, see Scheduling) doesn't ride along on `run()`'s SDAC-anchored schedule or vice versa. Calls the same `fetch_and_parse()`/`dump()` as `run()` with `bidding_zones=["IE"]` — no duplicated fetch/parse logic. `market` label resolved 2026-07-20 (see Known gaps): `fetch_and_parse()`/`parse_response()` now take an optional `market` param (default `MARKET`, still `"SDAC"` for `run()`'s real SDAC zones), and `run_ie()` passes the new `MARKET_IE = "SEM_DA"` constant — `clients/semo/endpoints/day_ahead.py` updated to match.

**Nordpool + EPEX + ENTSO-E cross-check**
- [x] `@flow` decorator on all three endpoints' `run()`.
- [x] ≥2 sources per zone confirmed for all 24 zones (GB was the last gap, closed by the Nordpool GB endpoint + EPEX's GB zone).
- [x] DST transition handling:
  - **ENTSO-E**: correct by construction — `_day_bounds_utc()` uses `pytz.localize()`, `num_positions` derived from the actual UTC span, not an assumed 24h.
  - **EPEX**: static `Hour 3A`/`Hour 3B` columns disambiguate fall-back via `ambiguous=True/False`; spring-forward hours are null and filtered out before conversion. Known non-issue: plain `Hour N` columns use `ambiguous="raise"` with no `nonexistent=` handling, uncaught by `fetch_and_parse` — harmless since EPEX always pre-splits ambiguous hours.
  - **Nordpool**: reviewed statically only (see above).
- [ ] Historical backfill for these 24 zones — scope/timing now tracked as one item covering all sources, see To do → Cross-cutting → Historical backfill.

**OTE (Czech Republic)**
- [x] client + day-ahead endpoint — `clients/ote/`, live to `prod.prices`. SOAP via `zeep` (`PublicDataService` WSDL, `GetDamPricePeriodE`), single zone (CZ). Matches ENTSO-E's CZ feed to the cent (confirms EUR — endpoint has no currency field). Data only available from **2025-10-01** (CZ 15-min go-live) — note Production's `ote_api.py` has a stale comment claiming 2025-06-12, confirmed wrong. Legacy hourly endpoint (`GetDamPriceE`) not wired up, needs CZK/EUR + hourly handling of its own, deferred with backfill.

**SEMO (Ireland)**
- [x] client + day-ahead endpoint — `clients/semo/`, live to `prod.prices`. Lists/downloads SEMOpx static-reports (`DPuG_ID=EA-001`), filtered to `MarketResult_SEM-DA_*`. Single zone (IE) — only `ROI-DA` parsed (`NI-DA` is byte-identical but out of scope). Averages to ENTSO-E's hourly IE prices, confirming parsing + EUR assumption. Timestamps already carry explicit UTC `Z`, so no DST handling needed. SEM-DA is a **D+1 auction** with delivery day on CET/CEST boundaries, not Irish time — `fetch_day_ahead_documents()` queries one day earlier to account for this. Listing only retains ~12 months of documents — same rolling-window limit as Nordpool.
- [x] SEMO's static-reports catalog batch-publishes every document at Irish midnight the day *after* its own `Date` field, confirmed directly via the API's `PublishTime` field (not previously exposed in this repo's parsing, found by querying the listing endpoint directly): e.g. a `SEM-DA` document with `Date=2026-07-19` (the delivery day) carries `PublishTime=2026-07-20T00:00`. This is one uniform rule, not a per-report-type quirk — it also explains `IDA2`/`IDA3`'s apparent "next-day" publish (their `Date` is the same-day trading date, not a delivery day one day later, so `PublishTime` lands only 1 day after `DateRetention` instead of 2). `run()` legitimately returns 0 rows for very recent delivery days, not a bug. Invalidated the Scheduling section's "results shortly after gate closure" assumption for `semo` (not for `entsoe.run_ie`, confirmed still correct — see Scheduling).
- [x] Scheduling fixed: `run()` now defaults to **yesterday's** delivery day (the newest one actually published as of any run day), not tomorrow's; cron moved from the old gate-closure-based `*/15 12-13 CET` to an Irish-midnight-anchored `5,20,35,50 1-2 CET/CEST` catch-up window. See Scheduling section and `clients/semo/endpoints/day_ahead.py`.

**OPCOM (Romania)**
- [x] client + day-ahead endpoint — `clients/opcom/`, live to `prod.prices`. XML export from opcom.ro's report page, no auth wall beyond a User-Agent check (WAF blocked default `python-requests` UA — fixed with a static `Mozilla/5.0` header). Single zone (RO), no per-row timestamp — `valuetime` derived from 1-based `Pos` vs. the true UTC day span (same approach as ENTSO-E/OTE/OMIE). Dates with no report return HTTP 200 with an empty `<resultset/>`. History goes back to at least 2015-01-01 (not fully bisected). Delivery-day boundary confirmed CET/CEST, not RO's own EET/EEST — cross-checked against ENTSO-E. Currency hardcoded EUR (no field to read).

**OMIE (Spain / Portugal)**
- [x] client + day-ahead endpoint — `clients/omie/`, live to `prod.prices`. No API — daily flat files on a Drupal file-browser, one file covers both ES and PT (joint MIBEL auction). `list_files()` scrapes the listing to resolve the current-version filename per date (corrected files get incremented suffixes). Forecasttime from file mtime. Resolution derived per-file. ES and PT price columns aren't always identical — diverge during interconnector congestion, so both are parsed as distinct rows. Delivery-day boundary uses `Europe/Madrid` for both zones — cross-checked against ENTSO-E. Pre-2023 history exists as yearly zip archives, not wired up.

**OKTE (Slovakia)**
- [x] client + day-ahead endpoint — `clients/okte/`, live to `prod.prices`. Public unauthenticated REST API (`isot.okte.sk/api/v1/dam/results`), no WAF issue. Single zone (SK). Response timestamps are already full UTC ISO-8601, so no local-time boundary math is needed. Data available back to 2010. One request accepts a full date range, so `fetch_day_ahead_prices()` does a single bulk call per run, unlike OPCOM/OMIE's per-day loop. Currency hardcoded EUR (no field). Cross-checked against ENTSO-E's SK feed; also confirmed SK/CZ/HU/RO clear on the same 4M Market Coupling price.

**ENEX (Greece)**
- [x] client + day-ahead endpoint — `clients/enex/`, live to `prod.prices`. HEnEx's EL-DAM results xlsx on a Liferay page, no auth wall. Single zone (GR); targets the "Results" portlet specifically (instance `6eBaUXF5VIb7`). Listing ignores `_delta=`, so `list_files()` paginates via `_cur=1,2,3,...`.
- [x] Results sheet repeats each period's MCP once per breakdown row (exports, load, generation mix, ...) — `parse_response` dedupes to one row per `SORT` position. `DELIVERY_DURATION` read per row, not hardcoded.
- [x] Delivery-day boundary confirmed CET/CEST, not Greece's own EET/EEST — cross-checked against ENTSO-E's GR feed. `valuetime` reconstructed from the 1-based `SORT` position rather than parsing the ambiguous wall-clock column directly.
- [x] Currency hardcoded EUR (no field; confirmed via the ENTSO-E cross-check).
- [x] `forecasttime` uses `utcnow()` fallback — no reliably-timezoned native publish timestamp.
- [x] Listing only retains documents back to **2026-01-01** — fall-back DST transition not verifiable until 2025-10-26 ages back into range; revisit after 2026-10-25.

**CROPEX (Croatia)**
- [ ] Client not started — blocked on paid/unconfirmed API access, see Known gaps.

**HUPX (Hungary)**
- [ ] Client not started — blocked on paid "HUPX Labs" API access, see Known gaps.

**GME (Italy)**
- [ ] Not started — would be IT's second live source; not blocked on access like HR/HU/SI, see Known gaps.

**BSP Southpool (Slovenia)**
- [ ] No standalone source — only reachable via the HUPX Labs bundle, see Known gaps.

**Cross-cutting / not scoped to one source**
- [ ] Historical backfill — start from **2024-01-01** wherever a source can reach that far back; only **one** source per zone/day is required (the ≥2-sources rule is live-operation outage insurance, not a backfill requirement — see Goal). Not started yet.
  - **Per-source floor** (can't reach 2024-01-01, source-side limit, not a bug): Nord Pool (~2-month rolling window), OTE (floor 2025-10-01, CZ 15-min go-live), SEMO (~12-month retention), ENEX (archive depth stops well before 2024). ENTSO-E, EPEX, OPCOM, OMIE, OKTE all confirmed reaching back to 2024-01-01.
  - **Resolution change (October 2025)**: many zones moved 60-min → 15-min settlement then, so backfilled 2024 rows must be labeled `resolution=60` for those zones, not the current value. ENTSO-E/OPCOM/OMIE/OKTE derive `resolution` dynamically per response, so this is safe by construction. EPEX's `ZONE_FILE_CONFIG` used to hardcode one static resolution per zone (breaking 2024 fetches for the 17 zones now at 15 min) — fixed: `fetch_day_ahead_file()` falls back to the 60-min file/path when the configured-resolution one doesn't exist.
- [x] Deactivate the per-zone CSV dump in every endpoint's `dump()` — commented out (not deleted) in all 10 `clients/*/endpoints/day_ahead*.py` files, kept available to uncomment for manual debugging/cross-checking.
- [ ] Intraday scrapers (IDA1-3 auctions, ID1/ID3/FULL VWAPs) — schema already supports this via `market`.
- [x] NATS publish alongside DB write — every `clients/*/endpoints/day_ahead*.py` file publishes automatically via `PriceStore.dump()` (see Streaming section above). Verified across single-source and multi-source batch runs with no `msg_id` collisions, including rows across different zones/sources sharing a subject and `valuetime`.
- [ ] Market code reference/lookup table — only if free-text `market` values start causing problems; `id-tables-design.drawio` sketches an FK-based alternative.
- [ ] Fuller normalized table design (`id-tables-design.drawio`) — standing idea for a `dim_bidding_zone`/`dim_product`/`dim_market`/`dim_source` model with FKs into `prod.prices`, independent of the typo problem. No plan yet.
- [ ] Day-ahead volumes alongside prices — needs a schema decision (extend `prod.prices` vs. separate table); currently out of scope.
- [x] Dependency manifest — `pyproject.toml`/`poetry.lock` added, Poetry-managed like Production. Shared deps pinned to match Production's `pyproject.toml`/`poetry.lock` exactly (`prefect` exact `3.4.14`, others same `~=` tilde ranges: `pandas`, `sqlalchemy`, `psycopg2`, `requests`, `paramiko`, `xmltodict`, `zeep`, `pytz`, `lxml`, `openpyxl`, `python-dateutil`). One deliberate deviation: `quent_core` pinned to git tag `v1.0.160-alpha.4`, not Production's `v1.0.158` — the `streaming` module this project depends on (`core/price_store.py`'s NATS publish) doesn't exist before `v1.0.160-alpha.1`. Kept independent (own `pyproject.toml`, `package-mode = false`, own `.venv`) rather than merged into Production's, so a future merge is a straightforward pin reconciliation, not a rewrite. Two additional explicit pins added 2026-07-20 to fix locally-broken transitive deps of `prefect==3.4.14` (see Known gaps): `importlib-metadata = ">=4.4"` and `fastapi = "~=0.115.6"`.
- [x] Consistent retry policy — every `clients/*/client.py` uses the same `RETRY_ATTEMPTS = 2` / `RETRY_BACKOFF_SECONDS = 10` pattern (one retry after a fixed backoff, then log and return `None` so callers skip/continue).
- [x] Failure alerting reframed as a data-completeness check, not a Prefect-run-failure check — see Monitoring section below. `monitoring/day_ahead_completeness.py` exists and is verified against the live DB; actual alert channel (email vs Teams) still undecided, so it currently only logs.

## Scheduling

Design only — nothing deployed yet (see Known gaps). Captured here because the grouping/catch-up/redundancy decisions are non-obvious and worth settling before wiring up Prefect deployments.

**Granularity**: one Prefect deployment per `@flow`-decorated function. Each flow processes only the zones/markets passed to `fetch_and_parse()` — EPEX and ENTSO-E each expose two flows in the same file (`run()` for SDAC zones, `run_gb()`/`run_ie()` for the one non-SDAC zone), so a schedule can target either without wasting a call on the other's not-yet-published zone. That's 12 flows total for day-ahead: `nordpool`, `nordpool_gb`, `epex.run`, `epex.run_gb`, `entsoe.run`, `entsoe.run_ie`, `ote`, `semo`, `opcom`, `omie`, `okte`, `enex`.

GB and IE used to be bundled into `epex`'s and `entsoe`'s single SDAC flow — split into a second `@flow` function in the *same file* (`run_gb()` in `clients/epex/endpoints/day_ahead.py`, `run_ie()` in `clients/entsoe/endpoints/day_ahead.py`), not a separate endpoint file: both call the same `fetch_and_parse()`/`dump()`/`parse_csv()` already in that file with a different zone list, so there's no duplicated fetch/parse logic to keep in sync. `nordpool` predates this and was already fully separate (`day_ahead.py` vs. `day_ahead_gb.py`) since its Nord Pool API call shape genuinely differs for GB — that split was left as-is.

**Timing groups** (anchor = the auction/coupling result the schedule is built around). Exact `cron` expressions live as comments directly above each `@flow` decorator — this section stays the narrative summary. Prefect itself runs in CET/CEST, so every cron is written in that single wall-clock timezone rather than per-source local time, converting UK/Irish local auction times to CET/CEST (currently a flat +1h, since the UK/Ireland and EU both change clocks on the same date — noted per-flow as a DST assumption to revisit if that ever stops holding):
- **SDAC** (~12:55 CET/CEST clearing) — `nordpool`, `epex.run`, `entsoe.run`, `ote`, `opcom`, `okte`, `enex`, `omie`. OTE/OPCOM/OKTE/ENEX/OMIE are assumed to publish on their own portals close to the same SDAC/4M MC clearing time; this isn't independently confirmed per operator, and the catch-up window below is partly there to absorb that uncertainty as well as genuine exchange-side delays.
- **N2EX + GB HalfHourly** (GB, two separate auctions, both earlier than SDAC) — `nordpool_gb`, `epex.run_gb`. N2EX gate closure 09:50 UK = 10:50 CET, results by 10:00 UK = 11:00 CET; HalfHourly gate closure 14:30 UK = 15:30 CET, results shortly after. Both flows fetch *both* GB markets in one call (`nordpool_gb`'s `MARKETS = ["N2EX_DayAhead", "GbHalfHour_DayAhead"]`, EPEX's `ZONE_FILE_CONFIG["GB"]` hourly + half-hourly `ZoneFile` entries) — a single ~2h catch-up window can't cover both clearings 4.5h apart, so each of these two flows needs **two** schedules, not one.
- **SEM-DA** (Ireland, separate auction, earlier than SDAC) — `semo`, `entsoe.run_ie`. Gate closure firm at 11:00 Irish time = 12:00 CET. The two live sources are **not on the same publish timeline** (see SEMO to-do section for why) and are scheduled differently on purpose:
  - `entsoe.run_ie` — no publish lag beyond ordinary SDAC-style same-day availability. Keeps its `*/15 12-13 CET` gate-closure-anchored cron, defaulting to tomorrow's delivery day.
  - `semo` — publishes a full day later than `entsoe.run_ie` (Irish-midnight batch publish). `run()` defaults to yesterday's delivery day, cron moved to an Irish-midnight-anchored `5,20,35,50 1-2 CET/CEST` catch-up window.

**Catch-up pattern**: start ~5 min after the expected publish time, poll every 15 minutes for up to 2 hours, to absorb minor exchange-side delays without per-operator retry tuning.

**Redundancy vs. cadence**: the ≥2-sources-per-zone requirement (see Goal) is outage insurance, not simultaneous real-time redundancy — it doesn't require every source per zone to run the same aggressive catch-up cadence. Per-zone primary/backup assignment not yet decided.

## Monitoring

A Prefect flow only fails on a code exception — correct for genuine errors, but wrong for a source legitimately returning zero rows for a given delivery day (e.g. SEMO's documented same-day 0-row behavior, see SEMO to-do). That conflation meant a real gap (a source silently breaking, or a zone losing its last live source) produced no signal at all. Data completeness is checked separately from flow health instead.

- **`monitoring/day_ahead_completeness.py`** — new top-level module (sibling to `clients/` and `core/`, since it's cross-cutting ops tooling, not a scraper endpoint or a shared library). `@flow`-decorated `run(target_date=None)`, defaulting to tomorrow's delivery day.
- **Check**: every in-scope bidding zone must have **at least one** `prod.prices` row with `product="DAY_AHEAD"` for the target delivery day — any live source counts, consistent with the ≥1-source-per-zone redundancy framing (see Goal); this is a zone-level check, not per-source.
- **In-scope zones**: a static list of every zone from the matrix above with ≥1 live source (35 country-level rows, expanded to 41 `bidding_zone` codes since IT counts as 7 ENTSO-E sub-zones instead of one `IT` code). Hardcoded directly in the script rather than shared via `core/`, since scrapers may split into their own repos later and a shared constant would complicate that split — revisit as a `core/` constant only if that need actually arises.
- **Delivery-day bounds**: reuses the same `_day_bounds_utc()` pattern (pytz `localize()` + `.astimezone(utc)`) already duplicated across `entsoe`/`opcom`/`enex`, anchored to `Europe/Copenhagen` — the same single CET/CEST anchor every existing scraper uses, including for GB/IE (see the flat +1h DST assumption in Scheduling above).
- **Timing**: `0 17 * * *` CET/CEST — after every live source's catch-up window for tomorrow's delivery day has closed (GB HalfHourly is the latest, ~15:30 CET).
- **Never fails the Prefect run** on missing data — a zone with zero rows is logged (and will be alerted, once the channel below is picked), not raised as an exception. Verified live: flow completes in state `Completed()` even when zones are missing.
- **Alerting — open decision**: `send_alert()` currently only logs a warning listing the missing zones. Email vs. Teams (or something else) is not decided yet; deliberately left as a stub rather than guessing, per the same "flag as a decision to make" approach used elsewhere in this doc.
- **Not done yet**: no Prefect deployment/schedule created for this flow (same "design only" status as the rest of Scheduling).

## Known gaps

Findings from a full pass over the current code and docs. Flagged rather than silently fixed, since several involve a decision to make.

**Nordpool's free API only serves a rolling ~2-month window, not full history.** `DayAheadPrices` returns `200` for recent dates and `401` for anything older (confirmed via direct `curl`, independent of this repo's code). Affects both `day_ahead.py` and `day_ahead_gb.py`; EPEX and ENTSO-E are unaffected. Raises priority of the v2-portal migration to-do; historical backfill will land with only 2 of 3 sources for older dates unless v2 access is obtained first.

**Not to forget later:** the `PREFECT_LOGGING_EXTRA_LOGGERS` setting (and the `PREFECT_HOME` override below) live on the local dev machine's Prefect profile only. Once a work pool/deployment actually gets created (still not wired up, see below), the same env vars need to be set wherever that worker runs (work pool job template or `prefect.yaml`'s `env:` block) — otherwise the worker process won't have them and UI logs will silently go back to being incomplete.

**No deployment/schedule exists yet.** `@flow` alone doesn't run anything on a cadence; no `prefect.yaml`, `flow.serve()`, `flow.deploy()`, or work pool. Design exists (see **Scheduling** above — grouping, catch-up window, redundancy cadence); actual deployment/schedule creation is still pending.

**`id-tables-design.drawio` is archived, not a pending plan.** Sketches `dim_bidding_zone`/`dim_product`/`dim_market`/`dim_source` tables with FKs, to stop typos like `"day ahead"` vs `"DAY_AHEAD"` landing silently. Decision: keep free text (see `product` vs `market` in Data model); diagram kept only as a future idea if bad `market` values become a real problem.

**Several remaining local providers are gated behind paid or paperwork-based access:**
- **HUPX (Hungary)** — paid "HUPX Labs" API, access unconfirmed. Also bundles BSP Southpool (SI) and SEEPEX (RS) — could unlock a second SI source too (RS isn't in scope). **Blocked for the time being (2026-07-20)** — HU and SI (only reachable via the same HUPX Labs bundle) are both deprioritized, not being actively pursued near-term.
- **CROPEX (Croatia)** — likely paid, same unconfirmed status. **Blocked for the time being (2026-07-20)**, same reasoning as HU/SI.
- **GME (Italy)** — free but needs paperwork/registration. Lower barrier than HUPX/CROPEX. Would give IT a second live source (currently 1, via ENTSO-E) for the ≥2-sources redundancy goal — not pursued yet, not blocked either.
- **OPCOM (Romania)** — no auth wall found so far, picked up first for that reason. Worth re-confirming once the client is built, in case automated (non-browser) access needs auth.
