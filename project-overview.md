# European Day-Ahead Price Scrapers

## Goal

Scrape day-ahead (and later intraday) electricity prices for all European bidding zones into a single Postgres table, replacing per-source formats scattered across scrapers.

Redundancy requirement: at least **two independent sources per bidding zone**, so a single source outage doesn't create a data gap. This is about having a working fallback available, not running every source on an identical real-time schedule — day-to-day, a single primary source landing data per zone is sufficient; secondary sources are backup for when the primary is down (see Scheduling).

## Scope

- In scope: day-ahead auction prices (`DAY_AHEAD`), all bidding zones listed below, historical backfill, Prefect-scheduled runs with logging.
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

`core/streaming.py` publishes every row `PriceStore.dump()` writes to Postgres onto a NATS stream, so the data is trivially accessible to any future consumer — how anything consumes it is deliberately not decided yet (out of scope, see Scope).

- **Producer only**: publishes via `quent_core.streaming`, doesn't touch the `quent-data-stream` gateway repo itself.
- **Stream**: own dedicated JetStream stream, named **`PRICES`**. Not scoped to "auction" or "day-ahead" in the name because `product`/`market` already carry the day-ahead-vs-intraday and auction-vs-continuous distinctions per-event, and `INTRADAY` will land on the same stream later. Known tradeoff, accepted: this data is **not** reachable via the gateway's `/replay`, `/ws`, `/hybrid` routes unless/until the gateway is separately updated to also watch `PRICES` — consumers need a direct NATS/JetStream connection for now. Created on first publish by `quent_core.streaming.ensure_stream()`, which only sets `name`+`subjects` (no explicit retention/duplicate_window/storage config) — so the stream runs on whatever `nats-py`/JetStream server defaults apply.
- **Subject**: one subject per `product`, not per source/zone — `prices.<product>.updates`, e.g. `prices.day_ahead.updates` today. `INTRADAY` gets its own subject automatically later, no code change needed. Consumers filter by zone/source/market client-side via the event's `data` fields. `NatsConfig.stream_subjects` is set to `prices.>` (a wildcard) precisely because `ensure_stream()` only applies the subjects list passed on first creation — a non-wildcard would have locked the stream to whichever product published first.
- **Connection**: same shared endpoint/certs as the Empire producer (`tls://192.168.1.202:4222`) — team-shared infra, not machine-specific.
- **Integration point**: centralized in `PriceStore.dump()`, not duplicated per endpoint — every `clients/*/endpoints/day_ahead*.py` file publishes "for free" with no changes of its own. `PriceStore(engine, publish=False)` (constructor) or `price_store.dump(df, publish=False)` (per call) disable it — e.g. for local/dev work or a historical backfill that shouldn't replay old rows onto the live stream. Only rows that actually committed to Postgres are published; a NATS outage is caught and logged, never fails the DB write.
- **Dedup key**: `core/streaming.py` defines its own `build_msg_id()`, building the full natural key (`subject:valuetime:bidding_zone:product:market:source` — the same columns as `PriceStore.KEY_COLUMNS` plus `product`), because one shared subject per product means many rows (different zone/source/market) share subject+valuetime within the same `dump()` batch — anything coarser would collide under any nonzero `duplicate_window`. Publishing goes through `EventPublisher(nats_cfg, logger)` (`connect()` / `publish_many()` / `close()`), always passing this explicit `msg_id`.

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
- [x] `PriceStore.dump()` / `.get()` — `core/price_store.py`, append-only change-detected writes. Targets `prod.prices`.
- [x] `prod.prices` DDL — `db/migrations/0001_create_prices.sql`, matches the live `prod.prices` schema; free-text columns, no FK/dimension tables (see Known gaps).
- [x] shared logging setup — `core/logging.py`, stdlib config only. **Not yet wired into Prefect** — see Known gaps.

**Nordpool**
- [x] client + day-ahead endpoint — `clients/nordpool/`, live to `prod.prices`; full 22-zone `BIDDING_ZONE_TO_NORDPOOL_AREA` mapping active.
- [x] GB day-ahead endpoint — `clients/nordpool/endpoints/day_ahead_gb.py`. GB isn't part of Nordpool's SDAC batch call, so it's a separate endpoint hitting two Nord Pool markets (`N2EX_DayAhead` hourly + `GbHalfHour_DayAhead` half-hourly), landed as two `market` rows for `bidding_zone=GB`; currency read off the response.
- [x] Free API only serves ~2 months of history — `401` for older dates, source-side rolling window, not a parsing bug (see Known gaps).
- [x] DST reviewed statically (no local-time day-boundary reconstruction). Live verification deferred to **2026-10-24–2026-12-24**, once the fall-back day is inside the rolling window, or sooner with v2 access.
- [ ] Currency handling — `day_ahead.py` still hardcodes `CURRENCY = "EUR"` instead of reading it off the response (harmless, every zone it covers settles EUR). `day_ahead_gb.py` already reads it correctly since GB settles GBP.
- [ ] Migrate to Nord Pool's gated v2 data portal — free API only serves ~2 months of history (`401` for older dates). Blocked on v2 access.

**EPEX**
- [x] client + day-ahead endpoint — `clients/epex/`, live to `prod.prices`; `ZONE_FILE_CONFIG` covers 19 zones (18 SDAC zones + CH) fetched by `run()`, plus GB (`market="Hourly"`/`"HalfHourly"`) fetched separately by `run_gb()`.
- [x] DST transition handling verified (see Nordpool + EPEX + ENTSO-E cross-check below).
- [x] `run_gb()` — second `@flow` in `day_ahead.py`, so GB (own N2EX-timed schedule, see Scheduling) doesn't ride along on `run()`'s SDAC-anchored schedule or vice versa. Calls the same `fetch_and_parse()`/`dump()` as `run()` with `bidding_zones=["GB"]` — no duplicated fetch/parse logic.

**ENTSO-E**
- [x] client + day-ahead endpoint — `clients/entsoe/`, live to `prod.prices`; `BIDDING_ZONE_TO_ENTSOE_AREA` mapping covers 33 of 35 zones (all except GB and IT — IT excluded by ENTSO-E's ~7-way sub-zone split). `run()` fetches 32 of those (excludes IE); IE fetched separately by `run_ie()`.
- [x] DST transition handling verified (see cross-check below).
- [x] `run_ie()` — second `@flow` in `day_ahead.py`, so IE (own SEM-DA-timed schedule, see Scheduling) doesn't ride along on `run()`'s SDAC-anchored schedule or vice versa. Calls the same `fetch_and_parse()`/`dump()` as `run()` with `bidding_zones=["IE"]` — no duplicated fetch/parse logic. Still hardcodes `MARKET = "SDAC"` for IE, same as `clients/semo/endpoints/day_ahead.py` does — technically wrong (I-SEM's SEM-DA), flagged but left as-is since both live sources already agree on that label and relabeling is a separate decision (see Known gaps).

**Nordpool + EPEX + ENTSO-E cross-check**
- [x] `@flow` decorator on all three endpoints' `run()`.
- [x] ≥2 sources per zone confirmed for all 24 zones (GB was the last gap, closed by the Nordpool GB endpoint + EPEX's GB zone).
- [x] DST transition handling:
  - **ENTSO-E**: correct by construction — `_day_bounds_utc()` uses `pytz.localize()`, `num_positions` derived from the actual UTC span, not an assumed 24h.
  - **EPEX**: static `Hour 3A`/`Hour 3B` columns disambiguate fall-back via `ambiguous=True/False`; spring-forward hours are null and filtered out before conversion. Known non-issue: plain `Hour N` columns use `ambiguous="raise"` with no `nonexistent=` handling, uncaught by `fetch_and_parse` — harmless since EPEX always pre-splits ambiguous hours.
  - **Nordpool**: reviewed statically only (see above).
- [ ] Historical backfill for these 24 zones — deferred on purpose, pending review of current progress, so nothing gets backfilled twice.

**OTE (Czech Republic)**
- [x] client + day-ahead endpoint — `clients/ote/`, live to `prod.prices`. SOAP via `zeep` (`PublicDataService` WSDL, `GetDamPricePeriodE`), single zone (CZ). Matches ENTSO-E's CZ feed to the cent (confirms EUR — endpoint has no currency field). Data only available from **2025-10-01** (CZ 15-min go-live) — note Production's `ote_api.py` has a stale comment claiming 2025-06-12, confirmed wrong. Legacy hourly endpoint (`GetDamPriceE`) not wired up, needs CZK/EUR + hourly handling of its own, deferred with backfill.

**SEMO (Ireland)**
- [x] client + day-ahead endpoint — `clients/semo/`, live to `prod.prices`. Lists/downloads SEMOpx static-reports (`DPuG_ID=EA-001`), filtered to `MarketResult_SEM-DA_*`. Single zone (IE) — only `ROI-DA` parsed (`NI-DA` is byte-identical but out of scope). Averages to ENTSO-E's hourly IE prices, confirming parsing + EUR assumption. Timestamps already carry explicit UTC `Z`, so no DST handling needed. SEM-DA is a **D+1 auction** with delivery day on CET/CEST boundaries, not Irish time — `fetch_day_ahead_documents()` queries one day earlier to account for this. Listing only retains ~12 months of documents — same rolling-window limit as Nordpool.
- [x] SEMO's static-reports catalog indexes `SEM-DA`/`IDA1` documents ~2 calendar days after their `DateRetention` date, unlike `IDA2`/`IDA3` which publish next-day — `run()` legitimately returns 0 rows for very recent delivery days, not a bug. Invalidates the Scheduling section's "results shortly after gate closure" assumption for `semo`/`entsoe.run_ie` — see Scheduling.

**OPCOM (Romania)**
- [x] client + day-ahead endpoint — `clients/opcom/`, live to `prod.prices`. XML export from opcom.ro's report page, no auth wall beyond a User-Agent check (WAF blocked default `python-requests` UA — fixed with a static `Mozilla/5.0` header). Single zone (RO), no per-row timestamp — `valuetime` derived from 1-based `Pos` vs. the true UTC day span (same approach as ENTSO-E/OTE/OMIE). Dates with no report return HTTP 200 with an empty `<resultset/>`. History goes back to at least 2015-01-01 (not fully bisected). Delivery-day boundary confirmed CET/CEST, not RO's own EET/EEST — cross-checked against ENTSO-E. Currency hardcoded EUR (no field to read).

**OMIE (Spain / Portugal)**
- [x] client + day-ahead endpoint — `clients/omie/`, live to `prod.prices`. No API — daily flat files on a Drupal file-browser, one file covers both ES and PT (joint MIBEL auction). `list_files()` scrapes the listing to resolve the current-version filename per date (corrected files get incremented suffixes). Forecasttime from file mtime. Resolution derived per-file. ES and PT price columns aren't always identical — diverge during interconnector congestion, so both are parsed as distinct rows. Delivery-day boundary uses `Europe/Madrid` for both zones — cross-checked against ENTSO-E. Pre-2023 history exists as yearly zip archives, not wired up.

**CROPEX (Croatia)**
- [ ] Client not started — likely paid API access, unconfirmed (see Known gaps); blocked.

**HUPX (Hungary)**
- [ ] Client not started — paid "HUPX Labs" API, unconfirmed access (see Known gaps); blocked. HUPX Labs also bundles BSP Southpool (SI) and SEEPEX (RS) data — could unlock a second SI source too.

**OKTE (Slovakia)**
- [x] client + day-ahead endpoint — `clients/okte/`, live to `prod.prices`. Public unauthenticated REST API (`isot.okte.sk/api/v1/dam/results`), no WAF issue. Single zone (SK). Response timestamps are already full UTC ISO-8601, so no local-time boundary math is needed. Data available back to 2010. One request accepts a full date range, so `fetch_day_ahead_prices()` does a single bulk call per run, unlike OPCOM/OMIE's per-day loop. Currency hardcoded EUR (no field). Cross-checked against ENTSO-E's SK feed; also confirmed SK/CZ/HU/RO clear on the same 4M Market Coupling price.

**ENEX (Greece)**
- [x] client + day-ahead endpoint — `clients/enex/`, live to `prod.prices`. HEnEx's EL-DAM results xlsx on a Liferay page, no auth wall. Single zone (GR); targets the "Results" portlet specifically (instance `6eBaUXF5VIb7`). Listing ignores `_delta=`, so `list_files()` paginates via `_cur=1,2,3,...`.
- [x] Results sheet repeats each period's MCP once per breakdown row (exports, load, generation mix, ...) — `parse_response` dedupes to one row per `SORT` position. `DELIVERY_DURATION` read per row, not hardcoded.
- [x] Delivery-day boundary confirmed CET/CEST, not Greece's own EET/EEST — cross-checked against ENTSO-E's GR feed. `valuetime` reconstructed from the 1-based `SORT` position rather than parsing the ambiguous wall-clock column directly.
- [x] Currency hardcoded EUR (no field; confirmed via the ENTSO-E cross-check).
- [x] `forecasttime` uses `utcnow()` fallback — no reliably-timezoned native publish timestamp.
- [x] Listing only retains documents back to **2026-01-01** — fall-back DST transition not verifiable until 2025-10-26 ages back into range; revisit after 2026-10-25.

**GME (Italy)**
- [ ] Not started. API looks pricey and needs paperwork/registration.

**BSP Southpool (Slovenia)**
- [ ] Southpool (SI) only reachable via the HUPX Labs bundle — no standalone source identified yet.

**Cross-cutting / not scoped to one source**
- [ ] Deactivate the per-zone CSV dump in every endpoint's `dump()` before going live — each still writes `output/<source>/<endpoint>/<zone>.csv` alongside the `prod.prices` write. Not documented as functionality anywhere (docstrings/README/CLAUDE.md deliberately say nothing about it); kept only as a debug line for manual cross-checking, not something to rely on.
- [ ] Intraday scrapers (IDA1-3 auctions, ID1/ID3/FULL VWAPs) — schema already supports this via `market`.
- [x] NATS publish alongside DB write — every `clients/*/endpoints/day_ahead*.py` file publishes automatically via `PriceStore.dump()` (see Streaming section above). Verified across single-source and multi-source batch runs with no `msg_id` collisions, including rows across different zones/sources sharing a subject and `valuetime`.
- [ ] Market code reference/lookup table — only if free-text `market` values start causing problems; `id-tables-design.drawio` sketches an FK-based alternative.
- [ ] Fuller normalized table design (`id-tables-design.drawio`) — standing idea for a `dim_bidding_zone`/`dim_product`/`dim_market`/`dim_source` model with FKs into `prod.prices`, independent of the typo problem. No plan yet.
- [ ] Day-ahead volumes alongside prices — needs a schema decision (extend `prod.prices` vs. separate table); currently out of scope.
- [ ] Dependency manifest — no `requirements.txt`/`pyproject.toml`; `prefect`, `pandas`, `sqlalchemy`, `paramiko`, `xmltodict`, `pytz`, `requests`, `zeep`, `openpyxl`, `quent_core` all unpinned.
- [ ] Consistent retry policy — EPEX retries once on a dropped SFTP connection; Nordpool and ENTSO-E fail immediately with no retry.
- [ ] Failure alerting — nothing notifies anyone on a failed run beyond the log line; consider a Prefect automation.

## Scheduling

Design only — nothing deployed yet (see Known gaps). Captured here because the grouping/catch-up/redundancy decisions are non-obvious and worth settling before wiring up Prefect deployments.

**Granularity**: one Prefect deployment per `@flow`-decorated function. Each flow processes only the zones/markets passed to `fetch_and_parse()` — EPEX and ENTSO-E each expose two flows in the same file (`run()` for SDAC zones, `run_gb()`/`run_ie()` for the one non-SDAC zone), so a schedule can target either without wasting a call on the other's not-yet-published zone. That's 12 flows total for day-ahead: `nordpool`, `nordpool_gb`, `epex.run`, `epex.run_gb`, `entsoe.run`, `entsoe.run_ie`, `ote`, `semo`, `opcom`, `omie`, `okte`, `enex`.

GB and IE used to be bundled into `epex`'s and `entsoe`'s single SDAC flow — split into a second `@flow` function in the *same file* (`run_gb()` in `clients/epex/endpoints/day_ahead.py`, `run_ie()` in `clients/entsoe/endpoints/day_ahead.py`), not a separate endpoint file: both call the same `fetch_and_parse()`/`dump()`/`parse_csv()` already in that file with a different zone list, so there's no duplicated fetch/parse logic to keep in sync. `nordpool` predates this and was already fully separate (`day_ahead.py` vs. `day_ahead_gb.py`) since its Nord Pool API call shape genuinely differs for GB — that split was left as-is.

**Timing groups** (anchor = the auction/coupling result the schedule is built around). Exact `cron` expressions live as comments directly above each `@flow` decorator — this section stays the narrative summary. Prefect itself runs in CET/CEST, so every cron is written in that single wall-clock timezone rather than per-source local time, converting UK/Irish local auction times to CET/CEST (currently a flat +1h, since the UK/Ireland and EU both change clocks on the same date — noted per-flow as a DST assumption to revisit if that ever stops holding):
- **SDAC** (~12:55 CET/CEST clearing) — `nordpool`, `epex.run`, `entsoe.run`, `ote`, `opcom`, `okte`, `enex`, `omie`. OTE/OPCOM/OKTE/ENEX/OMIE are assumed to publish on their own portals close to the same SDAC/4M MC clearing time; this isn't independently confirmed per operator, and the catch-up window below is partly there to absorb that uncertainty as well as genuine exchange-side delays.
- **N2EX + GB HalfHourly** (GB, two separate auctions, both earlier than SDAC) — `nordpool_gb`, `epex.run_gb`. N2EX gate closure 09:50 UK = 10:50 CET, results by 10:00 UK = 11:00 CET; HalfHourly gate closure 14:30 UK = 15:30 CET, results shortly after. Both flows fetch *both* GB markets in one call (`nordpool_gb`'s `MARKETS = ["N2EX_DayAhead", "GbHalfHour_DayAhead"]`, EPEX's `ZONE_FILE_CONFIG["GB"]` hourly + half-hourly `ZoneFile` entries) — a single ~2h catch-up window can't cover both clearings 4.5h apart, so each of these two flows needs **two** schedules, not one.
- **SEM-DA** (Ireland, separate auction, earlier than SDAC) — `semo`, `entsoe.run_ie`. Gate closure firm at 11:00 Irish time = 12:00 CET. **The "results shortly after gate closure" publish-time assumption is wrong**: SEMO's static-reports catalog indexes `SEM-DA` (and `IDA1`) documents roughly **2 calendar days** after their auction/`DateRetention` date, not same-day, while `IDA2`/`IDA3` publish next-day. The `*/15 12-13 CET` cron for both flows is built on the wrong assumption and needs redesigning around this ~2-day catalog lag, not gate-closure time — not fixed here, flagging as a scheduling decision to make.

**Catch-up pattern**: start ~5 min after the expected publish time, poll every 15 minutes for up to 2 hours, to absorb minor exchange-side delays without per-operator retry tuning.

**Redundancy vs. cadence**: the ≥2-sources-per-zone requirement (see Goal) is outage insurance, not simultaneous real-time redundancy — it doesn't require every source per zone to run the same aggressive catch-up cadence. Per-zone primary/backup assignment not yet decided.

## Known gaps

Findings from a full pass over the current code and docs. Flagged rather than silently fixed, since several involve a decision to make.

**Nordpool's free API only serves a rolling ~2-month window, not full history.** `DayAheadPrices` returns `200` for recent dates and `401` for anything older (confirmed via direct `curl`, independent of this repo's code). Affects both `day_ahead.py` and `day_ahead_gb.py`; EPEX and ENTSO-E are unaffected. Raises priority of the v2-portal migration to-do; historical backfill will land with only 2 of 3 sources for older dates unless v2 access is obtained first.

**Flow logs aren't wired into Prefect.** All three endpoints still use `logging.getLogger(__name__)`/`setup_logging()` instead of `prefect.logging.get_run_logger()` — `@flow` is on `run()`, but log lines won't show as flow-run logs in the Prefect UI. Needs a decision: switch to `get_run_logger()`, add a stdout-forwarding handler, or accept incomplete UI logs.

**No deployment/schedule exists yet.** `@flow` alone doesn't run anything on a cadence; no `prefect.yaml`, `flow.serve()`, `flow.deploy()`, or work pool. Design exists (see **Scheduling** above — grouping, catch-up window, redundancy cadence); actual deployment/schedule creation is still pending.

**`id-tables-design.drawio` is archived, not a pending plan.** Sketches `dim_bidding_zone`/`dim_product`/`dim_market`/`dim_source` tables with FKs, to stop typos like `"day ahead"` vs `"DAY_AHEAD"` landing silently. Decision: keep free text (see Resolved Decisions); diagram kept only as a future idea if bad `market` values become a real problem.

**Inconsistent retry behavior across clients.** EPEX retries once on a dropped SFTP connection; Nordpool and ENTSO-E fail immediately on any request exception. Tolerable today since `dump()`'s change-detection makes rescrapes cheap, but worth a deliberate policy once these run unattended.

**No dependency manifest.** No `requirements.txt`/`pyproject.toml` — `prefect`, `pandas`, `sqlalchemy`, `paramiko`, `xmltodict`, `pytz`, `requests`, `zeep`, and `quent_core` are all unpinned. Not urgent solo, but will matter once deployed to a different machine.

**IE rows are labeled `market="SDAC"` on both live sources, but IE isn't SDAC.** `clients/entsoe/endpoints/day_ahead.py`'s `run_ie()` and `clients/semo/endpoints/day_ahead.py` both hardcode `MARKET = "SDAC"` for Ireland's I-SEM `SEM-DA` auction. Left as-is — both sources already agree on the label, so it isn't currently causing a cross-source mismatch, and relabeling touches already-written rows. Flagging as a decision to make, not fixing silently.

**Several remaining local providers are gated behind paid or paperwork-based access:**
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
| Repo name | `day-ahead-prices` |
| Rescrape dedup strategy | Append-only, not upsert: `PriceStore.dump()` looks up the latest known price per key (one query per batch, not per row) and inserts a new row (new `forecasttime`) only when price actually changed; unchanged rescrapes are skipped. `forecasttime` therefore means "when this price last changed", not "when we last checked" — true for both source-native forecasttime (e.g. EPEX file mtime) and `utcnow()` fallback sources alike. `ON CONFLICT DO NOTHING` on the full PK is kept only as a safety net against exact re-inserts (e.g. a retried failed run), not as the change-detection mechanism. Comparison is price-only — resolution/currency changes alone don't trigger a new row. |
| Column types | `bidding_zone`/`product`/`market`/`source` are `varchar(20)`, `currency` is `varchar(10)` (not open `text`); `resolution` is `smallint` (minutes) rather than an ISO 8601 duration string like `PT60M`. All columns are `NOT NULL`. Verified against the draft DDL for the table. |
| Dimension tables / FKs (`id-tables-design.drawio`) | Not adopted. Free-text columns stand per the `product` vs `market` decision above. Diagram kept only as a future idea to revisit if invalid/misspelled `market` values actually cause problems. |
| GB day-ahead via Nord Pool | Land both `N2EX_DayAhead` (hourly) and `GbHalfHour_DayAhead` (half-hourly) as separate `market` rows for `bidding_zone=GB` — same one-zone-two-markets pattern as AT's `SDAC`/`EXAA_EARLY`, not a choice between them. |
| NATS subject granularity | One subject per `product` (`prices.<product>.updates`), not per source or per zone — consumers filter by zone/source/market via the event's `data` fields. See Streaming section above. |
| NATS integration point | Centralized in `PriceStore.dump()`, not duplicated per endpoint (unlike the Empire producer, which publishes inline in each endpoint) — every `clients/*/endpoints/day_ahead*.py` file is untouched. |
