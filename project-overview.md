# European Day-Ahead Price Scrapers

## Goal

Scrape day-ahead (and later intraday) electricity prices for most European bidding zones into a single Postgres table, replacing ad-hoc queries from dashboards and algorithms.

Redundancy requirement: at least **two independent sources per bidding zone**, so a single source outage doesn't create a data gap. This is about having a working fallback available, not running every source on an identical real-time schedule — day-to-day, a single primary source landing data per zone is sufficient; secondary sources are backup for when the primary is down (see Scheduling). This requirement is about live operation, not backfill.

## Scope

- In scope: day-ahead auction prices (`DAY_AHEAD`), all bidding zones listed below, historical backfill (2024-01-01 onward — see Historical backfill), Prefect-scheduled runs with logging.
- Later, not now: intraday (`INTRADAY`) scrapers. Schema already supports it (see Data model).
- Out of scope: anything not price-related (volumes, nominations, flows, imbalance prices) — stays in existing scraper setups.

## Architecture

- **Monorepo** (for now), not one repo per scraper. A client (e.g. EPEX) can split into its own repo later if it grows enough to justify it.
- `core/` — shared library: DB dump/retrieve (`PriceStore`), logging setup, common utils. Used by every client. Moves to QUENT Core once tested and approved.
- `clients/<source>/` — one folder per data source (nordpool, epex, entsoe, cropex, ote, ...):
  - `client.py` — auth + HTTP request handling only, no parsing.
  - `config.py` — source-specific config/secrets.
  - `endpoints/<endpoint>.py` — fetch, parse, dump per endpoint. Also hosts the `@flow`-decorated `run()`, with backfill exposed via optional `bidding_zones`/date-range params (2026-07-23: `bidding_zones` made consistent across every flow, see below). The per-zone CSV dump that used to run alongside the DB write is commented out (not deleted) in every endpoint, kept available to uncomment for manual debugging/cross-checking.
- **Consistent `bidding_zones` param (multi-zone flows only)**: `fetch_and_parse(bidding_zones, from_date, to_date, ...)` and `run(bidding_zones=None, from_date=None, to_date=None)` share the same shape on **Nordpool's main `run()`, EPEX's main `run()`, ENTSO-E's main `run()`, and OMIE** — the four flows that each cover more than one zone. A subset (e.g. `run(bidding_zones=["NO1"])`) re-runs/backfills just that zone without looping over the rest — the pattern `scripts/backfill_no1_no2_gap.py` already used for EPEX/ENTSO-E, now also on Nordpool and OMIE. `run()`'s default (`bidding_zones=None`) reproduces the flow's normal full zone list, so the live scheduled behavior is unchanged. Nordpool's `deliveryArea`/EPEX's per-zone file/ENTSO-E's per-zone request mean a subset genuinely fetches less; OMIE's joint ES+PT file is still fetched in full regardless, so a subset there only narrows what gets parsed/dumped. Every single-zone flow (OTE, SEMO, OPCOM, OKTE, ENEX, plus Nordpool's/EPEX's/ENTSO-E's separate GB/IE flows) deliberately does **not** carry this param — there's nothing to subset when the flow only ever covers its one zone, so `fetch_and_parse(from_date, to_date)`/`run(from_date=None, to_date=None)` stay as they were.
- **Prefect**: `@flow` sits directly on each endpoint's `run()`. Logs go to Prefect so failures are visible without digging through server logs.
- **Storage**: writes directly to Postgres via `PriceStore` (see Dependencies — now sourced from `quent_core`, not this repo).
- **DB engine**: `from Database.db_connect import engine` — same shared engine as for example `ImbalancePriceHandler`. `PriceStore` takes this as a constructor arg rather than building its own connection.
- **Dependencies**: Poetry-managed (`pyproject.toml`/`poetry.lock`), own independent `.venv` — not merged into Production's. Shared deps pinned to match Production's exactly; `quent_core` is the one deliberate deviation, pinned to `v1.0.161-seb-database-functions` (Production is on `v1.0.158`). `PriceStore` moved out of this repo (`core/price_store.py` deleted) and now lives in `quent_core.database.price_store` — `core/__init__.py` re-exports it from there so every client's `from core import PriceStore` import is unchanged. `streamlit ~=1.43.0` added for `monitoring/coverage.py`.

## Streaming (quent-data-stream)

Publishing to `quent-data-stream` is **not currently active** in this repo. The old `core/price_store.py` had a working NATS JetStream publish path (stream `PRICES`, subject `prices.<market_type>.updates`), but it was cut when `PriceStore` moved to `quent_core` — the ported `quent_core.database.price_store.PriceStore` is dump/retrieve only, no publish, since `quent_core`'s own streaming module is mid-rework and too unstable to build against right now (2026-07-23 decision). Ties back to `Goal`'s "how anything consumes it is deliberately not decided yet" — still true, now with no producer either.

- Expected to come back as a `quent_core`-side add-on once that rework lands, requiring only a minimal change here (passing a publish flag/config through, not re-implementing the NATS logic).
- Until then: no `PRICES` stream, no NATS cert/config in this repo (`core/certs/` removed), no `publish=` parameter on `PriceStore`.
- The previous dedup-key design (`_build_msg_id()` building the full natural key `subject:valuetime:bidding_zone:market_type:market:source`, not NATS's `subject:valuetime` default) and the gateway gap (`PRICES` not reachable via `/replay`/`/ws`/`/hybrid`) are notes for whenever publishing returns, not current behavior.

## Data model

Table: **`prod.prices`**.

| Column       | Type              | PK  | Not Null | Description                                               |
| ------------ | ----------------- | :-: | :------: | ----------------------------------------------------------- |
| valuetime    | `timestamptz`     |  ✓  |    ✓     | Start of delivery period (UTC)                              |
| forecasttime | `timestamptz`     |  ✓  |    ✓     | Timestamp when the data was scraped (UTC)                   |
| bidding_zone | `varchar(20)`     |  ✓  |    ✓     | Delivery area (DE, DK1, NO2, GB, ...)                        |
| market_type  | `varchar(20)`     |  ✓  |    ✓     | Coarse bucket: `DAY_AHEAD` or `INTRADAY`                     |
| market       | `varchar(20)`     |  ✓  |    ✓     | The actual price series identity (see below)                |
| source       | `varchar(20)`     |  ✓  |    ✓     | Data source (`EPEX`, `NORDPOOL`, `ENTSOE`, `EXAA`, ...)     |
| resolution   | `smallint`        |     |    ✓     | Delivery resolution in minutes (`60`, `30`, `15`)            |
| currency     | `varchar(10)`     |     |    ✓     | Native currency (`EUR`, `GBP`, `CHF`, `NOK`, ...)            |
| price        | `numeric(10,2)`   |     |    ✓     | Market clearing price / VWAP                                |

`bidding_zone`/`market_type`/`market`/`source` capped at 20 chars, `currency` at 10 — may need extending eventually.

**`market_type` vs `market`**: `market_type` is a coarse filter, `market` is what actually disambiguates. `DAY_AHEAD` doesn't always mean SDAC — GB isn't part of SDAC at all, and AT has both SDAC and EXAA's early auction for the same delivery day. `market` covers auction codes (`SDAC`, `EXAA_EARLY`, `IDA1-3`) and intraday VWAP series (`ID1`, `ID3`, `FULL`) as open text, no enum. A fuller normalized design (`dim_bidding_zone`/`dim_market_type`/`dim_market`/`dim_source` tables with FKs) is sketched in `id-tables-design.drawio`, archived as a future option, not a pending plan — revisit only if bad `market` values actually become a problem.

**Resolution**: most zones have moved to 15-minute settlement, some are still 30 or 60. Stored as plain integer minutes, read per response — never hardcoded per zone, since a zone can change resolution over time.

`PriceStore.get()` collapses to the latest `forecasttime` per `valuetime`/zone/market_type/market/source, so consumers get the current price curve, not every scrape snapshot.

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

31 of 35 country-level zones have ≥2 live sources. HR, HU, SI and IT are the remaining single-source zones — HR/HU/SI's local sources (CROPEX, HUPX, BSP Southpool) and IT's second source (GME) are all gated behind paid/unconfirmed access (see per-source notes below), so none are being actively pursued near-term.

## Sources

One entry per source: how it's implemented, and any source-side behavior that shapes scheduling or backfill reach. All are live and landing rows in `prod.prices` unless marked not started.

**Nordpool** — `clients/nordpool/`. Full 22-zone `BIDDING_ZONE_TO_NORDPOOL_AREA` mapping via `day_ahead.py`; GB handled by a separate `day_ahead_gb.py` (own API call shape) hitting `N2EX_DayAhead` (hourly) + `GbHalfHour_DayAhead` (half-hourly), landed as two `market` rows for `bidding_zone=GB`. Currency read per-response, not hardcoded. Free API only serves a rolling ~2-month window (`401` for older dates) — source-side limit, not a bug; the gated v2 data portal would remove it (blocked on v2 access). DST handling reviewed statically only, not yet live-verified (the rolling window won't reach a fall-back transition until 2026-10-24).

**EPEX** — `clients/epex/`. `ZONE_FILE_CONFIG` covers 19 zones (18 SDAC zones + CH) via `run()`, plus GB (hourly + half-hourly) via a separate `run_gb()` so GB's own N2EX-timed schedule doesn't ride along on `run()`'s SDAC-anchored one. `fetch_day_ahead_file()` tries the configured resolution first and falls back to 60 min if that file doesn't exist, needed for zones now configured at 15 min that were hourly before Oct 2025. DST transition handling verified (see cross-check below). SFTP client (`client.py`) bounds the connect with a 30s socket timeout before handing off to `paramiko.Transport`, and skips retrying permanent misses (`FileNotFoundError`) instead of wasting a retry backoff on them.

**ENTSO-E** — `clients/entsoe/`. `BIDDING_ZONE_TO_ENTSOE_AREA` covers 34 of 35 country-level zones (all except GB) — IT is split into its own 7 sub-zones rather than one `IT` entry, so the mapping has 40 dict entries total. `run()` fetches 39 of those (excludes IE); IE fetched separately by `run_ie()` (own SEM-DA-timed schedule) passing `market="SEM_DA"` instead of `run()`'s default `"SDAC"`. DST transition handling correct by construction — `_day_bounds_utc()` derives the number of settlement positions from the actual UTC span, not an assumed 24h. The API throttles under sustained concurrent load (seen during the 2024 historical backfill, running requests 5-zones-concurrently) — if backfill concurrency is used again, treat 5 workers as near the safe ceiling and re-verify per-zone coverage afterward rather than trusting a clean exit code.

**Nordpool + EPEX + ENTSO-E cross-check**: `@flow` on all three's `run()`; ≥2 sources per zone confirmed for all 35 country-level zones (GB was the last gap, closed by Nordpool's GB endpoint + EPEX's GB zone). DST: EPEX's static `Hour 3A`/`Hour 3B` columns disambiguate fall-back via `ambiguous=True/False` and spring-forward hours are null and filtered before conversion (its plain `Hour N` columns use `ambiguous="raise"` with no `nonexistent=` handling, harmless since EPEX always pre-splits ambiguous hours).

**OTE (Czech Republic)** — `clients/ote/`. SOAP via `zeep` (`PublicDataService` WSDL, `GetDamPricePeriodE`), single zone (CZ). Matches ENTSO-E's CZ feed to the cent (confirms EUR — endpoint has no currency field). Data only available from **2025-10-01** (CZ 15-min go-live). Legacy hourly endpoint (`GetDamPriceE`) not wired up, would need its own CZK/EUR + hourly handling.

**SEMO (Ireland)** — `clients/semo/`. Lists/downloads SEMOpx static-reports (`DPuG_ID=EA-001`), filtered to `MarketResult_SEM-DA_*`. Single zone (IE) — only `ROI-DA` parsed (`NI-DA` is byte-identical but out of scope). Averages to ENTSO-E's hourly IE prices, confirming parsing + EUR assumption. Timestamps carry explicit UTC `Z`, so no DST handling needed. SEM-DA is a **D+1 auction** with delivery day on CET/CEST boundaries, not Irish time — `fetch_day_ahead_documents()` queries one day earlier to account for this. The catalog batch-publishes every document at Irish midnight the day *after* its delivery day (per the API's `PublishTime` field) — a full day later than ENTSO-E's IE feed — so `run()` defaults to **yesterday's** delivery day, not tomorrow's, with a cron anchored to that Irish-midnight publish (see Scheduling). Listing retains roughly the last 12 months (~327 days measured) of documents — same rolling-window shape as Nordpool.

**OPCOM (Romania)** — `clients/opcom/`. XML export from opcom.ro's report page, no auth wall beyond a User-Agent check (a static `Mozilla/5.0` header avoids the WAF's default-`python-requests` block). Single zone (RO), no per-row timestamp — `valuetime` derived from 1-based `Pos` vs. the true UTC day span (same approach as ENTSO-E/OTE/OMIE). Dates with no report return HTTP 200 with an empty `<resultset/>`. History goes back to at least 2015-01-01. Delivery-day boundary is CET/CEST, not RO's own EET/EEST — cross-checked against ENTSO-E. Currency hardcoded EUR (no field to read).

**OMIE (Spain / Portugal)** — `clients/omie/`. No API — daily flat files on a Drupal file-browser, one file covers both ES and PT (joint MIBEL auction). `list_files()` scrapes the listing to resolve the current-version filename per date (corrected files get incremented suffixes). Forecasttime from file mtime, resolution derived per-file. ES and PT price columns aren't always identical — diverge during interconnector congestion, so both are parsed as distinct rows. Delivery-day boundary uses `Europe/Madrid` for both zones — cross-checked against ENTSO-E. Pre-2023 history exists as yearly zip archives, not wired up.

**OKTE (Slovakia)** — `clients/okte/`. Public unauthenticated REST API (`isot.okte.sk/api/v1/dam/results`). Single zone (SK). Response timestamps are already full UTC ISO-8601, no local-time boundary math needed. Data available back to 2010. One request accepts a full date range, so `fetch_day_ahead_prices()` does a single bulk call per run, unlike OPCOM/OMIE's per-day loop. Currency hardcoded EUR (no field). Cross-checked against ENTSO-E's SK feed; also confirms SK/CZ/HU/RO clear on the same 4M Market Coupling price.

**ENEX (Greece)** — `clients/enex/`. HEnEx's EL-DAM results xlsx on a Liferay page, no auth wall; targets the "Results" portlet (instance `6eBaUXF5VIb7`), paginated via `_cur=1,2,3,...`. The results sheet repeats each period's MCP once per breakdown row (exports, load, generation mix, ...) — `parse_response` dedupes to one row per `SORT` position, with `valuetime` reconstructed from that 1-based position rather than the ambiguous wall-clock column. Delivery-day boundary is CET/CEST, not Greece's own EET/EEST — cross-checked against ENTSO-E. Currency hardcoded EUR; `forecasttime` uses `utcnow()` fallback (no reliably-timezoned native publish timestamp). Listing retention is a rolling window (~6 weeks measured, reaching back to 2026-06-09), not a fixed floor.

**CROPEX (Croatia)**, **HUPX (Hungary)**, **GME (Italy)** — not started, all gated behind paid or unconfirmed API access. HUPX also bundles BSP Southpool (SI) and SEEPEX (RS, out of scope) — a second SI source would come along with it. All three deprioritized, not being actively pursued near-term.

**BSP Southpool (Slovenia)** — no standalone source; only reachable via the HUPX Labs bundle above.

## Historical backfill

Backfilled to **2024-01-01** wherever a source can reach that far back; only **one** source per zone/day is required for backfill (the ≥2-sources rule is live-operation outage insurance, see Goal). Driven by the one-off `scripts/backfill_2024.py` (not scheduled). No `publish=False` needed anymore — `PriceStore.dump()` doesn't publish anywhere right now (see Streaming).

Verified with `scripts/verify_backfill.py` — a day-by-day gap scan across all 41 in-scope `bidding_zone` codes (not just MIN/MAX per zone, which can miss holes in the middle of an otherwise-normal-looking range). Re-run this after any future bulk backfill rather than trusting a clean exit code or MIN/MAX alone.

**Per-source floor** (can't reach 2024-01-01, source-side limit, not a bug): Nordpool (~2-month rolling window), OTE (floor 2025-10-01, CZ 15-min go-live), SEMO (~327-day rolling retention), ENEX (~6-week rolling window). All four zones are still fully covered back to 2024-01-01 overall via their other live source(s). ENTSO-E, EPEX, OPCOM, OMIE, OKTE all confirmed reaching back to 2024-01-01.

**Resolution change (October 2025)**: many zones moved 60-min → 15-min settlement then, so backfilled 2024 rows are labeled `resolution=60` for those zones, not the current value. ENTSO-E/OPCOM/OMIE/OKTE derive `resolution` dynamically per response, safe by construction; EPEX's historical-resolution fallback (see EPEX above) handles it explicitly.

## Scheduling

Design only — nothing deployed yet (see Open items). Captured here because the grouping/catch-up/redundancy decisions are non-obvious and worth settling before wiring up Prefect deployments.

**Granularity**: one Prefect deployment per `@flow`-decorated function. Each flow processes only the zones/markets passed to `fetch_and_parse()` — EPEX and ENTSO-E each expose two flows in the same file (`run()` for SDAC zones, `run_gb()`/`run_ie()` for the one non-SDAC zone), so a schedule can target either without wasting a call on the other's not-yet-published zone. That's 12 flows total for day-ahead: `nordpool`, `nordpool_gb`, `epex.run`, `epex.run_gb`, `entsoe.run`, `entsoe.run_ie`, `ote`, `semo`, `opcom`, `omie`, `okte`, `enex`.

**Timing groups** (anchor = the auction/coupling result the schedule is built around). Exact `cron` expressions live as comments directly above each `@flow` decorator — this section stays the narrative summary. Prefect itself runs in CET/CEST, so every cron is written in that single wall-clock timezone rather than per-source local time, converting UK/Irish local auction times to CET/CEST (currently a flat +1h, since the UK/Ireland and EU both change clocks on the same date — noted per-flow as a DST assumption to revisit if that ever stops holding):
- **SDAC** (~12:55 CET/CEST clearing) — `nordpool`, `epex.run`, `entsoe.run`, `ote`, `opcom`, `okte`, `enex`, `omie`. OTE/OPCOM/OKTE/ENEX/OMIE are assumed to publish on their own portals close to the same SDAC/4M MC clearing time; this isn't independently confirmed per operator, and the catch-up window below is partly there to absorb that uncertainty as well as genuine exchange-side delays.
- **N2EX + GB HalfHourly** (GB, two separate auctions, both earlier than SDAC) — `nordpool_gb`, `epex.run_gb`. N2EX gate closure 09:50 UK = 10:50 CET, results by 10:00 UK = 11:00 CET; HalfHourly gate closure 14:30 UK = 15:30 CET, results shortly after. Both flows fetch *both* GB markets in one call — a single ~2h catch-up window can't cover both clearings 4.5h apart, so each of these two flows needs **two** schedules, not one.
- **SEM-DA** (Ireland, separate auction, earlier than SDAC) — `semo`, `entsoe.run_ie`. Gate closure firm at 11:00 Irish time = 12:00 CET. The two live sources are **not on the same publish timeline** (see SEMO above) and are scheduled differently on purpose:
  - `entsoe.run_ie` — no publish lag beyond ordinary SDAC-style same-day availability. Keeps its `*/15 12-13 CET` gate-closure-anchored cron, defaulting to tomorrow's delivery day.
  - `semo` — publishes a full day later (Irish-midnight batch publish). `run()` defaults to yesterday's delivery day, cron moved to an Irish-midnight-anchored `5,20,35,50 1-2 CET/CEST` catch-up window.

**Catch-up pattern**: start ~5 min after the expected publish time, poll every 15 minutes for up to 2 hours, to absorb minor exchange-side delays without per-operator retry tuning.

**Redundancy vs. cadence**: the ≥2-sources-per-zone requirement (see Goal) is outage insurance, not simultaneous real-time redundancy — it doesn't require every source per zone to run the same aggressive catch-up cadence. Per-zone primary/backup assignment not yet decided.

## Monitoring

A Prefect flow only fails on a code exception — correct for genuine errors, but wrong for a source legitimately returning zero rows for a given delivery day (e.g. SEMO's documented same-day 0-row behavior, see SEMO above). That conflation meant a real gap (a source silently breaking, or a zone losing its last live source) produced no signal at all. Data completeness is checked separately from flow health instead.

- **`monitoring/day_ahead_completeness.py`** — top-level module (sibling to `clients/` and `core/`, cross-cutting ops tooling, not a scraper endpoint). `@flow`-decorated `run(target_date=None)`, defaulting to tomorrow's delivery day.
- **Check**: every in-scope bidding zone must have **at least one** `prod.prices` row with `market_type="DAY_AHEAD"` for the target delivery day — any live source counts, consistent with the ≥1-source-per-zone redundancy framing; this is a zone-level check, not per-source.
- **In-scope zones**: a static list of every zone from the matrix above with ≥1 live source (35 country-level rows, expanded to 41 `bidding_zone` codes since IT counts as 7 ENTSO-E sub-zones). Hardcoded directly in the script rather than shared via `core/`, since scrapers may split into their own repos later — revisit as a `core/` constant only if that need actually arises.
- **Delivery-day bounds**: reuses the same `_day_bounds_utc()` pattern (pytz `localize()` + `.astimezone(utc)`) already duplicated across `entsoe`/`opcom`/`enex`, anchored to `Europe/Copenhagen` — the same single CET/CEST anchor every existing scraper uses, including for GB/IE (see the flat +1h DST assumption in Scheduling).
- **Timing**: `0 17 * * *` CET/CEST — after every live source's catch-up window for tomorrow's delivery day has closed (GB HalfHourly is the latest, ~15:30 CET).
- **Never fails the Prefect run** on missing data — a zone with zero rows is logged (and will be alerted, once the channel below is picked), not raised as an exception.
- **Alerting — open decision**: `send_alert()` currently only logs a warning listing the missing zones. Email vs. Teams (or something else) is not decided yet, left as a stub rather than guessed.
- **Not done yet**: no Prefect deployment/schedule created for this flow (same "design only" status as Scheduling).

**`monitoring/coverage.py`** — Streamlit prototype (run with `poetry run streamlit run monitoring/coverage.py`), for eyeballing "which zone from which scraper is in already" without querying the DB by hand. Separate from the completeness check above, not a replacement for it: interactive/manual, per-**source** granularity, no deployment/alerting of its own.
- Date picker for one delivery day at a time (default tomorrow), queries `PriceStore.get(market_type="DAY_AHEAD", ...)` for that day's UTC bounds, then groups results **by source** — each scraper gets its own block listing the zones it landed as chips, e.g. `OMIE` → `PT (96/96)`, `ES (96/96)`.
- Chip counts are actual vs. **expected** settlement periods (delivery-day UTC span ÷ that row's own resolution, so 23h/25h DST transition days are handled for free) — a source that landed only *some* of a zone's periods shows amber/partial rather than looking identical to full coverage.
- Grouped by `(source, bidding_zone, market)` before summing to actual/expected per zone, not straight to `(source, bidding_zone)` — GB lands as two separate `market` rows per source (N2EX hourly + GbHalfHour half-hourly) with different resolutions each; collapsing too early miscomputes `expected`.
- `IN_SCOPE_ZONES` duplicated from `monitoring/day_ahead_completeness.py` rather than shared via `core/` — same reasoning as that module, now with a second consumer.
- Sources are whatever `source` values actually appear that day, not a static per-zone expected list — a new source landing data shows up with no code change.
- **Known limitation, by design for now**: reads `prod.prices` only, so it shows *whether* data is in, not *why* it's missing (a source erroring vs. simply not having published yet look identical) — e.g. SEMO/IE's day-later batch publish will show as red without being a real gap. A logs-based system with actual run/error status is a separate, later piece of work; this dashboard would plug into that as an additional data source.

## Open items

- Migrate Nordpool to its gated v2 data portal — the free API's ~2-month rolling window is the main blocker to full backfill parity across all three main sources.
- Intraday scrapers (IDA1-3 auctions, ID1/ID3/FULL VWAPs) — schema already supports this via `market`.
- Market code reference/lookup table — only if free-text `market` values start causing problems; `id-tables-design.drawio` sketches an FK-based alternative (see Data model).
- Day-ahead volumes alongside prices — needs a schema decision (extend `prod.prices` vs. separate table); currently out of scope.
- CROPEX (HR), HUPX (HU), GME (IT), BSP Southpool (SI) — not started, blocked on paid/unconfirmed access (see Sources).
- No Prefect deployment/schedule exists yet for any flow — design only (see Scheduling), including the monitoring flow.
- Alert channel for `monitoring/day_ahead_completeness.py` (email vs. Teams) not decided — currently logs only.
- Re-enable publishing to `quent-data-stream` once `quent_core`'s streaming rework lands (see Streaming) — expected as a small add-on to `quent_core.database.price_store.PriceStore`, not a rebuild.

## Not to forget later

The `PREFECT_LOGGING_EXTRA_LOGGERS` setting (and a `PREFECT_HOME` override) live on the local dev machine's Prefect profile only. Once a work pool/deployment actually gets created, the same env vars need to be set wherever that worker runs (work pool job template or `prefect.yaml`'s `env:` block) — otherwise the worker process won't have them and UI logs will silently go back to being incomplete.
