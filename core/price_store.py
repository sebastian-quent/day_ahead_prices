import asyncio
import logging
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from quent_core.streaming import EventPublisher, NatsConfig
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

TABLE = "prod.prices"
PRIMARY_KEY = ["valuetime", "forecasttime", "bidding_zone", "market_type", "market", "source"]
KEY_COLUMNS = ["valuetime", "bidding_zone", "market_type", "market", "source"]  # PK minus forecasttime
VALUE_COLUMNS = ["resolution", "currency", "price"]
COLUMNS = PRIMARY_KEY + VALUE_COLUMNS

CHUNK_SIZE = 5000
PRICE_DECIMALS = 2  # no source publishes finer than cent-level precision

# same shared NATS endpoint the "empire" producer uses (QUENTI_DATA_STREAMI/scrapers/empire/config.py) -
# team-shared infra, not machine-specific, per team decision.
_NATS_URL = "tls://192.168.1.202:4222"
# ca-cert.pem only - server-auth TLS (ssl.Purpose.SERVER_AUTH), no client cert/key, so this is a public
# CA cert, not a credential. checked into the repo so the module works out of the box for anyone.
_CERTS_DIR = str(Path(__file__).parent / "certs")

# own dedicated stream, not the shared DATA_PIPE (quent-data-stream gateway's quent.data.> catch-all was
# getting cramped). decision 2026-07-17: this means events are NOT reachable via the gateway's
# /replay, /ws, /hybrid routes unless/until the gateway is separately updated to also watch this stream -
# accepted as fine for now. subject prefix drops the quent.data. namespace since we're no longer inside
# the gateway's claimed filter.
# named PRICES, not DAY_AHEAD_AUCTION/AUCTION_PRICES - market_type/market already carry the day-ahead vs
# intraday and auction vs continuous-VWAP distinctions per-event, so the stream name doesn't need to.
_STREAM = "PRICES"
_SUBJECT_PREFIX = "prices"

_SCHEMA_VERSION = 1
_SOURCE_SYSTEM = "day_ahead_prices"

_INSERT_SQL = text(
    f"""
    INSERT INTO {TABLE} ({', '.join(COLUMNS)})
    VALUES ({', '.join(f':{c}' for c in COLUMNS)})
    ON CONFLICT ({', '.join(PRIMARY_KEY)}) DO NOTHING
    """
)

_LATEST_KNOWN_SQL = text(
    f"""
    SELECT DISTINCT ON (valuetime, bidding_zone, market_type, market, source)
        valuetime, bidding_zone, market_type, market, source, price
    FROM {TABLE}
    WHERE source IN :sources
      AND market_type IN :market_types
      AND market IN :markets
      AND bidding_zone IN :bidding_zones
      AND valuetime BETWEEN :from_valuetime AND :to_valuetime
    ORDER BY valuetime, bidding_zone, market_type, market, source, forecasttime DESC
    """
).bindparams(
    bindparam("sources", expanding=True),
    bindparam("market_types", expanding=True),
    bindparam("markets", expanding=True),
    bindparam("bidding_zones", expanding=True),
)


class PriceStore:
    """dump/retrieve rows in prod.prices.

    prices are append-only: a rescrape only inserts a new row (with a new forecasttime)
    when the price actually differs from the latest known value for that valuetime/
    bidding_zone/market_type/market/source. an unchanged rescrape is skipped rather than
    written again, so forecasttime marks "when this price last changed", not "when we
    last checked". takes the shared engine as a constructor arg rather than building its
    own connection, e.g. `PriceStore(engine)` with `from Database.db_connect import engine`.
    """

    def __init__(self, engine: Engine, publish: bool = True):
        self.engine = engine
        self._publish_default = publish

    def dump(self, df: pd.DataFrame, publish: Optional[bool] = None) -> int:
        """insert new/changed price rows into prod.prices, then publish them to quent-data-stream.

        looks up the latest known price per key with a single query (not one query per
        row), keeps only rows that are new or whose price differs from that known value,
        then inserts them chunked so one failing chunk is logged and skipped instead of
        rolling back the whole batch. returns the number of rows written.

        only rows that actually committed to the DB are published (never a chunk that
        raised). `publish` overrides the instance's default for this call - e.g. a future
        historical backfill can pass publish=False so it doesn't replay old rows onto the
        live stream. a NATS outage is caught and logged, never fails the DB write above it.
        """
        if df.empty:
            return 0

        missing = set(COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"PriceStore.dump: missing required columns {sorted(missing)}")

        df = df[COLUMNS]
        _require_utc(df, "valuetime")
        _require_utc(df, "forecasttime")

        df = df.copy()
        df["forecasttime"] = df["forecasttime"].dt.floor("s")
        df["price"] = df["price"].round(PRICE_DECIMALS)

        to_write = self._changed_rows(df)
        skipped = len(df) - len(to_write)
        if skipped:
            logger.info("PriceStore.dump: skipping %d unchanged row(s)", skipped)
        if to_write.empty:
            return 0

        written = 0
        written_chunks: list[pd.DataFrame] = []
        for start in range(0, len(to_write), CHUNK_SIZE):
            chunk = to_write.iloc[start : start + CHUNK_SIZE]
            try:
                with self.engine.begin() as conn:
                    conn.execute(_INSERT_SQL, chunk.to_dict(orient="records"))
                written += len(chunk)
                written_chunks.append(chunk)
            except Exception:
                logger.error(
                    "PriceStore.dump: failed to write rows %d-%d (source=%s, bidding_zone=%s)",
                    start,
                    start + len(chunk),
                    chunk["source"].iat[0],
                    sorted(chunk["bidding_zone"].unique()),
                    exc_info=True,
                )

        should_publish = self._publish_default if publish is None else publish
        if should_publish and written_chunks:
            self._publish(pd.concat(written_chunks, ignore_index=True))

        return written

    def _publish(self, written: pd.DataFrame) -> None:
        """publish written rows to quent-data-stream, one market_type group (=one subject) at a time.

        each group is isolated so a failure publishing one market_type doesn't block another.
        """
        for market_type, group in written.groupby("market_type"):
            try:
                asyncio.run(_publish_events(group, logger))
            except Exception:
                logger.error(
                    "PriceStore.dump: failed to publish %d row(s) for market_type=%s to quent-data-stream",
                    len(group),
                    market_type,
                    exc_info=True,
                )

    def _changed_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """rows in df that are new or whose price differs from the latest known value."""
        latest_known = pd.read_sql(
            _LATEST_KNOWN_SQL,
            self.engine,
            params={
                "sources": df["source"].unique().tolist(),
                "market_types": df["market_type"].unique().tolist(),
                "markets": df["market"].unique().tolist(),
                "bidding_zones": df["bidding_zone"].unique().tolist(),
                "from_valuetime": df["valuetime"].min(),
                "to_valuetime": df["valuetime"].max(),
            },
        )
        if latest_known.empty:
            return df

        latest_known["valuetime"] = pd.to_datetime(latest_known["valuetime"], utc=True)
        merged = df.merge(
            latest_known.rename(columns={"price": "known_price"}),
            on=KEY_COLUMNS,
            how="left",
        )
        changed = merged["known_price"].isna() | (
            merged["price"].round(PRICE_DECIMALS) != merged["known_price"].round(PRICE_DECIMALS)
        )
        return merged.loc[changed, COLUMNS].reset_index(drop=True)

    def get(
        self,
        bidding_zone: Optional[str] = None,
        market_type: Optional[str] = None,
        market: Optional[str] = None,
        source: Optional[str] = None,
        from_valuetime: Optional[pd.Timestamp] = None,
        to_valuetime: Optional[pd.Timestamp] = None,
        latest_only: bool = True,
    ) -> pd.DataFrame:
        """retrieve price rows from prod.prices as a plain DataFrame (valuetime stays a column, not the index).

        by default collapses to the latest forecasttime per valuetime/bidding_zone/market_type/market/source,
        i.e. the current known price curve. set latest_only=False for every scrape snapshot.
        """
        filters, params = [], {}
        for column, value in (
            ("bidding_zone", bidding_zone),
            ("market_type", market_type),
            ("market", market),
            ("source", source),
        ):
            if value is not None:
                filters.append(f"{column} = :{column}")
                params[column] = value
        if from_valuetime is not None:
            filters.append("valuetime >= :from_valuetime")
            params["from_valuetime"] = from_valuetime
        if to_valuetime is not None:
            filters.append("valuetime < :to_valuetime")
            params["to_valuetime"] = to_valuetime
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

        if latest_only:
            query = f"""
                SELECT DISTINCT ON (valuetime, bidding_zone, market_type, market, source)
                    {', '.join(COLUMNS)}
                FROM {TABLE}
                {where_clause}
                ORDER BY valuetime, bidding_zone, market_type, market, source, forecasttime DESC
            """
        else:
            query = f"""
                SELECT {', '.join(COLUMNS)}
                FROM {TABLE}
                {where_clause}
                ORDER BY valuetime
            """

        df = pd.read_sql(text(query), self.engine, params=params)
        df["valuetime"] = pd.to_datetime(df["valuetime"], utc=True)
        df["forecasttime"] = pd.to_datetime(df["forecasttime"], utc=True)
        return df


def _require_utc(df: pd.DataFrame, column: str) -> None:
    dtype = df[column].dtype
    if not isinstance(dtype, pd.DatetimeTZDtype):
        logger.error("PriceStore.dump: rejecting batch, '%s' is not a tz-aware timestamp column", column)
        raise ValueError(f"PriceStore.dump: column '{column}' must be tz-aware UTC, got dtype {dtype}")


def _subject_for_market_type(market_type: str) -> str:
    """one subject per market_type bucket (day_ahead, intraday, ...), shared across all zones/sources within it."""
    return f"{_SUBJECT_PREFIX}.{market_type.lower()}.updates"


def _build_nats_config(market_type: str) -> NatsConfig:
    return NatsConfig(
        url=_NATS_URL,
        stream=_STREAM,
        subject=_subject_for_market_type(market_type),
        certs_dir=_CERTS_DIR,
        connect_name="conn-producer-day-ahead-prices",
        # wildcard filter so the stream (created on first publish, quent_core's ensure_stream only
        # passes subjects=[stream_subject_filter] on creation) covers every market_type's subject from
        # the start, not just whichever market_type happens to publish first.
        stream_subjects=f"{_SUBJECT_PREFIX}.>",
    )


def _row_to_event(row: Any, subject: str) -> dict[str, Any]:
    """build an EventEnvelope-shaped dict from one prod.prices row (a pandas itertuples namedtuple)."""
    return {
        "schema_version": _SCHEMA_VERSION,
        "subject": subject,
        "source_system": _SOURCE_SYSTEM,
        "source": row.source,
        "event_type": f"{row.market_type.lower()}_price",
        "valuetime": row.valuetime.isoformat(),
        "snapshot_time": row.forecasttime.isoformat(),
        "data": {
            "bidding_zone": row.bidding_zone,
            "market_type": row.market_type,
            "market": row.market,
            "resolution": row.resolution,
            "currency": row.currency,
            "price": float(row.price),
        },
    }


def _build_msg_id(subject: str, row: Any) -> str:
    """dedup key for JetStream.

    quent_core requires an explicit msg_id but doesn't derive one for us. subject:valuetime alone
    isn't enough here - every zone/source/market sharing one subject also shares valuetime within
    the same dump() batch, so use the full natural key instead (same columns as
    PriceStore.KEY_COLUMNS plus market_type).
    """
    return f"{subject}:{row.valuetime.isoformat()}:{row.bidding_zone}:{row.market_type}:{row.market}:{row.source}"


async def _publish_events(df: pd.DataFrame, logger: logging.Logger) -> None:
    """publish one market_type-group of already-written prod.prices rows to quent-data-stream.

    connects, ensures the stream, publishes each row with an explicit msg_id, then closes.
    raises on failure - callers are responsible for catch-and-log so a NATS outage never
    fails the DB write that already committed.
    """
    if df.empty:
        return

    market_type = df["market_type"].iat[0]
    subject = _subject_for_market_type(market_type)
    nats_cfg = _build_nats_config(market_type)

    publisher = EventPublisher(nats_cfg, logger)
    try:
        await publisher.connect()
        events = [(_row_to_event(row, subject), _build_msg_id(subject, row)) for row in df.itertuples(index=False)]
        await publisher.publish_many(events)
        logger.info("published %d event(s) to %s", len(df), subject)
    finally:
        await publisher.close()
