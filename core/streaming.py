from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from quent_core.streaming import EventPublisher, NatsConfig

# same shared NATS endpoint + certs the "empire" producer uses (QUENTI_DATA_STREAMI/scrapers/empire/config.py) -
# team-shared infra, not machine-specific, per team decision.
NATS_URL = "tls://192.168.1.202:4222"
CERTS_DIR = r"C:\Users\SebastianWiesner\PycharmProjects\Quent-Production\Scrapers\QUENTI_DATA_STREAMI"

# own dedicated stream, not the shared DATA_PIPE (quent-data-stream gateway's quent.data.> catch-all was
# getting cramped). decision 2026-07-17: this means events are NOT reachable via the gateway's
# /replay, /ws, /hybrid routes unless/until the gateway is separately updated to also watch this stream -
# accepted as fine for now. subject prefix drops the quent.data. namespace since we're no longer inside
# the gateway's claimed filter.
# named PRICES, not DAY_AHEAD_AUCTION/AUCTION_PRICES - product/market already carry the day-ahead vs
# intraday and auction vs continuous-VWAP distinctions per-event, so the stream name doesn't need to.
STREAM = "PRICES"
SUBJECT_PREFIX = "prices"

SCHEMA_VERSION = 1
SOURCE_SYSTEM = "day_ahead_prices"


def subject_for_product(product: str) -> str:
    """one subject per product bucket (day_ahead, intraday, ...), shared across all zones/sources within it."""
    return f"{SUBJECT_PREFIX}.{product.lower()}.updates"


def build_nats_config(product: str) -> NatsConfig:
    return NatsConfig(
        url=NATS_URL,
        stream=STREAM,
        subject=subject_for_product(product),
        certs_dir=CERTS_DIR,
        connect_name="conn-producer-day-ahead-prices",
        # wildcard filter so the stream (created on first publish, quent_core's ensure_stream only
        # passes subjects=[stream_subject_filter] on creation) covers every product's subject from
        # the start, not just whichever product happens to publish first.
        stream_subjects=f"{SUBJECT_PREFIX}.>",
    )


def row_to_event(row: Any, subject: str) -> dict[str, Any]:
    """build an EventEnvelope-shaped dict from one prod.prices row (a pandas itertuples namedtuple)."""
    return {
        "schema_version": SCHEMA_VERSION,
        "subject": subject,
        "source_system": SOURCE_SYSTEM,
        "source": row.source,
        "event_type": f"{row.product.lower()}_price",
        "valuetime": row.valuetime.isoformat(),
        "snapshot_time": row.forecasttime.isoformat(),
        "data": {
            "bidding_zone": row.bidding_zone,
            "product": row.product,
            "market": row.market,
            "resolution": row.resolution,
            "currency": row.currency,
            "price": float(row.price),
        },
    }


def build_msg_id(subject: str, row: Any) -> str:
    """dedup key for JetStream.

    quent_core requires an explicit msg_id but doesn't derive one for us. subject:valuetime alone
    isn't enough here - every zone/source/market sharing one subject also shares valuetime within
    the same dump() batch, so use the full natural key instead (same columns as
    PriceStore.KEY_COLUMNS plus product).
    """
    return f"{subject}:{row.valuetime.isoformat()}:{row.bidding_zone}:{row.product}:{row.market}:{row.source}"


async def publish_events(df: pd.DataFrame, logger: logging.Logger) -> None:
    """publish one product-group of already-written prod.prices rows to quent-data-stream.

    connects, ensures the stream, publishes each row with an explicit msg_id, then closes.
    raises on failure - callers are responsible for catch-and-log so a NATS outage never
    fails the DB write that already committed.
    """
    if df.empty:
        return

    product = df["product"].iat[0]
    subject = subject_for_product(product)
    nats_cfg = build_nats_config(product)

    publisher = EventPublisher(nats_cfg, logger)
    try:
        await publisher.connect()
        events = [(row_to_event(row, subject), build_msg_id(subject, row)) for row in df.itertuples(index=False)]
        await publisher.publish_many(events)
        logger.info("published %d event(s) to %s", len(df), subject)
    finally:
        await publisher.close()
