import logging
from typing import Optional

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

TABLE = "prod.prices"
PRIMARY_KEY = ["valuetime", "forecasttime", "bidding_zone", "product", "market", "source"]
KEY_COLUMNS = ["valuetime", "bidding_zone", "product", "market", "source"]  # PK minus forecasttime
VALUE_COLUMNS = ["resolution", "currency", "price"]
COLUMNS = PRIMARY_KEY + VALUE_COLUMNS

CHUNK_SIZE = 5000
PRICE_DECIMALS = 2  # no source publishes finer than cent-level precision

_INSERT_SQL = text(
    f"""
    INSERT INTO {TABLE} ({', '.join(COLUMNS)})
    VALUES ({', '.join(f':{c}' for c in COLUMNS)})
    ON CONFLICT ({', '.join(PRIMARY_KEY)}) DO NOTHING
    """
)

_LATEST_KNOWN_SQL = text(
    f"""
    SELECT DISTINCT ON (valuetime, bidding_zone, product, market, source)
        valuetime, bidding_zone, product, market, source, price
    FROM {TABLE}
    WHERE source IN :sources
      AND product IN :products
      AND market IN :markets
      AND bidding_zone IN :bidding_zones
      AND valuetime BETWEEN :from_valuetime AND :to_valuetime
    ORDER BY valuetime, bidding_zone, product, market, source, forecasttime DESC
    """
).bindparams(
    bindparam("sources", expanding=True),
    bindparam("products", expanding=True),
    bindparam("markets", expanding=True),
    bindparam("bidding_zones", expanding=True),
)


class PriceStore:
    """dump/retrieve rows in prod.prices.

    prices are append-only: a rescrape only inserts a new row (with a new forecasttime)
    when the price actually differs from the latest known value for that valuetime/
    bidding_zone/product/market/source. an unchanged rescrape is skipped rather than
    written again, so forecasttime marks "when this price last changed", not "when we
    last checked". takes the shared engine as a constructor arg rather than building its
    own connection, e.g. `PriceStore(engine)` with `from Database.db_connect import engine`.
    """

    def __init__(self, engine: Engine):
        self.engine = engine

    def dump(self, df: pd.DataFrame) -> int:
        """insert new/changed price rows into prod.prices.

        looks up the latest known price per key with a single query (not one query per
        row), keeps only rows that are new or whose price differs from that known value,
        then inserts them chunked so one failing chunk is logged and skipped instead of
        rolling back the whole batch. returns the number of rows written.
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
        for start in range(0, len(to_write), CHUNK_SIZE):
            chunk = to_write.iloc[start : start + CHUNK_SIZE]
            try:
                with self.engine.begin() as conn:
                    conn.execute(_INSERT_SQL, chunk.to_dict(orient="records"))
                written += len(chunk)
            except Exception:
                logger.error(
                    "PriceStore.dump: failed to write rows %d-%d (source=%s, bidding_zone=%s)",
                    start,
                    start + len(chunk),
                    chunk["source"].iat[0],
                    sorted(chunk["bidding_zone"].unique()),
                    exc_info=True,
                )
        return written

    def _changed_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """rows in df that are new or whose price differs from the latest known value."""
        latest_known = pd.read_sql(
            _LATEST_KNOWN_SQL,
            self.engine,
            params={
                "sources": df["source"].unique().tolist(),
                "products": df["product"].unique().tolist(),
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
        product: Optional[str] = None,
        market: Optional[str] = None,
        source: Optional[str] = None,
        from_valuetime: Optional[pd.Timestamp] = None,
        to_valuetime: Optional[pd.Timestamp] = None,
        latest_only: bool = True,
    ) -> pd.DataFrame:
        """retrieve price rows from prod.prices as a plain DataFrame (valuetime stays a column, not the index).

        by default collapses to the latest forecasttime per valuetime/bidding_zone/product/market/source,
        i.e. the current known price curve. set latest_only=False for every scrape snapshot.
        """
        filters, params = [], {}
        for column, value in (
            ("bidding_zone", bidding_zone),
            ("product", product),
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
                SELECT DISTINCT ON (valuetime, bidding_zone, product, market, source)
                    {', '.join(COLUMNS)}
                FROM {TABLE}
                {where_clause}
                ORDER BY valuetime, bidding_zone, product, market, source, forecasttime DESC
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
