"""Bronze landing: write rows exactly as-received, with ingestion metadata.

The bronze contract:
  * every column from the source is stored as VARCHAR, untouched — no
    cleaning, no type coercion, no renaming beyond snake_case column labels
  * three metadata columns are appended:
      _source          logical source name
      _ingested_at     UTC timestamp of the landing run
      _source_row_hash sha256 of the raw row values, used for idempotency
  * landing is idempotent: a row whose hash already exists in the target
    table is skipped, so re-running an extract never duplicates data
  * every landing run is recorded in meta.ingestion_log with received /
    inserted / skipped counts and a status, so failures are loud
"""

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone

import duckdb
import pandas as pd

INGESTION_LOG_DDL = """
CREATE TABLE IF NOT EXISTS meta.ingestion_log (
    source        VARCHAR NOT NULL,
    target_table  VARCHAR NOT NULL,
    started_at    TIMESTAMP NOT NULL,
    rows_received BIGINT NOT NULL,
    rows_inserted BIGINT NOT NULL,
    rows_skipped  BIGINT NOT NULL,
    status        VARCHAR NOT NULL,
    detail        VARCHAR
)
"""


@dataclass(frozen=True)
class LandingResult:
    source: str
    target_table: str
    rows_received: int
    rows_inserted: int
    rows_skipped: int


def row_hash(values: list[str]) -> str:
    """Stable hash of a raw row. Unit separator avoids ('ab','c') == ('a','bc')."""
    joined = "\x1f".join("" if v is None else str(v) for v in values)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def land(
    con: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    source: str,
    table: str,
) -> LandingResult:
    """Idempotently insert a raw dataframe into bronze.<table>."""
    con.execute(INGESTION_LOG_DDL)
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    target = f"bronze.{table}"

    payload = df.astype("string").copy()
    payload["_source"] = source
    payload["_ingested_at"] = started_at
    payload["_source_row_hash"] = [
        row_hash(list(row)) for row in df.astype("string").itertuples(index=False, name=None)
    ]
    # source rows that are exact duplicates of each other collapse here too;
    # the received/inserted delta in the log keeps that visible
    payload = payload.drop_duplicates(subset="_source_row_hash")

    columns = ", ".join(f'"{c}" VARCHAR' for c in df.columns)
    con.execute(
        f"""CREATE TABLE IF NOT EXISTS {target} (
            {columns},
            _source VARCHAR NOT NULL,
            _ingested_at TIMESTAMP NOT NULL,
            _source_row_hash VARCHAR NOT NULL
        )"""
    )

    con.register("_landing_payload", payload)
    inserted = con.execute(
        f"""INSERT INTO {target}
            SELECT * FROM _landing_payload p
            WHERE NOT EXISTS (
                SELECT 1 FROM {target} t
                WHERE t._source_row_hash = p._source_row_hash
            )"""
    ).fetchone()[0]
    con.unregister("_landing_payload")

    result = LandingResult(
        source=source,
        target_table=target,
        rows_received=len(df),
        rows_inserted=inserted,
        rows_skipped=len(df) - inserted,
    )
    con.execute(
        "INSERT INTO meta.ingestion_log VALUES (?, ?, ?, ?, ?, ?, 'ok', NULL)",
        [source, target, started_at, result.rows_received, result.rows_inserted,
         result.rows_skipped],
    )
    return result


def log_failure(con: duckdb.DuckDBPyConnection, source: str, table: str, error: str) -> None:
    """Record a failed extraction so a missing source is loud, not silent."""
    con.execute(INGESTION_LOG_DDL)
    con.execute(
        "INSERT INTO meta.ingestion_log VALUES (?, ?, ?, 0, 0, 0, 'error', ?)",
        [source, f"bronze.{table}", datetime.now(timezone.utc).replace(tzinfo=None), error[:2000]],
    )
