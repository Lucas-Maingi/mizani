"""Quarantine: rows that fail validation are kept, with reasons — never dropped.

Every silver transform returns (clean, quarantined). Quarantined rows carry
the original raw payload as JSON plus a semicolon-joined reason list, so a
human can inspect exactly what was wrong and re-process after a fix.
"""

import json
from datetime import datetime, timezone

import duckdb
import pandas as pd
import pandera.pandas as pa

QUARANTINE_DDL = """
CREATE TABLE IF NOT EXISTS silver.quarantine (
    source           VARCHAR NOT NULL,
    target_table     VARCHAR NOT NULL,
    quarantined_at   TIMESTAMP NOT NULL,
    reasons          VARCHAR NOT NULL,
    raw_payload      VARCHAR NOT NULL,
    _source_row_hash VARCHAR
)
"""


def validate_and_split(
    df: pd.DataFrame, schema: pa.DataFrameSchema
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
    """Run a Pandera schema lazily; return (clean, failed, reasons_per_failed_row).

    Reasons aggregate every failed check for a row, e.g.
    'rate_date: greater_than_or_equal_to(1993-01-01); mean_rate: not_nullable'.
    """
    if df.empty:
        return df, df.head(0), pd.Series(dtype="string")
    try:
        clean = schema.validate(df, lazy=True)
        return clean, df.head(0), pd.Series(dtype="string")
    except pa.errors.SchemaErrors as err:
        failures = err.failure_cases
        # index of the offending row in `df`; schema-level errors have no index
        failures = failures[failures["index"].notna()]
        # dataframe-level checks are reported once per column; name them once
        df_level = failures["schema_context"].astype(str) == "DataFrameSchema"
        reasons = (
            failures.assign(
                reason=lambda f: f["check"].astype(str).where(
                    df_level, f["column"].astype(str) + ": " + f["check"].astype(str)
                )
            )
            .groupby("index")["reason"]
            .agg(lambda r: "; ".join(sorted(set(r))))
        )
        bad_idx = reasons.index
        clean = schema.validate(df.drop(index=bad_idx), lazy=False)
        return clean, df.loc[bad_idx], reasons.astype("string")


def write_quarantine(
    con: duckdb.DuckDBPyConnection,
    raw_rows: pd.DataFrame,
    reasons: pd.Series,
    source: str,
    target_table: str,
) -> int:
    """Persist failed rows (as their ORIGINAL bronze payload) with reasons."""
    con.execute(QUARANTINE_DDL)
    if raw_rows.empty:
        return 0
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    hashes = (
        raw_rows["_source_row_hash"]
        if "_source_row_hash" in raw_rows.columns
        else pd.Series([None] * len(raw_rows), index=raw_rows.index)
    )
    records = pd.DataFrame(
        {
            "source": source,
            "target_table": target_table,
            "quarantined_at": now,
            "reasons": reasons.reindex(raw_rows.index).fillna("unspecified"),
            "raw_payload": raw_rows.drop(
                columns=[c for c in raw_rows.columns if c.startswith("_")]
            ).apply(lambda r: json.dumps(r.to_dict(), default=str), axis=1),
            "_source_row_hash": hashes,
        }
    )
    con.register("_quarantine_payload", records)
    con.execute("INSERT INTO silver.quarantine SELECT * FROM _quarantine_payload")
    con.unregister("_quarantine_payload")
    return len(records)
