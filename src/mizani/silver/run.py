"""Build the silver layer: transform bronze, validate, quarantine, dedup.

Silver is rebuilt deterministically from bronze on every run (bronze is
the incremental layer; silver and gold are pure functions of it). Steps
per source:

  1. transform: raw strings -> typed candidate rows (index-aligned with bronze)
  2. validate:  Pandera schema, lazy — failed rows quarantined with reasons
  3. dedup:     exact duplicates on business key + values collapse (the same
                fact republished in two date formats); rows that share a
                business key but DISAGREE on values are all quarantined as
                conflicting — the pipeline never guesses which one is true
"""

import logging
import sys
from datetime import datetime, timezone

import duckdb
import pandas as pd

from mizani.db import connect
from mizani.silver import cbk_fx, cbk_mobile, gsma, worldbank
from mizani.silver.quarantine import QUARANTINE_DDL, validate_and_split, write_quarantine

log = logging.getLogger("mizani.silver")

TRANSFORMS = [worldbank, cbk_fx, cbk_mobile, gsma]

SILVER_LOG_DDL = """
CREATE TABLE IF NOT EXISTS meta.silver_log (
    source           VARCHAR NOT NULL,
    target_table     VARCHAR NOT NULL,
    built_at         TIMESTAMP NOT NULL,
    rows_in          BIGINT NOT NULL,
    rows_clean       BIGINT NOT NULL,
    rows_quarantined BIGINT NOT NULL,
    rows_deduped     BIGINT NOT NULL,
    status           VARCHAR NOT NULL,
    detail           VARCHAR
)
"""


def _dedup(clean: pd.DataFrame, key: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Collapse exact duplicates; surface conflicting ones for quarantine."""
    value_cols = [c for c in clean.columns if not c.startswith("_")]
    collapsed = clean.drop_duplicates(subset=value_cols, keep="first")
    conflicts = collapsed[collapsed.duplicated(subset=key, keep=False)]
    return collapsed.drop(index=conflicts.index), conflicts


def build_source(con: duckdb.DuckDBPyConnection, module) -> dict:
    # silver (and its quarantine slate) is rebuilt per source: clear this
    # source's previous quarantine rows so re-runs don't accumulate
    con.execute(QUARANTINE_DDL)
    con.execute(
        "DELETE FROM silver.quarantine WHERE target_table = ?",
        [f"silver.{module.SILVER_TABLE}"],
    )
    bronze = con.execute(f"SELECT * FROM bronze.{module.BRONZE_TABLE}").fetchdf()
    candidate = module.transform(bronze)
    clean, failed, reasons = validate_and_split(candidate, module.SCHEMA)

    quarantined = write_quarantine(
        con, bronze.loc[failed.index], reasons, module.SOURCE, f"silver.{module.SILVER_TABLE}"
    )

    deduped, conflicts = _dedup(clean, module.BUSINESS_KEY)
    if not conflicts.empty:
        conflict_reasons = pd.Series(
            f"conflicting duplicate on business key {module.BUSINESS_KEY}",
            index=conflicts.index,
            dtype="string",
        )
        quarantined += write_quarantine(
            con,
            bronze.loc[conflicts.index],
            conflict_reasons,
            module.SOURCE,
            f"silver.{module.SILVER_TABLE}",
        )

    con.register("_silver_payload", deduped.reset_index(drop=True))
    con.execute(
        f"CREATE OR REPLACE TABLE silver.{module.SILVER_TABLE} AS "
        "SELECT * FROM _silver_payload"
    )
    con.unregister("_silver_payload")

    stats = {
        "rows_in": len(candidate),
        "rows_clean": len(deduped),
        "rows_quarantined": quarantined,
        "rows_deduped": len(clean) - len(conflicts) - len(deduped),
    }
    con.execute(SILVER_LOG_DDL)
    con.execute(
        "INSERT INTO meta.silver_log VALUES (?, ?, ?, ?, ?, ?, ?, 'ok', NULL)",
        [
            module.SOURCE,
            f"silver.{module.SILVER_TABLE}",
            datetime.now(timezone.utc).replace(tzinfo=None),
            stats["rows_in"],
            stats["rows_clean"],
            stats["rows_quarantined"],
            stats["rows_deduped"],
        ],
    )
    return stats


def run_all(db_path=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    con = connect(db_path)
    failures = 0
    for module in TRANSFORMS:
        try:
            stats = build_source(con, module)
            log.info(
                "%s -> silver.%s: in=%d clean=%d quarantined=%d deduped=%d",
                module.SOURCE,
                module.SILVER_TABLE,
                stats["rows_in"],
                stats["rows_clean"],
                stats["rows_quarantined"],
                stats["rows_deduped"],
            )
        except Exception as exc:
            failures += 1
            con.execute(SILVER_LOG_DDL)
            con.execute(
                "INSERT INTO meta.silver_log VALUES (?, ?, ?, 0, 0, 0, 0, 'error', ?)",
                [
                    module.SOURCE,
                    f"silver.{module.SILVER_TABLE}",
                    datetime.now(timezone.utc).replace(tzinfo=None),
                    repr(exc)[:2000],
                ],
            )
            log.error("%s FAILED: %r — continuing", module.SOURCE, exc)
    con.close()
    return failures


if __name__ == "__main__":
    sys.exit(1 if run_all() else 0)
