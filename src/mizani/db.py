"""DuckDB connection helper and schema bootstrap."""

from pathlib import Path

import duckdb

from mizani.config import DB_PATH

SCHEMAS = ("bronze", "silver", "gold", "meta")


def connect(db_path: str | Path | None = None) -> duckdb.DuckDBPyConnection:
    """Open the warehouse and make sure the medallion schemas exist."""
    path = Path(db_path) if db_path is not None else DB_PATH
    if str(path) != ":memory:":
        path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    for schema in SCHEMAS:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    return con
