"""GSMA Global Mobile Money Dataset extractor (XLSX).

The workbook is human-formatted: title rows, padding columns, sheet names
with trailing spaces, and metrics spread wide across ~100 quarterly date
columns. The 'All Data Table' sheet is machine-readable-ish: a real header
row, then one row per (measure, geography, metric) with quarter columns.
Bronze melts the wide quarter columns into (period_raw, value_raw) rows —
a lossless reshape, no value is touched — because a 100-column bronze
table would make hash-based idempotency break every time GSMA appends a
quarter.
"""

import io

import pandas as pd

from mizani.bronze import http
from mizani.config import GSMA_DATASET_XLSX

SOURCE = "gsma_xlsx"
TABLE = "gsma_mobile_money"

SHEET = "All Data Table"
ID_COLUMNS = ["Measure", "Geo_view", "Geo_name", "Attribute", "Unit", "Metric"]

COLUMNS = [
    "measure_raw",
    "geo_view_raw",
    "geo_name_raw",
    "attribute_raw",
    "unit_raw",
    "metric_raw",
    "period_raw",
    "value_raw",
]


def parse(xlsx_bytes: bytes) -> pd.DataFrame:
    wide = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name=SHEET, dtype=str)
    missing = [c for c in ID_COLUMNS if c not in wide.columns]
    if missing:
        raise ValueError(f"GSMA '{SHEET}' sheet is missing id columns: {missing}")

    long = wide.melt(
        id_vars=ID_COLUMNS,
        var_name="period_raw",
        value_name="value_raw",
    )
    long.columns = COLUMNS
    # empty cells in the wide grid are absent observations, not data
    long = long[long["value_raw"].notna() & (long["value_raw"].str.strip() != "")]
    if long.empty:
        raise ValueError(f"GSMA '{SHEET}' sheet melted to zero rows")
    return long.reset_index(drop=True)


def fetch() -> bytes:
    return http.get(GSMA_DATASET_XLSX).content


def extract() -> pd.DataFrame:
    return parse(fetch())
