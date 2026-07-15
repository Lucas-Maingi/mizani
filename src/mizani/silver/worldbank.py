"""Silver transform: World Bank indicators.

Null values are legitimate absent observations in the API response; they
are quarantined (reason: value not_nullable) rather than silently dropped,
so the gap count stays auditable.
"""

import pandas as pd
import pandera.pandas as pa

from mizani.config import WORLDBANK_COUNTRIES, WORLDBANK_INDICATORS

SOURCE = "worldbank_api"
BRONZE_TABLE = "worldbank_indicators"
SILVER_TABLE = "worldbank_annual"
BUSINESS_KEY = ["indicator_code", "country_iso3", "year"]

SCHEMA = pa.DataFrameSchema(
    {
        "indicator_code": pa.Column(
            str, nullable=False, checks=pa.Check.isin(sorted(WORLDBANK_INDICATORS))
        ),
        "country_iso3": pa.Column(
            str, nullable=False, checks=pa.Check.isin(sorted(WORLDBANK_COUNTRIES))
        ),
        "year": pa.Column("int64", nullable=False, checks=pa.Check.in_range(1960, 2030)),
        "value": pa.Column(float, nullable=False, checks=pa.Check.ge(0)),
        "_source_row_hash": pa.Column(str, nullable=False),
    },
    strict=True,
    coerce=True,
)


def transform(bronze: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "indicator_code": bronze["indicator_id"],
            "country_iso3": bronze["country_iso3"],
            "year": pd.to_numeric(bronze["year"], errors="coerce").fillna(-1).astype("int64"),
            "value": pd.to_numeric(bronze["value"], errors="coerce"),
            "_source_row_hash": bronze["_source_row_hash"],
        },
        index=bronze.index,
    )
