"""Silver transform: GSMA global mobile money metrics.

Period column labels are DD/MM/YYYY quarter-end markers (01/03/2001 =
Q1 2001). The 'All Data Table' sheet carries Region/Subregion geographies
only — country-level GSMA data is not redistributed in this workbook.
"""

import pandas as pd
import pandera.pandas as pa

SOURCE = "gsma_xlsx"
BRONZE_TABLE = "gsma_mobile_money"
SILVER_TABLE = "gsma_metrics_quarterly"
BUSINESS_KEY = ["measure", "geo_view", "geo_name", "attribute", "unit", "metric", "period_quarter"]

SCHEMA = pa.DataFrameSchema(
    {
        "measure": pa.Column(str, nullable=False),
        "geo_view": pa.Column(
            str, nullable=False, checks=pa.Check.isin(["Region", "Subregion"])
        ),
        "geo_name": pa.Column(str, nullable=False),
        "attribute": pa.Column(str, nullable=False),
        "unit": pa.Column(str, nullable=False),
        "metric": pa.Column(str, nullable=False),
        "period_quarter": pa.Column(
            "datetime64[ns]",
            nullable=False,
            checks=pa.Check.in_range(
                pd.Timestamp("2001-01-01"), pd.Timestamp.now().normalize()
            ),
        ),
        "value": pa.Column(float, nullable=False, checks=pa.Check.ge(0)),
        "_source_row_hash": pa.Column(str, nullable=False),
    },
    strict=True,
    coerce=True,
)


def transform(bronze: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "measure": bronze["measure_raw"].str.strip(),
            "geo_view": bronze["geo_view_raw"].str.strip(),
            "geo_name": bronze["geo_name_raw"].str.strip(),
            "attribute": bronze["attribute_raw"].str.strip(),
            "unit": bronze["unit_raw"].str.strip(),
            "metric": bronze["metric_raw"].str.strip(),
            "period_quarter": pd.to_datetime(
                bronze["period_raw"], format="%d/%m/%Y", errors="coerce"
            ),
            "value": pd.to_numeric(bronze["value_raw"], errors="coerce"),
            "_source_row_hash": bronze["_source_row_hash"],
        },
        index=bronze.index,
    )
