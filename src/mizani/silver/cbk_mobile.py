"""Silver transform: CBK monthly mobile payments statistics."""

import pandas as pd
import pandera.pandas as pa

SOURCE = "cbk_mobile_html"
BRONZE_TABLE = "cbk_mobile_payments"
SILVER_TABLE = "mobile_payments_monthly"
BUSINESS_KEY = ["period_month"]

MONTHS = {
    "JANUARY": 1, "FEBRUARY": 2, "MARCH": 3, "APRIL": 4, "MAY": 5, "JUNE": 6,
    "JULY": 7, "AUGUST": 8, "SEPTEMBER": 9, "OCTOBER": 10, "NOVEMBER": 11, "DECEMBER": 12,
}

SCHEMA = pa.DataFrameSchema(
    {
        "period_month": pa.Column(
            "datetime64[ns]",
            nullable=False,
            checks=pa.Check.in_range(
                pd.Timestamp("2007-03-01"), pd.Timestamp.now().normalize()
            ),
        ),
        "active_agents": pa.Column("int64", nullable=False, checks=pa.Check.ge(0)),
        "registered_accounts_millions": pa.Column(
            float, nullable=False, checks=pa.Check.in_range(0, 500)
        ),
        "agent_cico_volume_million": pa.Column(float, nullable=False, checks=pa.Check.ge(0)),
        "agent_cico_value_ksh_billions": pa.Column(float, nullable=False, checks=pa.Check.ge(0)),
        "_source_row_hash": pa.Column(str, nullable=False),
    },
    strict=True,
    coerce=True,
)


def _to_number(raw: pd.Series) -> pd.Series:
    return pd.to_numeric(raw.str.replace(",", "", regex=False).str.strip(), errors="coerce")


def transform(bronze: pd.DataFrame) -> pd.DataFrame:
    year = pd.to_numeric(bronze["year_raw"].str.strip(), errors="coerce")
    month = bronze["month_raw"].fillna("").str.upper().str.strip().map(MONTHS)
    period = pd.to_datetime(
        {
            "year": year,
            "month": month,
            "day": 1,
        },
        errors="coerce",
    )
    agents = _to_number(bronze["active_agents_raw"])
    return pd.DataFrame(
        {
            "period_month": period,
            # int64 can't hold NaN: keep unparseable as -1, which the ge(0) check
            # then quarantines with a visible reason instead of a cast crash
            "active_agents": agents.fillna(-1).astype("int64"),
            "registered_accounts_millions": _to_number(
                bronze["registered_accounts_millions_raw"]
            ),
            "agent_cico_volume_million": _to_number(bronze["agent_cico_volume_million_raw"]),
            "agent_cico_value_ksh_billions": _to_number(
                bronze["agent_cico_value_ksh_billions_raw"]
            ),
            "_source_row_hash": bronze["_source_row_hash"],
        },
        index=bronze.index,
    )
