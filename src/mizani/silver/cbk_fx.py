"""Silver transform: CBK forex rates.

Handles the real mess found in the published file (profiled 2026-07-15):
  * three date formats in ONE column: ISO (37,382 rows), DD/MM/YYYY
    (9 rows), and a mangled header fragment ('Date,US DOLLAR,STG POUND')
  * slash dates proven to be DD/MM by cross-format value matching
    (11/10/2016 rows carry identical rates to 2016-10-11 rows)
  * at least one future date (2038-01-22) — quarantined by range check
  * free-form currency labels, inconsistent spacing, one empty label
  * JPY quoted per 100 units; KES/x cross rates quoted as units-per-KES
    while majors are quoted KES-per-unit
"""

import numpy as np
import pandas as pd
import pandera.pandas as pa

SOURCE = "cbk_fx_csv"
BRONZE_TABLE = "cbk_fx_rates"
SILVER_TABLE = "fx_rates_daily"
BUSINESS_KEY = ["rate_date", "currency_code"]

KES_PER_UNIT = "KES_PER_UNIT"
UNITS_PER_KES = "UNITS_PER_KES"

# label (whitespace-collapsed, uppercased) -> (iso_code, quote_basis, units_per_quote)
CURRENCY_MAP = {
    "US DOLLAR": ("USD", KES_PER_UNIT, 1),
    "STG POUND": ("GBP", KES_PER_UNIT, 1),
    "EURO": ("EUR", KES_PER_UNIT, 1),
    "SA RAND": ("ZAR", KES_PER_UNIT, 1),
    "AE DIRHAM": ("AED", KES_PER_UNIT, 1),
    "CAN $": ("CAD", KES_PER_UNIT, 1),
    "S FRANC": ("CHF", KES_PER_UNIT, 1),
    "JPY (100)": ("JPY", KES_PER_UNIT, 100),
    "AUSTRALIAN $": ("AUD", KES_PER_UNIT, 1),
    "NOR KRONER": ("NOK", KES_PER_UNIT, 1),
    "SW KRONER": ("SEK", KES_PER_UNIT, 1),
    "DAN KRONER": ("DKK", KES_PER_UNIT, 1),
    "SAUDI RIYAL": ("SAR", KES_PER_UNIT, 1),
    "IND RUPEE": ("INR", KES_PER_UNIT, 1),
    "CHINESE YUAN": ("CNY", KES_PER_UNIT, 1),
    "SINGAPORE DOLLAR": ("SGD", KES_PER_UNIT, 1),
    "SINGAPORE $": ("SGD", KES_PER_UNIT, 1),
    "HONGKONG DOLLAR": ("HKD", KES_PER_UNIT, 1),
    "KES / TSHS": ("TZS", UNITS_PER_KES, 1),
    "KES / USHS": ("UGX", UNITS_PER_KES, 1),
    "KES/USHS": ("UGX", UNITS_PER_KES, 1),
    "KES / RWF": ("RWF", UNITS_PER_KES, 1),
    "KES / BIF": ("BIF", UNITS_PER_KES, 1),
}

KNOWN_CODES = sorted({iso for iso, _, _ in CURRENCY_MAP.values()})

SCHEMA = pa.DataFrameSchema(
    {
        "rate_date": pa.Column(
            "datetime64[ns]",
            nullable=False,
            checks=pa.Check.in_range(
                pd.Timestamp("2000-01-01"), pd.Timestamp.now().normalize()
            ),
        ),
        "currency_code": pa.Column(str, nullable=False, checks=pa.Check.isin(KNOWN_CODES)),
        "quote_basis": pa.Column(
            str, nullable=False, checks=pa.Check.isin([KES_PER_UNIT, UNITS_PER_KES])
        ),
        "units_per_quote": pa.Column("int64", nullable=False),
        "mean_rate": pa.Column(float, nullable=False, checks=pa.Check.gt(0)),
        "buy_rate": pa.Column(float, nullable=False, checks=pa.Check.gt(0)),
        "sell_rate": pa.Column(float, nullable=False, checks=pa.Check.gt(0)),
        "_source_row_hash": pa.Column(str, nullable=False),
    },
    checks=pa.Check(
        lambda df: df["buy_rate"] <= df["sell_rate"],
        name="buy_rate_lte_sell_rate",
    ),
    strict=True,
    coerce=True,
)


def _parse_mixed_dates(raw: pd.Series) -> pd.Series:
    iso = pd.to_datetime(raw, format="%Y-%m-%d", errors="coerce")
    slash = pd.to_datetime(raw, format="%d/%m/%Y", errors="coerce")
    return iso.fillna(slash)


def _to_number(raw: pd.Series) -> pd.Series:
    return pd.to_numeric(raw.str.replace(",", "", regex=False).str.strip(), errors="coerce")


def transform(bronze: pd.DataFrame) -> pd.DataFrame:
    """Bronze rows -> typed candidate rows. Index is preserved so failed
    rows can be traced back to their original bronze payload."""
    label = (
        bronze["currency_raw"]
        .fillna("")
        .str.upper()
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    mapped = label.map(CURRENCY_MAP)
    return pd.DataFrame(
        {
            "rate_date": _parse_mixed_dates(bronze["date_raw"]),
            "currency_code": mapped.map(lambda m: m[0] if isinstance(m, tuple) else None),
            "quote_basis": mapped.map(lambda m: m[1] if isinstance(m, tuple) else None),
            "units_per_quote": mapped.map(
                lambda m: m[2] if isinstance(m, tuple) else np.nan
            ).astype("float").fillna(-1).astype("int64"),
            "mean_rate": _to_number(bronze["mean_raw"]),
            "buy_rate": _to_number(bronze["buy_raw"]),
            "sell_rate": _to_number(bronze["sell_raw"]),
            "_source_row_hash": bronze["_source_row_hash"],
        },
        index=bronze.index,
    )
