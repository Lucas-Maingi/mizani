"""Silver validation tests: transforms, rules that SHOULD reject rows,
quarantine bookkeeping, and semantic dedup. All offline."""

import json

import pandas as pd

from mizani.silver import cbk_fx, cbk_mobile, gsma, worldbank
from mizani.silver.quarantine import validate_and_split, write_quarantine
from mizani.silver.run import _dedup, build_source


def _fx_bronze(rows):
    df = pd.DataFrame(
        rows, columns=["date_raw", "currency_raw", "mean_raw", "buy_raw", "sell_raw"]
    )
    df["_source_row_hash"] = [f"hash{i}" for i in range(len(df))]
    return df


GOOD_FX_ROW = ["2024-01-03", "US DOLLAR", "157.5", "157.4", "157.6"]


# --- CBK fx transform ---


def test_fx_parses_both_real_date_formats():
    out = cbk_fx.transform(
        _fx_bronze([GOOD_FX_ROW, ["11/10/2016", "AE DIRHAM", "27.5", "27.4", "27.6"]])
    )
    assert out["rate_date"].tolist() == [
        pd.Timestamp("2024-01-03"),
        pd.Timestamp("2016-10-11"),  # DD/MM, as proven by cross-format matching
    ]


def test_fx_maps_messy_currency_labels():
    out = cbk_fx.transform(
        _fx_bronze(
            [
                ["2024-01-03", "CAN $", "116", "115", "117"],
                ["2024-01-03", "KES/USHS", "24", "23", "25"],  # spacing variant
                ["2024-01-03", "  us  dollar ", "157", "156", "158"],  # case/space noise
                ["2024-01-03", "JPY (100)", "108", "107", "109"],
            ]
        )
    )
    assert out["currency_code"].tolist() == ["CAD", "UGX", "USD", "JPY"]
    assert out["quote_basis"].tolist()[1] == cbk_fx.UNITS_PER_KES
    assert out["units_per_quote"].tolist()[3] == 100


def test_fx_validation_rejects_the_documented_mess():
    bronze = _fx_bronze(
        [
            GOOD_FX_ROW,
            ["Date", "US DOLLAR", "STG POUND", None, None],  # real header fragment
            ["2038-01-22", "AUSTRALIAN $", "82.3", "82.2", "82.4"],  # real future date
            ["2024-01-03", "GALLEON", "1", "1", "1"],  # unknown label
            ["2024-01-03", "EURO", "171", "172", "170"],  # buy > sell
            ["2024-01-03", "SA RAND", "-8.5", "8.4", "8.6"],  # negative rate
        ]
    )
    clean, failed, reasons = validate_and_split(cbk_fx.transform(bronze), cbk_fx.SCHEMA)
    assert len(clean) == 1
    assert len(failed) == 5
    joined = " | ".join(reasons)
    assert "rate_date: not_nullable" in joined
    assert "in_range" in joined  # future date
    assert "currency_code: not_nullable" in joined  # unknown label maps to null
    assert "buy_rate_lte_sell_rate" in joined
    assert "mean_rate: greater_than(0)" in joined


# --- dedup ---


def test_dedup_collapses_republished_rows_and_quarantines_conflicts():
    clean = cbk_fx.transform(
        _fx_bronze(
            [
                GOOD_FX_ROW,
                ["03/01/2024", "US DOLLAR", "157.5", "157.4", "157.6"],  # same fact, other format
                ["2024-01-03", "EURO", "171", "170", "172"],
                ["2024-01-03", "EURO", "173", "172", "174"],  # conflicting revision
            ]
        )
    )
    deduped, conflicts = _dedup(clean, cbk_fx.BUSINESS_KEY)
    assert len(deduped) == 1  # only the USD fact survives
    assert set(conflicts["currency_code"]) == {"EUR"}
    assert len(conflicts) == 2  # both conflicting versions quarantined, no guessing


# --- CBK mobile ---


def test_mobile_transform_parses_months_and_commas():
    bronze = pd.DataFrame(
        [["2026", "May", "564,330", "94.09", "214.33", "681.45"]],
        columns=[
            "year_raw",
            "month_raw",
            "active_agents_raw",
            "registered_accounts_millions_raw",
            "agent_cico_volume_million_raw",
            "agent_cico_value_ksh_billions_raw",
        ],
    )
    bronze["_source_row_hash"] = ["h"]
    out = cbk_mobile.transform(bronze)
    assert out.iloc[0]["period_month"] == pd.Timestamp("2026-05-01")
    assert out.iloc[0]["active_agents"] == 564330


def test_mobile_validation_rejects_bad_month_and_negative_agents():
    bronze = pd.DataFrame(
        [
            ["2026", "Maytember", "100", "1", "1", "1"],
            ["2026", "May", "-5", "1", "1", "1"],
        ],
        columns=[
            "year_raw",
            "month_raw",
            "active_agents_raw",
            "registered_accounts_millions_raw",
            "agent_cico_volume_million_raw",
            "agent_cico_value_ksh_billions_raw",
        ],
    )
    bronze["_source_row_hash"] = ["h1", "h2"]
    clean, failed, _ = validate_and_split(cbk_mobile.transform(bronze), cbk_mobile.SCHEMA)
    assert clean.empty
    assert len(failed) == 2


# --- World Bank ---


def test_worldbank_missing_observations_are_quarantined_not_dropped(con):
    bronze = pd.DataFrame(
        {
            "indicator_id": ["PA.NUS.FCRF"] * 2,
            "indicator_name": ["x"] * 2,
            "country_iso3": ["KEN", "KEN"],
            "country_name": ["Kenya"] * 2,
            "year": ["2024", "2025"],
            "value": ["144.4", None],
            "unit": ["", ""],
            "obs_status": ["", ""],
            "_source_row_hash": ["h1", "h2"],
        }
    )
    clean, failed, reasons = validate_and_split(worldbank.transform(bronze), worldbank.SCHEMA)
    assert len(clean) == 1
    assert reasons.iloc[0] == "value: not_nullable"

    n = write_quarantine(con, bronze.loc[failed.index], reasons, "worldbank_api", "silver.t")
    assert n == 1
    payload, hash_ = con.execute(
        "SELECT raw_payload, _source_row_hash FROM silver.quarantine"
    ).fetchone()
    assert json.loads(payload)["year"] == "2025"
    assert hash_ == "h2"


# --- GSMA ---


def test_gsma_period_labels_parse_as_day_first():
    bronze = pd.DataFrame(
        {
            "measure_raw": ["Active Services"],
            "geo_view_raw": ["Region"],
            "geo_name_raw": ["Sub-Saharan Africa "],
            "attribute_raw": ["Active Services"],
            "unit_raw": ["Services"],
            "metric_raw": ["Mobile Money Services"],
            "period_raw": ["01/12/2004"],
            "value_raw": ["12"],
            "_source_row_hash": ["h"],
        }
    )
    out = gsma.transform(bronze)
    assert out.iloc[0]["period_quarter"] == pd.Timestamp("2004-12-01")
    assert out.iloc[0]["geo_name"] == "Sub-Saharan Africa"  # trailing space stripped


# --- end-to-end build over a duckdb connection ---


def test_build_source_writes_silver_and_quarantine(con):
    bronze = _fx_bronze([GOOD_FX_ROW, ["Date", "US DOLLAR", "STG POUND", None, None]])
    con.register("bronze_df", bronze)
    con.execute("CREATE TABLE bronze.cbk_fx_rates AS SELECT * FROM bronze_df")
    stats = build_source(con, cbk_fx)
    assert stats == {"rows_in": 2, "rows_clean": 1, "rows_quarantined": 1, "rows_deduped": 0}
    assert con.execute("SELECT count(*) FROM silver.fx_rates_daily").fetchone()[0] == 1
    assert con.execute("SELECT count(*) FROM silver.quarantine").fetchone()[0] == 1
    assert con.execute("SELECT count(*) FROM meta.silver_log").fetchone()[0] == 1


def test_validate_and_split_empty_frame():
    empty = cbk_fx.transform(_fx_bronze([]))
    clean, failed, reasons = validate_and_split(empty, cbk_fx.SCHEMA)
    assert clean.empty and failed.empty and reasons.empty
