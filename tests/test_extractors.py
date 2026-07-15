"""Offline parse tests against committed real-data fixtures.

Fixtures are unmodified subsets of the real sources, captured 2026-07-15.
No test here touches the network.
"""

import json

import pytest

from mizani.bronze import cbk_fx, cbk_mobile, gsma, worldbank

# --- World Bank ---


def test_worldbank_parse_flattens_records(fixtures_dir):
    page = json.loads((fixtures_dir / "worldbank_fcrf_sample.json").read_text(encoding="utf-8"))
    df = worldbank.parse([page])
    assert list(df.columns) == worldbank.COLUMNS
    assert len(df) == 14  # 2 countries x 7 years, per the captured response
    assert set(df["country_iso3"]) == {"KEN", "TZA"}
    assert (df["indicator_id"] == "PA.NUS.FCRF").all()


def test_worldbank_parse_tolerates_empty_page():
    meta_only = [{"page": 1, "pages": 1, "total": 0}, None]
    assert worldbank.parse([meta_only]).empty


# --- CBK forex CSV ---


def test_cbk_fx_parse_headerless_csv(fixtures_dir):
    text = (fixtures_dir / "cbk_fx_sample.csv").read_text(encoding="utf-8")
    df = cbk_fx.parse(text)
    assert list(df.columns) == cbk_fx.COLUMNS
    assert len(df) == 60
    # the real file contains exact duplicate rows — parse must NOT drop them
    assert df.duplicated().any()
    # values stay raw strings, including the messy currency labels
    assert "AE DIRHAM" in set(df["currency_raw"])


def test_cbk_fx_parse_keeps_malformed_rows_for_quarantine():
    # synthetic malformed input (short row, long row) — silver's job to reject
    text = "11/10/2016,US DOLLAR,101.3,101.2,101.4\nbadrow,onlytwo\na,b,c,d,e,f,g\n"
    df = cbk_fx.parse(text)
    assert len(df) == 3
    assert pd.isna(df.iloc[1]["mean_raw"])  # short row padded
    assert df.iloc[2]["sell_raw"] == "e,f,g"  # long row overflow preserved


def test_cbk_fx_parse_skips_blank_lines():
    assert len(cbk_fx.parse("a,b,c,d,e\n\n,,,,\n")) == 1


# --- CBK mobile payments HTML ---


def test_cbk_mobile_parse_scrapes_table(fixtures_dir):
    html = (fixtures_dir / "cbk_mobile_sample.html").read_text(encoding="utf-8")
    df = cbk_mobile.parse(html)
    assert list(df.columns) == cbk_mobile.COLUMNS
    assert len(df) == 18
    assert df.iloc[0]["year_raw"] == "2026"
    assert df.iloc[0]["month_raw"] == "May"


def test_cbk_mobile_parse_fails_loudly_when_table_missing():
    with pytest.raises(ValueError, match="table#table_1 not found"):
        cbk_mobile.parse("<html><body><p>maintenance</p></body></html>")


def test_cbk_mobile_parse_fails_loudly_when_headers_change(fixtures_dir):
    html = (fixtures_dir / "cbk_mobile_sample.html").read_text(encoding="utf-8")
    mutated = html.replace("Active Agents", "Agents (Active)")
    with pytest.raises(ValueError, match="headers changed"):
        cbk_mobile.parse(mutated)


# --- GSMA XLSX ---


def test_gsma_parse_melts_wide_quarters(fixtures_dir):
    df = gsma.parse((fixtures_dir / "gsma_sample.xlsx").read_bytes())
    assert list(df.columns) == gsma.COLUMNS
    assert len(df) > 0
    # id fields survived the melt
    assert "Sub-Saharan Africa" in set(df["geo_name_raw"])
    # periods are the original wide column labels, untouched
    assert df["period_raw"].str.match(r"\d{2}/\d{2}/\d{4}").all()
    # empty grid cells were excluded
    assert (df["value_raw"].str.strip() != "").all()


def test_gsma_parse_fails_loudly_on_missing_id_columns(fixtures_dir):
    import io

    import pandas as pd

    wrong = io.BytesIO()
    pd.DataFrame({"Nope": ["1"]}).to_excel(wrong, sheet_name=gsma.SHEET, index=False)
    with pytest.raises(ValueError, match="missing id columns"):
        gsma.parse(wrong.getvalue())
