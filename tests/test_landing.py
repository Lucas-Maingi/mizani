import pandas as pd

from mizani.bronze.landing import land, log_failure, row_hash


def _df(rows):
    return pd.DataFrame(rows, columns=["a", "b"])


def test_lands_rows_with_metadata(con):
    result = land(con, _df([["1", "x"], ["2", "y"]]), "test_src", "t")
    assert result.rows_received == 2
    assert result.rows_inserted == 2

    rows = con.execute(
        "SELECT a, b, _source, _source_row_hash FROM bronze.t ORDER BY a"
    ).fetchall()
    assert rows[0][:3] == ("1", "x", "test_src")
    assert len(rows[0][3]) == 64  # sha256 hex


def test_rerun_is_idempotent(con):
    df = _df([["1", "x"], ["2", "y"]])
    land(con, df, "test_src", "t")
    second = land(con, df, "test_src", "t")
    assert second.rows_inserted == 0
    assert second.rows_skipped == 2
    assert con.execute("SELECT count(*) FROM bronze.t").fetchone()[0] == 2


def test_exact_duplicate_source_rows_collapse_but_are_counted(con):
    result = land(con, _df([["1", "x"], ["1", "x"], ["2", "y"]]), "test_src", "t")
    assert result.rows_received == 3
    assert result.rows_inserted == 2
    assert result.rows_skipped == 1


def test_incremental_rows_append(con):
    land(con, _df([["1", "x"]]), "test_src", "t")
    result = land(con, _df([["1", "x"], ["2", "y"]]), "test_src", "t")
    assert result.rows_inserted == 1
    assert con.execute("SELECT count(*) FROM bronze.t").fetchone()[0] == 2


def test_row_hash_is_boundary_safe():
    assert row_hash(["ab", "c"]) != row_hash(["a", "bc"])
    # documented behavior: None normalizes to empty string
    assert row_hash(["a", None]) == row_hash(["a", ""])


def test_ingestion_log_records_runs_and_failures(con):
    land(con, _df([["1", "x"]]), "src_ok", "t")
    log_failure(con, "src_bad", "t", "ConnectionError('boom')")
    log = con.execute(
        "SELECT source, status FROM meta.ingestion_log ORDER BY source"
    ).fetchall()
    assert ("src_ok", "ok") in log
    assert ("src_bad", "error") in log
