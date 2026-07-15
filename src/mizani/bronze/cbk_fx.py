"""Central Bank of Kenya historical forex CSV extractor.

The published file has no header row, contains exact duplicate rows,
ambiguous MM/DD/YYYY dates, and free-form currency labels ("CAN $",
"AE DIRHAM"). Bronze assigns positional *_raw column names and nothing
else — every one of those problems is silver's job.
"""

import csv
import io

import pandas as pd
import requests

from mizani.config import CBK_FX_HISTORICAL_CSV, REQUEST_TIMEOUT, USER_AGENT

SOURCE = "cbk_fx_csv"
TABLE = "cbk_fx_rates"

COLUMNS = ["date_raw", "currency_raw", "mean_raw", "buy_raw", "sell_raw"]


def parse(csv_text: str) -> pd.DataFrame:
    """Parse the headerless CSV as-received. Short/long rows are kept
    (padded/overflow into the last column) so silver can quarantine them
    with a reason instead of the parser silently discarding them."""
    rows = []
    for record in csv.reader(io.StringIO(csv_text)):
        if not record or all(f.strip() == "" for f in record):
            continue
        if len(record) > len(COLUMNS):
            record = record[: len(COLUMNS) - 1] + [",".join(record[len(COLUMNS) - 1 :])]
        record = record + [None] * (len(COLUMNS) - len(record))
        rows.append(record)
    return pd.DataFrame(rows, columns=COLUMNS)


def fetch() -> str:
    resp = requests.get(
        CBK_FX_HISTORICAL_CSV, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT
    )
    resp.raise_for_status()
    return resp.text


def extract() -> pd.DataFrame:
    return parse(fetch())
