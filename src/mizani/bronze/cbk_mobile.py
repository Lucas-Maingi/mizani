"""Central Bank of Kenya mobile payments extractor (HTML scrape).

CBK publishes monthly mobile-money statistics (Mar 2007 onward) only as
an embedded wpDataTable — no CSV/XLSX export. The full history is present
in the static HTML, so this scrapes table#table_1 without needing a
browser. Cell text is landed verbatim.
"""

import pandas as pd
import requests
from lxml import html as lxml_html

from mizani.config import CBK_MOBILE_PAYMENTS_PAGE, REQUEST_TIMEOUT, USER_AGENT

SOURCE = "cbk_mobile_html"
TABLE = "cbk_mobile_payments"

# positional names for the 6 data columns; the page's own headers are
# captured too so silver can detect if CBK reorders the table
COLUMNS = [
    "year_raw",
    "month_raw",
    "active_agents_raw",
    "registered_accounts_millions_raw",
    "agent_cico_volume_million_raw",
    "agent_cico_value_ksh_billions_raw",
]
EXPECTED_HEADERS = [
    "Year",
    "Month",
    "Active Agents",
    "Total Registered Mobile Money Accounts (Millions)",
    "Total Agent Cash in Cash Out (Volume Million)",
    "Total Agent Cash in Cash Out (Value KSh billions)",
]


def parse(page_html: str) -> pd.DataFrame:
    """Extract the statistics table. Raises loudly if the table vanishes
    or its headers change — a silently reshaped scrape is worse than a
    failed one."""
    tree = lxml_html.fromstring(page_html)
    tables = tree.xpath("//table[@id='table_1']")
    if not tables:
        raise ValueError("CBK mobile payments table#table_1 not found in page")
    table = tables[0]

    headers = [th.text_content().strip() for th in table.xpath(".//thead//th")]
    headers = [h for h in headers if h]  # wpDataTable pads with empty th cells
    if headers != EXPECTED_HEADERS:
        raise ValueError(f"CBK mobile payments table headers changed: {headers}")

    rows = []
    for tr in table.xpath(".//tbody/tr"):
        cells = [td.text_content().strip() for td in tr.xpath("./td")]
        if len(cells) != len(COLUMNS):
            raise ValueError(f"CBK mobile payments row has {len(cells)} cells: {cells}")
        rows.append(cells)
    if not rows:
        raise ValueError("CBK mobile payments table contained no data rows")
    return pd.DataFrame(rows, columns=COLUMNS)


def fetch() -> str:
    resp = requests.get(
        CBK_MOBILE_PAYMENTS_PAGE, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT
    )
    resp.raise_for_status()
    return resp.text


def extract() -> pd.DataFrame:
    return parse(fetch())
