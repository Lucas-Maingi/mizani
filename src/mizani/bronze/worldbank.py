"""World Bank Open Data API extractor (JSON).

Endpoint shape: /v2/country/{codes}/indicator/{id}?format=json returns
[page_metadata, [records...]]. Values arrive typed but are landed as
strings like every other source — bronze does not trust or coerce.
"""

import pandas as pd

from mizani.bronze import http
from mizani.config import (
    WORLDBANK_BASE,
    WORLDBANK_COUNTRIES,
    WORLDBANK_INDICATORS,
)

SOURCE = "worldbank_api"
TABLE = "worldbank_indicators"

COLUMNS = [
    "indicator_id",
    "indicator_name",
    "country_iso3",
    "country_name",
    "year",
    "value",
    "unit",
    "obs_status",
]


def parse(pages: list[list]) -> pd.DataFrame:
    """Flatten one indicator's paged JSON responses into raw rows."""
    records = []
    for page in pages:
        if not isinstance(page, list) or len(page) < 2 or page[1] is None:
            continue
        for rec in page[1]:
            records.append(
                {
                    "indicator_id": rec.get("indicator", {}).get("id"),
                    "indicator_name": rec.get("indicator", {}).get("value"),
                    "country_iso3": rec.get("countryiso3code"),
                    "country_name": rec.get("country", {}).get("value"),
                    "year": rec.get("date"),
                    "value": rec.get("value"),
                    "unit": rec.get("unit"),
                    "obs_status": rec.get("obs_status"),
                }
            )
    return pd.DataFrame(records, columns=COLUMNS)


def fetch(indicator: str, date_range: str = "2000:2026") -> list[list]:
    """Fetch all pages for one indicator across the configured countries."""
    countries = ";".join(WORLDBANK_COUNTRIES)
    url = f"{WORLDBANK_BASE}/country/{countries}/indicator/{indicator}"
    pages, page_no, total_pages = [], 1, 1
    while page_no <= total_pages:
        resp = http.get(
            url,
            params={"format": "json", "date": date_range, "per_page": 500, "page": page_no},
        )
        body = resp.json()
        if not isinstance(body, list) or "message" in (body[0] or {}):
            raise ValueError(f"World Bank API error for {indicator}: {body}")
        total_pages = int(body[0].get("pages", 1))
        pages.append(body)
        page_no += 1
    return pages


def extract() -> pd.DataFrame:
    frames = [parse(fetch(indicator)) for indicator in WORLDBANK_INDICATORS]
    return pd.concat(frames, ignore_index=True)
