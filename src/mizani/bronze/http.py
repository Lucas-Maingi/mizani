"""Shared HTTP session with retries for all bronze extractors.

Both the World Bank API and the CBK website have produced transient
failures in real runs (a 60s read timeout from a GitHub runner; a dropped
TLS handshake locally). Dagster retries at the asset level, but the plain
`python -m mizani.bronze.run` path used by CI and the live publish
workflow needs resilience too — so retries live at the HTTP layer, where
every entry point gets them.
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from mizani.config import REQUEST_TIMEOUT, USER_AGENT

RETRY = Retry(
    total=4,
    connect=4,
    read=4,
    status=4,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    backoff_factor=2,  # 0s, 2s, 4s, 8s between attempts
)


def session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    adapter = HTTPAdapter(max_retries=RETRY)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def get(url: str, **kwargs) -> requests.Response:
    kwargs.setdefault("timeout", REQUEST_TIMEOUT)
    with session() as s:
        resp = s.get(url, **kwargs)
    resp.raise_for_status()
    return resp
