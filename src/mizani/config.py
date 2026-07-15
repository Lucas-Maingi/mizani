"""Central configuration: paths, source URLs, warehouse location.

Everything overridable via environment variables so the same code runs
locally, in Docker, and in CI (where MIZANI_DB points at a throwaway file).
"""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = Path(os.environ.get("MIZANI_DATA_DIR", PROJECT_ROOT / "data"))
DB_PATH = Path(os.environ.get("MIZANI_DB", DATA_DIR / "mizani.duckdb"))

REQUEST_TIMEOUT = 60
USER_AGENT = "mizani-etl/0.1 (+https://github.com/Lucas-Maingi/mizani)"

# --- Source endpoints (all verified live 2026-07-15; see README data sources) ---

WORLDBANK_BASE = "https://api.worldbank.org/v2"
# Official exchange rate (LCU per US$, period average) and account ownership
WORLDBANK_INDICATORS = {
    "PA.NUS.FCRF": "official_exchange_rate_lcu_per_usd",
    "FX.OWN.TOTL.ZS": "account_ownership_pct_15plus",
}
WORLDBANK_COUNTRIES = ["KEN", "TZA", "UGA", "RWA"]

CBK_FX_HISTORICAL_CSV = "https://www.centralbank.go.ke/uploads/fx_rates/historical_data.csv"
CBK_MOBILE_PAYMENTS_PAGE = "https://www.centralbank.go.ke/national-payments-system/mobile-payments/"

GSMA_DATASET_XLSX = (
    "https://www.gsma.com/wp-content/uploads/2025/07/Global_Mobile_Money_Dataset_2024.xlsx"
)
