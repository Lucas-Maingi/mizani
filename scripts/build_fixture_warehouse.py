"""Build a complete warehouse from the committed test fixtures — no network.

Used by CI (and local dev without connectivity) to exercise the whole
medallion path: bronze landing -> silver validation -> ready for dbt build.

    python scripts/build_fixture_warehouse.py [db_path]
    cd dbt && MIZANI_DB=<db_path> dbt build --profiles-dir .
"""

import json
import sys
from pathlib import Path

from mizani.bronze import cbk_fx, cbk_mobile, gsma, worldbank
from mizani.bronze.landing import land
from mizani.db import connect
from mizani.silver import run as silver_run

FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures"


def main(db_path: str) -> None:
    con = connect(db_path)

    frames = {
        worldbank: worldbank.parse(
            [json.loads((FIXTURES / "worldbank_fcrf_sample.json").read_text(encoding="utf-8"))]
        ),
        cbk_fx: cbk_fx.parse((FIXTURES / "cbk_fx_sample.csv").read_text(encoding="utf-8")),
        cbk_mobile: cbk_mobile.parse(
            (FIXTURES / "cbk_mobile_sample.html").read_text(encoding="utf-8")
        ),
        gsma: gsma.parse((FIXTURES / "gsma_sample.xlsx").read_bytes()),
    }
    for module, df in frames.items():
        result = land(con, df, module.SOURCE, module.TABLE)
        print(f"bronze.{module.TABLE}: {result.rows_inserted} rows")
    con.close()

    failures = silver_run.run_all(db_path)
    if failures:
        raise SystemExit(f"{failures} silver build(s) failed")
    print(f"fixture warehouse ready at {db_path}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "data/fixture.duckdb")
