"""Run all bronze extractions: fetch each source, land it, degrade loudly.

A source that fails is logged to meta.ingestion_log with status='error'
and the run continues — one flaky website must not block the others.
The process exits non-zero if anything failed so schedulers notice.
"""

import logging
import sys

from mizani.bronze import cbk_fx, cbk_mobile, gsma, worldbank
from mizani.bronze.landing import land, log_failure
from mizani.db import connect

log = logging.getLogger("mizani.bronze")

EXTRACTORS = [worldbank, cbk_fx, cbk_mobile, gsma]


def run_all(db_path=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    con = connect(db_path)
    failures = 0
    for module in EXTRACTORS:
        try:
            df = module.extract()
            result = land(con, df, module.SOURCE, module.TABLE)
            log.info(
                "%s -> %s: received=%d inserted=%d skipped=%d",
                result.source,
                result.target_table,
                result.rows_received,
                result.rows_inserted,
                result.rows_skipped,
            )
        except Exception as exc:
            failures += 1
            log_failure(con, module.SOURCE, module.TABLE, repr(exc))
            log.error("%s FAILED: %r — continuing with remaining sources", module.SOURCE, exc)
    con.close()
    return failures


if __name__ == "__main__":
    sys.exit(1 if run_all() else 0)
