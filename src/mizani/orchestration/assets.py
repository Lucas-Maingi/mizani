"""Dagster assets for the medallion pipeline.

Bronze assets fetch live sources and land them idempotently; each carries
a retry policy with exponential backoff because the sources demonstrably
flake (the CBK website dropped a TLS handshake during development).
Silver assets rebuild deterministically from bronze. Gold is built by
dbt via dagster-dbt (see definitions.py).
"""

from dagster import (
    AssetExecutionContext,
    Backoff,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)

from mizani.bronze import cbk_fx as bronze_cbk_fx
from mizani.bronze import cbk_mobile as bronze_cbk_mobile
from mizani.bronze import gsma as bronze_gsma
from mizani.bronze import worldbank as bronze_worldbank
from mizani.bronze.landing import land
from mizani.db import connect
from mizani.silver import cbk_fx as silver_cbk_fx
from mizani.silver import cbk_mobile as silver_cbk_mobile
from mizani.silver import gsma as silver_gsma
from mizani.silver import worldbank as silver_worldbank
from mizani.silver.run import build_source

FLAKY_SOURCE_RETRIES = RetryPolicy(max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL)


def _bronze_asset(extractor, description: str):
    @asset(
        name=extractor.TABLE,
        key_prefix="bronze",
        group_name="bronze",
        retry_policy=FLAKY_SOURCE_RETRIES,
        description=description,
    )
    def _asset(context: AssetExecutionContext) -> MaterializeResult:
        df = extractor.extract()
        with connect() as con:
            result = land(con, df, extractor.SOURCE, extractor.TABLE)
        context.log.info(
            "%s: received=%d inserted=%d skipped=%d",
            result.source,
            result.rows_received,
            result.rows_inserted,
            result.rows_skipped,
        )
        return MaterializeResult(
            metadata={
                "rows_received": MetadataValue.int(result.rows_received),
                "rows_inserted": MetadataValue.int(result.rows_inserted),
                "rows_skipped_duplicate": MetadataValue.int(result.rows_skipped),
            }
        )

    return _asset


worldbank_indicators = _bronze_asset(
    bronze_worldbank, "World Bank Open Data API: exchange rate + account ownership indicators."
)
cbk_fx_rates = _bronze_asset(
    bronze_cbk_fx, "CBK historical forex CSV (headerless, duplicated, mixed date formats)."
)
cbk_mobile_payments = _bronze_asset(
    bronze_cbk_mobile, "CBK mobile payments statistics scraped from the wpDataTable HTML."
)
gsma_mobile_money = _bronze_asset(
    bronze_gsma, "GSMA Global Mobile Money Dataset workbook, melted to long form."
)


def _silver_asset(module, bronze_upstream, description: str):
    @asset(
        name=module.SILVER_TABLE,
        key_prefix="silver",
        group_name="silver",
        deps=[bronze_upstream],
        description=description,
    )
    def _asset(context: AssetExecutionContext) -> MaterializeResult:
        with connect() as con:
            stats = build_source(con, module)
        context.log.info("silver.%s: %s", module.SILVER_TABLE, stats)
        return MaterializeResult(
            metadata={k: MetadataValue.int(v) for k, v in stats.items()}
        )

    return _asset


fx_rates_daily = _silver_asset(
    silver_cbk_fx, cbk_fx_rates, "Validated daily KES rates; failures quarantined with reasons."
)
mobile_payments_monthly = _silver_asset(
    silver_cbk_mobile, cbk_mobile_payments, "Validated monthly mobile-money statistics."
)
worldbank_annual = _silver_asset(
    silver_worldbank, worldbank_indicators, "Validated annual World Bank observations."
)
gsma_metrics_quarterly = _silver_asset(
    silver_gsma, gsma_mobile_money, "Validated quarterly GSMA metrics."
)

BRONZE_ASSETS = [worldbank_indicators, cbk_fx_rates, cbk_mobile_payments, gsma_mobile_money]
SILVER_ASSETS = [fx_rates_daily, mobile_payments_monthly, worldbank_annual,
                 gsma_metrics_quarterly]
