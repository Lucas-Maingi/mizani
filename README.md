# Mizani — East African economic & mobile-money data platform

**Medallion ETL · dbt · DuckDB · data quality · orchestration** — a bronze/silver/gold
pipeline that ingests four genuinely messy public data sources (one JSON API, one
headerless CSV, one HTML scrape, one human-formatted Excel workbook), validates and
quarantines them declaratively, and models them into a star schema.

*Mizani* is Swahili for "scales / balance."

> **Status: work in progress.** Bronze, silver, and the dbt gold star schema are
> complete and tested. Orchestration and CI are in flight — see the roadmap below.

## Data sources

All sources were verified live on 2026-07-15. Every number below is measured from an
actual pipeline run on that date, not estimated.

| Source | Format | The mess | Rows landed |
|---|---|---|---|
| [World Bank Open Data API](https://api.worldbank.org/v2/country/KEN;TZA;UGA;RWA/indicator/PA.NUS.FCRF?format=json) | JSON API | nulls for missing years; pagination | 208 |
| [CBK historical forex rates](https://www.centralbank.go.ke/rates/forex-exchange-rates/) | CSV | **no header row**, 485 exact duplicate rows (1.3%), ambiguous MM/DD/YYYY dates, labels like `CAN $` / `AE DIRHAM` | 37,392 (of 37,877 received) |
| [CBK mobile payments statistics](https://www.centralbank.go.ke/national-payments-system/mobile-payments/) | HTML table (no export offered) | scrape-only wpDataTable; month names as text; comma-formatted numbers | 231 (monthly, Mar 2007–May 2026) |
| [GSMA Global Mobile Money Dataset](https://www.gsma.com/solutions-and-impact/connectivity-for-good/mobile-for-development/gsma_resources/global-mobile-money-dataset/) | XLSX | title rows, padding columns, trailing spaces in sheet names, metrics spread across ~100 wide quarter columns | 56,616 (after lossless melt) |

**Dropped:** the Kenya Open Data portal (`kenya.opendataforafrica.org`) returned
`403 Forbidden` at verification time and is historically unreliable, so it was not used.

## Architecture

```
  World Bank API ─┐
  CBK forex CSV  ─┤   BRONZE          SILVER              GOLD
  CBK mobile HTML─┼─▶ as-received ─▶  validated (Pandera) ─▶ star schema (dbt)
  GSMA XLSX      ─┘   + row hash      + quarantine           + dbt tests/docs
                      + ingest log      with reasons
                            DuckDB warehouse (local-first)
```

### The bronze contract (implemented)

* Rows land **exactly as received** — every source column is VARCHAR, untouched.
* Three metadata columns: `_source`, `_ingested_at`, `_source_row_hash` (sha256).
* **Idempotent:** re-running any extract inserts 0 duplicate rows (hash anti-join);
  verified by re-running the full pipeline.
* **Degrades loudly:** a failing source is recorded in `meta.ingestion_log` with
  `status='error'` and the run continues. (This path has already fired for real —
  the CBK website dropped a TLS connection during a verification re-run.)
* Never cleaned in bronze: the CBK duplicate rows, `CAN $` labels, and MM/DD/YYYY
  dates all flow through for silver to handle.

### The silver contract (implemented)

Silver is rebuilt deterministically from bronze on each run. Validation is declarative
(Pandera schemas, one per source); rows that fail any check land in `silver.quarantine`
with their original raw payload and a human-readable reason list — nothing is silently
dropped. Rows sharing a business key with **different** values are both quarantined as
conflicts; the pipeline never guesses which revision is true.

Measured on the 2026-07-15 live run:

| Silver table | Rows in | Clean | Quarantined | Why quarantined |
|---|---|---|---|---|
| `fx_rates_daily` | 37,392 | 37,154 | 235 | 210 conflicting republished rates; **22 rows from 2017-03-28, a day CBK published with buy > sell across all currencies**; 1 future date (2038); 1 mangled header fragment; 1 empty currency label |
| `worldbank_annual` | 208 | 121 | 87 | missing observations (API returns explicit nulls) |
| `mobile_payments_monthly` | 231 | 231 | 0 | — |
| `gsma_metrics_quarterly` | 56,616 | 56,616 | 0 | — |

The CBK file's slash-dates were proven to be DD/MM (not MM/DD) by matching identical
rate values across the file's two date formats — documented in
[`silver/cbk_fx.py`](src/mizani/silver/cbk_fx.py).

### The gold star schema (implemented)

Built with dbt-duckdb: `dim_date` (9,193-day spine), `dim_country`, `dim_currency`,
and four facts — `fact_exchange_rate` (37,154 rows, every quote normalized to KES per
1 foreign unit across both published conventions), `fact_mobile_money` (231 monthly
rows), `fact_worldbank_indicator` (121), `fact_gsma_metric` (56,616). 28 dbt data
tests (unique, not_null, relationships, ranges) — 41/41 green including seeds/models.

## Running it

```bash
pip install -e ".[dev]"
python -m mizani.bronze.run   # fetches all four live sources into data/mizani.duckdb
pytest                        # offline test suite against committed real-data fixtures
```

Tests never touch the network: `tests/fixtures/` contains small unmodified subsets of
the real payloads captured on 2026-07-15.

## Roadmap

- [x] **M0** — verify sources are actually live; pick for messiness + reliability
- [x] **M1** — bronze extraction: 4 extractors, idempotent landing, ingestion log, 16 offline tests
- [x] **M2** — silver: declarative Pandera validation, quarantine-with-reason, semantic dedup
- [x] **M3** — gold: dbt-duckdb star schema (fact exchange rates / mobile money; dims country, currency, date) + dbt tests
- [ ] **M4** — orchestration: Dagster asset graph, retries, 30-day backfill story
- [ ] **M5** — CI (lint + tests + dbt build on fixtures), analytical notebook, Docker one-command run, limitations doc
