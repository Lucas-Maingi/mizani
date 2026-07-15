# Mizani — East African economic & mobile-money data platform

[![CI](https://github.com/Lucas-Maingi/mizani/actions/workflows/ci.yml/badge.svg)](https://github.com/Lucas-Maingi/mizani/actions/workflows/ci.yml)

**Medallion ETL · Dagster · dbt · DuckDB · data quality** — a bronze/silver/gold
pipeline that ingests four genuinely messy public data sources (one JSON API, one
headerless CSV, one HTML scrape, one human-formatted Excel workbook), validates and
quarantines them declaratively, and models them into a star schema.

*Mizani* is Swahili for "scales / balance."


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

### Orchestration (implemented)

Dagster models the pipeline as a 21-asset graph (bronze → silver → dbt staging/gold),
scheduled daily at 07:00 Nairobi time. The full live run — fetch all four sources,
rebuild silver, dbt build with all tests — takes **54 seconds** (measured 2026-07-15).

* **Why Dagster over Airflow:** the pipeline is asset-shaped, Dagster runs locally
  without a Postgres + scheduler stack, and `dagster-dbt` exposes every dbt model as a
  first-class asset with lineage.
* **Retries:** bronze assets retry 3× with exponential backoff — the CBK website
  actually dropped a TLS connection mid-run during development, so this is not
  hypothetical.
* **Backfill story:** every source publishes a full snapshot, so "re-run the last 30
  days after a format change" is: fix the parser, materialize the graph. Hash-based
  bronze landing re-inserts only changed rows; silver/gold rebuild deterministically.
  Per-day partitions would be dishonest — the sources cannot be fetched per-day.
* **Serialized execution:** DuckDB is single-writer, so the job pins the in-process
  executor instead of letting multiprocess steps race on the file.

```bash
dagster dev -m mizani.orchestration.definitions   # UI at localhost:3000
dagster job execute -m mizani.orchestration.definitions -j full_refresh
```

## The analytical payoff

[`notebooks/mobile_money_analysis.ipynb`](notebooks/mobile_money_analysis.ipynb) answers
one question from gold alone: **how big is Kenya's mobile-money cash economy in hard
currency, and is the region catching up?** The USD conversion joins CBK's monthly
cash-in/cash-out values against that month's average official rate — a join that only
works because silver normalized three date formats and two quote conventions. It exists
to prove the star schema is usable, not to be a research paper.

## Running it

```bash
docker compose up pipeline        # full live run: bronze -> silver -> dbt build
docker compose --profile ui up dagster   # Dagster UI + daily schedule at localhost:3000
```

or natively:

```bash
pip install -e ".[dev]"
python -m mizani.bronze.run                        # fetch live sources
python -m mizani.silver.run                        # validate + quarantine
cd dbt && dbt deps --profiles-dir . && dbt build --profiles-dir .   # gold + tests
pytest                                             # offline test suite (27 tests)
```

Tests never touch the network: `tests/fixtures/` contains small unmodified subsets of
the real payloads captured on 2026-07-15. CI (and the Docker job) builds a complete
warehouse from those fixtures and runs the full dbt build against it on every push.

## Design decisions & limitations

[`docs/design-decisions.md`](docs/design-decisions.md) covers the bronze/silver/gold
contract, the never-guess quarantine philosophy, why DuckDB + Dagster, the
full-snapshot backfill story, and an honest list of what breaks at 100× scale
(pandas-sized transforms, full-rebuild semantics, single-writer DuckDB, scrape
fragility).

## Roadmap

- [x] **M0** — verify sources are actually live; pick for messiness + reliability
- [x] **M1** — bronze extraction: 4 extractors, idempotent landing, ingestion log, 16 offline tests
- [x] **M2** — silver: declarative Pandera validation, quarantine-with-reason, semantic dedup
- [x] **M3** — gold: dbt-duckdb star schema (fact exchange rates / mobile money; dims country, currency, date) + dbt tests
- [x] **M4** — orchestration: Dagster asset graph, retries, honest backfill story
- [x] **M5** — CI (lint + tests + dbt build on fixtures + Docker), analytical notebook, limitations doc
