# Design decisions

This page explains *why* the pipeline is shaped the way it is, so the repo reads as
considered engineering rather than a stack of tools.

## The medallion contract

| Layer | Contract | Rebuild semantics |
|---|---|---|
| **bronze** | rows exactly as received, every source column VARCHAR, plus `_source`, `_ingested_at`, `_source_row_hash` | incremental, idempotent (hash anti-join) |
| **silver** | typed, validated (Pandera), quarantined-with-reason, semantically deduplicated | deterministic full rebuild from bronze |
| **gold** | dimensional star schema, built and tested by dbt | deterministic full rebuild from silver |

Two properties fall out of this split:

1. **Replayability.** Any bug in cleaning logic is fixed by editing silver and
   re-running — bronze still holds the original bytes. Nothing upstream ever needs
   re-fetching to fix a parse bug.
2. **Auditability.** Every row in gold traces to a `_source_row_hash` in bronze; every
   rejected row sits in `silver.quarantine` with its raw payload and reasons.

## Quarantine philosophy: never guess

Rows that fail validation are stored with a human-readable reason list — the pipeline
never silently drops data. This includes a case most pipelines get wrong: when a source
*republishes* a fact with different values (CBK re-issued 210 daily rates with revised
numbers), both versions go to quarantine. Picking "latest" or "first" would be inventing
a fact. A human resolves it or it stays visibly unresolved.

Real rejections caught by these rules on the first live run:

* 22 rows from **2017-03-28**, a day CBK published with buy/sell columns swapped across
  every currency (`buy_rate <= sell_rate` check)
* a **future-dated** row (2038-01-22)
* a mangled header fragment (`Date,US DOLLAR,STG POUND`) inside the CSV body
* 87 World Bank null observations, quarantined instead of dropped so the gap is auditable

## Evidence-based parsing, not assumptions

The CBK CSV mixes `DD/MM/YYYY` and ISO dates in one column, with no documentation. The
slash order was *proven* rather than assumed: rows carrying identical rate values appear
in both formats (`11/10/2016` ≡ `2016-10-11`), which is only consistent with day-first.
The reasoning is documented in `src/mizani/silver/cbk_fx.py` where the parse lives.

Currency quote conventions differ within the same file: majors are quoted KES-per-unit,
`KES / X` cross rates are units-per-KES, and JPY is per 100 units. Silver preserves the
published quote; gold's staging normalizes everything to KES per 1 unit (inverting a
quote swaps bid/ask, which the SQL handles explicitly).

## Why DuckDB

Local-first, zero-ops, columnar, and speaks Parquet/CSV natively. It mirrors the
"SQLite in dev, Postgres in prod" pattern: the dbt project would move to Snowflake/
BigQuery by swapping the profile. The single-writer constraint is handled by pinning
Dagster's in-process executor (documented in `definitions.py`).

## Why Dagster (not Airflow)

* The pipeline is **asset-shaped** — bronze tables → silver tables → dbt models. Dagster
  models that graph directly; Airflow would express it as tasks with implicit data
  dependencies.
* `dagster dev` runs the whole thing locally with a UI; Airflow needs a scheduler,
  webserver, and metadata database before the first DAG parses.
* `dagster-dbt` exposes each dbt model as a first-class asset, so bronze→silver→gold
  lineage is one graph, not two systems.

## The backfill story (and why it isn't partitioned)

All four sources publish **full snapshots** — the CBK CSV is the entire history on every
fetch, the GSMA workbook and World Bank responses likewise. "Re-run the last 30 days
after a format change" is therefore: fix the parser, materialize the graph. Bronze's
hash-based landing re-inserts only rows that changed; silver and gold rebuild
deterministically. Day-partitioned assets would be *dishonest* here: the sources cannot
be fetched per-day, so 30 partitions would re-download the same snapshot 30 times.

## Degrade loudly

A source that fails to fetch logs `status='error'` to `meta.ingestion_log` and the run
continues — one flaky website must not block three healthy sources, but the failure has
to be visible, and the process exits non-zero so schedulers alert. This path fired for
real during development: the CBK site dropped a TLS handshake mid-run. Bronze assets
retry 3× with exponential backoff for the same reason.

## Testing stance

CI never touches the network. `tests/fixtures/` holds small **unmodified subsets of the
real payloads** (captured 2026-07-15), so parser tests exercise the actual mess — the
duplicate rows and header fragment in the CBK sample are the real ones. Synthetic rows
appear only inside tests, to exercise rules that must reject (bad month names, negative
rates, buy>sell). CI builds a complete warehouse from fixtures and runs the full dbt
build with all 28 data tests against it, plus the same inside the Docker image.

## Known limitations (what breaks at 100x)

* **Pandas-sized.** Transforms materialize whole sources in memory. Fine at ~100k rows;
  at 100x, silver transforms should push down into DuckDB SQL or Polars lazy frames.
* **Full-rebuild silver/gold.** Deterministic and simple, but O(history) every run. At
  scale: incremental dbt models keyed on `_ingested_at` watermarks.
* **Single-writer DuckDB.** Serializing asset execution caps parallelism. At scale:
  a warehouse with real concurrency (MotherDuck/Postgres/Snowflake) and per-asset
  connections.
* **Scrape fragility.** The CBK mobile-payments table has no API contract; the scraper
  fails loudly on any header change (by design), which means a cosmetic site redesign
  stops that source until the parser is updated.
* **No SCD handling.** Dimensions are tiny and static; a real deployment tracking
  changing reference data would need slowly-changing-dimension logic in dbt.
