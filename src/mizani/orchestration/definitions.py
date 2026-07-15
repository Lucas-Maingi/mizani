"""Dagster Definitions: asset graph, dbt integration, schedule, backfill.

Why Dagster over Airflow: the pipeline is asset-shaped (bronze tables ->
silver tables -> dbt models), and Dagster's asset graph models that
directly, runs locally with `dagster dev` (no Postgres/scheduler stack),
and dagster-dbt exposes each dbt model as a first-class asset with lineage.

Backfill story: every source publishes a FULL snapshot (the CBK CSV is
the complete history on every fetch; the GSMA workbook and World Bank
API likewise). So "re-run the last 30 days after a format change" is:
fix the parser, then materialize the whole graph — bronze's hash-based
landing re-inserts only rows that changed, and silver/gold rebuild
deterministically from bronze. The `full_refresh` job is that backfill.
Per-day partitions would be dishonest here: the sources cannot be
fetched per-day, so partitioned assets would just re-download the same
snapshot 30 times.
"""

from pathlib import Path

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    in_process_executor,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from mizani.orchestration.assets import BRONZE_ASSETS, SILVER_ASSETS

DBT_PROJECT_DIR = Path(__file__).resolve().parents[3] / "dbt"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def gold_dbt_assets(context, dbt: DbtCliResource):
    """Gold star schema: dbt seeds, models, and data tests."""
    yield from dbt.cli(["build"], context=context).stream()


full_refresh = define_asset_job(
    name="full_refresh",
    selection=AssetSelection.all(),
    description=(
        "Fetch every source snapshot, rebuild silver and gold. Doubles as the "
        "backfill job: bronze landing is idempotent, downstream is deterministic."
    ),
)

daily_schedule = ScheduleDefinition(
    job=full_refresh,
    cron_schedule="0 7 * * *",
    execution_timezone="Africa/Nairobi",
    name="daily_refresh",
)

defs = Definitions(
    assets=[*BRONZE_ASSETS, *SILVER_ASSETS, gold_dbt_assets],
    jobs=[full_refresh],
    schedules=[daily_schedule],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
    # DuckDB allows a single writer process: serialize asset execution
    # instead of letting the multiprocess executor race on the file
    executor=in_process_executor,
)
