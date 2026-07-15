"""The Dagster definitions must load and wire the full lineage."""

from pathlib import Path

import pytest

MANIFEST = Path(__file__).parents[1] / "dbt" / "target" / "manifest.json"


@pytest.mark.skipif(
    not MANIFEST.exists(), reason="dbt manifest not built (run `dbt parse` in dbt/)"
)
def test_definitions_load_and_wire_medallion_lineage():
    from dagster import AssetKey

    from mizani.orchestration.definitions import defs

    graph = defs.resolve_asset_graph()
    keys = {str(k) for k in graph.get_all_asset_keys()}

    for expected in [
        "AssetKey(['bronze', 'cbk_fx_rates'])",
        "AssetKey(['silver', 'fx_rates_daily'])",
        "AssetKey(['staging', 'stg_fx_rates'])",
        "AssetKey(['gold', 'fact_exchange_rate'])",
    ]:
        assert expected in keys

    # bronze -> silver -> staging edges exist
    assert AssetKey(["bronze", "cbk_fx_rates"]) in graph.get(
        AssetKey(["silver", "fx_rates_daily"])
    ).parent_keys
    assert AssetKey(["silver", "fx_rates_daily"]) in graph.get(
        AssetKey(["staging", "stg_fx_rates"])
    ).parent_keys

    # bronze assets retry because sources demonstrably flake
    node = graph.get(AssetKey(["bronze", "cbk_mobile_payments"]))
    retry_policy = defs.get_assets_def(node.key).op.retry_policy
    assert retry_policy is not None and retry_policy.max_retries >= 3
