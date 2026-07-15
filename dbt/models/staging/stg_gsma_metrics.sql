select
    measure,
    geo_view,
    geo_name,
    attribute,
    unit,
    metric,
    cast(period_quarter as date)  as period_quarter,
    value,
    _source_row_hash
from {{ source('silver', 'gsma_metrics_quarterly') }}
