-- Grain: one row per (measure, geography, attribute, unit, metric, quarter).
-- Geographies are GSMA regions/subregions, not countries — the public
-- workbook does not redistribute country-level data.
select
    cast(strftime(g.period_quarter, '%Y%m%d') as integer)  as date_key,
    g.measure,
    g.geo_view,
    g.geo_name,
    g.attribute,
    g.unit,
    g.metric,
    g.value,
    g._source_row_hash
from {{ ref('stg_gsma_metrics') }} g
