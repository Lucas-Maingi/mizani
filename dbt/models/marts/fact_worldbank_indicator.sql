-- Grain: one row per (indicator, country, year).
select
    wb.indicator_code,
    wb.country_iso3,
    wb.year,
    wb.value,
    wb._source_row_hash
from {{ ref('stg_worldbank') }} wb
