select
    indicator_code,
    country_iso3,
    cast(year as integer)  as year,
    value,
    _source_row_hash
from {{ source('silver', 'worldbank_annual') }}
