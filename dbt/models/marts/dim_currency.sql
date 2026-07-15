select
    currency_code,
    currency_name,
    country_iso3
from {{ ref('currency_ref') }}
