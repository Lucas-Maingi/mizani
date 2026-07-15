select
    country_iso3,
    country_name,
    subregion
from {{ ref('country_ref') }}
