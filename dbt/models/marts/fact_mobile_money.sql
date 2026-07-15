-- Grain: one row per month. Kenya-only (CBK national statistics).
select
    cast(strftime(mp.period_month, '%Y%m%d') as integer)  as date_key,
    'KEN'                                                 as country_iso3,
    mp.active_agents,
    mp.registered_accounts_millions,
    mp.agent_cico_volume_million,
    mp.agent_cico_value_ksh_billions,
    mp._source_row_hash
from {{ ref('stg_mobile_payments') }} mp
