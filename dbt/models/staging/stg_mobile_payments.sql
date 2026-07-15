select
    cast(period_month as date)         as period_month,
    active_agents,
    registered_accounts_millions,
    agent_cico_volume_million,
    agent_cico_value_ksh_billions,
    _source_row_hash
from {{ source('silver', 'mobile_payments_monthly') }}
