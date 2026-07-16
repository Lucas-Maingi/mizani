-- Cross-source reconciliation: CBK daily USD rates vs the World Bank's
-- independently published annual average (PA.NUS.FCRF, Kenya).
-- Years need >= 200 trading days of CBK coverage to be comparable; a
-- dbt test asserts agreement within 1% (measured max divergence on full
-- years 2017-2023 was 0.09%; partial 2016 was 0.35%).
with cbk as (
    select
        d.year,
        avg(f.kes_per_unit_mean) as cbk_avg_rate,
        count(*)                 as cbk_trading_days
    from {{ ref('fact_exchange_rate') }} f
    join {{ ref('dim_date') }} d using (date_key)
    where f.currency_code = 'USD'
    group by 1
),

wb as (
    select year, value as worldbank_avg_rate
    from {{ ref('fact_worldbank_indicator') }}
    where indicator_code = 'PA.NUS.FCRF' and country_iso3 = 'KEN'
)

select
    cbk.year,
    cbk.cbk_avg_rate,
    wb.worldbank_avg_rate,
    cbk.cbk_trading_days,
    abs(cbk.cbk_avg_rate - wb.worldbank_avg_rate)
        / wb.worldbank_avg_rate as relative_difference
from cbk
join wb using (year)
where cbk.cbk_trading_days >= 200
