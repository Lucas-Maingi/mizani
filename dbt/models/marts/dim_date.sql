-- Calendar spine covering every date any fact references.
with bounds as (
    select
        least(
            (select min(rate_date) from {{ ref('stg_fx_rates') }}),
            (select min(period_month) from {{ ref('stg_mobile_payments') }}),
            (select min(period_quarter) from {{ ref('stg_gsma_metrics') }})
        ) as min_date,
        greatest(
            (select max(rate_date) from {{ ref('stg_fx_rates') }}),
            (select max(period_month) from {{ ref('stg_mobile_payments') }}),
            (select max(period_quarter) from {{ ref('stg_gsma_metrics') }})
        ) as max_date
),

spine as (
    select unnest(generate_series(
        (select min_date from bounds),
        (select max_date from bounds),
        interval 1 day
    ))::date as calendar_date
)

select
    cast(strftime(calendar_date, '%Y%m%d') as integer)  as date_key,
    calendar_date,
    extract(year from calendar_date)                    as year,
    extract(quarter from calendar_date)                 as quarter,
    extract(month from calendar_date)                   as month,
    strftime(calendar_date, '%B')                       as month_name,
    extract(day from calendar_date)                     as day_of_month,
    date_trunc('month', calendar_date)::date            as month_start,
    date_trunc('quarter', calendar_date)::date          as quarter_start
from spine
