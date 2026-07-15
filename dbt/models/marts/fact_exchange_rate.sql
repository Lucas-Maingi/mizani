-- Grain: one row per (rate_date, currency_code). All rates in KES per
-- 1 unit of the foreign currency (normalization happens in staging).
select
    cast(strftime(fx.rate_date, '%Y%m%d') as integer)  as date_key,
    fx.currency_code,
    fx.kes_per_unit_mean,
    fx.kes_per_unit_buy,
    fx.kes_per_unit_sell,
    fx.published_quote_basis,
    fx.published_mean_rate,
    fx._source_row_hash
from {{ ref('stg_fx_rates') }} fx
