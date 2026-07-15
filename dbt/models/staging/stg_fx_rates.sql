-- Normalize every quote to KES per 1 unit of foreign currency so the fact
-- table has one consistent grain. Two published conventions are unified:
--   KES_PER_UNIT  : rate is already KES per `units_per_quote` units
--   UNITS_PER_KES : rate is foreign units per 1 KES (CBK's "KES / X" rows)
select
    cast(rate_date as date)                        as rate_date,
    currency_code,
    quote_basis                                    as published_quote_basis,
    mean_rate                                      as published_mean_rate,
    case quote_basis
        when 'KES_PER_UNIT'  then mean_rate / units_per_quote
        when 'UNITS_PER_KES' then 1.0 / mean_rate
    end                                            as kes_per_unit_mean,
    case quote_basis
        when 'KES_PER_UNIT'  then buy_rate / units_per_quote
        when 'UNITS_PER_KES' then 1.0 / sell_rate  -- inverting swaps bid/ask
    end                                            as kes_per_unit_buy,
    case quote_basis
        when 'KES_PER_UNIT'  then sell_rate / units_per_quote
        when 'UNITS_PER_KES' then 1.0 / buy_rate
    end                                            as kes_per_unit_sell,
    _source_row_hash
from {{ source('silver', 'fx_rates_daily') }}
