{{ config(severity='warn', store_failures_as='table', alias='dq_um_spot_price_mismatch') }}

-- Warn: days both the contract and its spot pair traded, with closes more than 10 % apart
-- (price_matches false): a reused spot symbol, a redenomination, a wrong multiplier.
-- Features leave these days out; the table says which they are.

select contract_symbol, trade_date, spot_symbol, multiplier, spot_perp_log_gap
from {{ ref('int_um__spot_daily') }}
where not price_matches and spot_perp_log_gap is not null
