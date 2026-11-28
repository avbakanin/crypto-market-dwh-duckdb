-- The spot market of each USD-M contract's coin per UTC day, on the contract's own days.
-- Candidate pairs: the contract's symbol, and for a contract quoted per N coins the symbol
-- without its prefix (1000PEPEUSDT -> PEPEUSDT x 1000; 1000SATSUSDT trades on spot under
-- its own name). A candidate is the contract's pair when the median absolute log gap of
-- their daily closes, over days both traded, is below 2 %; of two, the closer one.
-- spot_close is per contract unit (price x N); spot_perp_log_gap = ln(contract close /
-- spot_close). A day off by more than 10 % (a reused symbol, a redenomination) or with
-- either side idle has price_matches false: features read days where it is true.
-- Archive only: day D-1 is published in the morning of D (source note 133), so a live
-- feature of D can read D-2 at the latest.

with contracts as (

    select
        symbol as contract_symbol,
        open_time::date as trade_date,
        close_price,
        trade_count
    from {{ ref('stg_binance__um_archive_klines_1d') }}

),

spot as (

    select
        symbol as spot_symbol,
        open_time::date as trade_date,
        close_price,
        quote_volume,
        taker_buy_quote_volume,
        trade_count
    from {{ ref('stg_binance__spot_archive_klines_1d') }}

),

candidates as (

    select distinct contract_symbol, contract_symbol as spot_symbol, 1 as multiplier
    from contracts
    union
    select distinct
        c.contract_symbol,
        substr(c.contract_symbol, length(p.prefix) + 1) as spot_symbol,
        p.multiplier
    from contracts as c
    cross join (values ('1000000', 1000000), ('1000', 1000), ('1M', 1000000))
        as p (prefix, multiplier)
    where starts_with(c.contract_symbol, p.prefix)
        and length(c.contract_symbol) > length(p.prefix) + length('USDT')

),

gaps as (

    select
        c.contract_symbol,
        c.spot_symbol,
        c.multiplier,
        k.trade_date,
        ln(k.close_price::double / (s.close_price::double * c.multiplier)) as log_gap
    from candidates as c
    inner join spot as s using (spot_symbol)
    inner join contracts as k
        on k.contract_symbol = c.contract_symbol and k.trade_date = s.trade_date
    where k.trade_count > 0 and s.trade_count > 0 and s.close_price > 0 and k.close_price > 0

),

pairs as (

    select contract_symbol, spot_symbol, multiplier, median(abs(log_gap)) as median_gap
    from gaps
    group by contract_symbol, spot_symbol, multiplier
    having median(abs(log_gap)) < 0.02
    qualify row_number() over (
        partition by contract_symbol order by median_gap, spot_symbol
    ) = 1

)

select
    p.contract_symbol,
    k.trade_date,
    p.spot_symbol,
    p.multiplier,
    s.close_price * p.multiplier as spot_close,
    s.quote_volume as spot_quote_volume,
    s.taker_buy_quote_volume as spot_taker_buy_quote_volume,
    s.trade_count as spot_trade_count,
    g.log_gap as spot_perp_log_gap,
    coalesce(abs(g.log_gap) < 0.1, false) as price_matches
from pairs as p
inner join contracts as k on k.contract_symbol = p.contract_symbol
inner join spot as s on s.spot_symbol = p.spot_symbol and s.trade_date = k.trade_date
left join gaps as g
    on g.contract_symbol = p.contract_symbol
    and g.spot_symbol = p.spot_symbol
    and g.trade_date = k.trade_date
