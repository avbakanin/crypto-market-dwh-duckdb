{#-
    The hourly profile around anchor hours: one row per (anchor, offset) from
    -pump_window_hours to +pump_window_hours. Offset 0 is the anchor hour; values are
    relative to the start, the end of hour -1 (row k of the market is the state at the end
    of hour k). Hours outside the loaded history are NULL, not missing.

    `anchors` is a CTE or relation with `symbol` and `anchor_open_time_ms`; its other
    columns (keys, flags) are passed through. The event window and the control window both
    call this macro, so pumps and ordinary hours are measured by the same SQL.
-#}
{% macro pump_window(anchors) %}
{%- set window = var('pump_window_hours') %}
select
    anchors.* exclude (anchor_open_time_ms),
    offsets.offset_hours,
    anchors.anchor_open_time_ms + offsets.offset_hours * 3600000 as open_time_ms,
    market.open_time,
    market.return_1h,
    market.close_price / start_hour.close_price - 1 as return_since_start,
    -- After the anchor hour closed: what an entry at its close would have seen.
    market.close_price / anchor_hour.close_price - 1 as return_since_anchor_close,
    -- Against the 7-day mean before the anchor hour (a mean: the median hour is ~0.7).
    market.quote_volume / anchor_hour.quote_volume_baseline as volume_ratio,
    market.taker_buy_share,
    market.open_interest_coins / nullif(start_hour.open_interest_coins, 0) - 1
        as open_interest_change_since_start,
    market.top_account_long_short_ratio,
    market.top_position_long_short_ratio,
    market.global_account_long_short_ratio,
    market.top_account_long_short_ratio
    / nullif(start_hour.top_account_long_short_ratio, 0) - 1
        as top_account_ratio_change_since_start,
    market.top_position_long_short_ratio
    / nullif(start_hour.top_position_long_short_ratio, 0) - 1
        as top_position_ratio_change_since_start,
    market.global_account_long_short_ratio
    / nullif(start_hour.global_account_long_short_ratio, 0) - 1
        as global_account_ratio_change_since_start,
    market.funding_rate_8h,
    market.funding_premium_8h,
    market.is_funding_stale
from {{ anchors }} as anchors
cross join (
    -- range() is dense: -window, ..., +window.
    select range as offset_hours
    from range(-{{ window }}, {{ window + 1 }})
) as offsets
left join {{ ref('int_binance__hourly_market') }} as start_hour
    on
        anchors.symbol = start_hour.symbol
        and anchors.anchor_open_time_ms - 3600000 = start_hour.open_time_ms
left join {{ ref('int_binance__hourly_market') }} as anchor_hour
    on
        anchors.symbol = anchor_hour.symbol
        and anchors.anchor_open_time_ms = anchor_hour.open_time_ms
left join {{ ref('int_binance__hourly_market') }} as market
    on
        anchors.symbol = market.symbol
        and anchors.anchor_open_time_ms + offsets.offset_hours * 3600000
        = market.open_time_ms
{% endmacro %}
