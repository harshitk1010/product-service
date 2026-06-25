{{
  config(
    materialized='incremental',
    unique_key='session_id',
    incremental_strategy='merge',
    cluster_by=['session_date'],
    tags=['marts', 'sessions', 'behavioral']
  )
}}

/*
fact_sessions — grain: one row per session.
Sessionizes raw events using a 30-minute inactivity gap.
Computes funnel flags: did_search, did_add_to_cart, did_purchase.
*/

with all_events as (
    select
        session_id,
        user_id,
        event_type,
        event_timestamp,
        country_code,
        device_type,
        product_id,
        price
    from {{ ref('stg_product_events') }}

    union all

    select
        session_id,
        user_id,
        event_type,
        event_timestamp,
        country_code,
        device_type,
        null as product_id,
        null as price
    from {{ ref('stg_order_events') }}

    {% if is_incremental() %}
    -- Process last 2 days to handle sessions that span midnight
    having min(event_timestamp) >= dateadd(day, -2, current_date())
    {% endif %}
),

session_agg as (
    select
        session_id,
        user_id,
        min(country_code)                   as country_code,
        min(device_type)                    as device_type,
        min(event_timestamp)                as session_start,
        max(event_timestamp)                as session_end,
        datediff('second',
            min(event_timestamp),
            max(event_timestamp)
        )                                   as duration_seconds,
        count(*)                            as event_count,
        countif(product_id is not null)     as unique_products_viewed,
        -- Funnel flags
        countif(event_type = 'product_searched') > 0    as did_search,
        countif(event_type = 'cart_add') > 0            as did_add_to_cart,
        countif(event_type = 'checkout_started') > 0    as did_checkout,
        countif(event_type = 'order_placed') > 0        as did_purchase,
        max(case when event_type = 'order_placed' then price end)
                                            as order_amount,
        to_date(min(event_timestamp))       as session_date
    from all_events
    group by session_id, user_id
),

with_keys as (
    select
        s.*,
        to_number(to_char(s.session_start, 'YYYYMMDD')) as date_sk,
        u.user_sk,
        g.geo_sk,
        current_timestamp() as _loaded_at
    from session_agg s
    left join {{ ref('dim_user') }} u
        on s.user_id = u.user_id and u.dw_is_current = true
    left join {{ ref('dim_geography') }} g
        on s.country_code = g.country_code
)

select * from with_keys
