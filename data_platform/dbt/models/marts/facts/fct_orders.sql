{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    cluster_by=['order_date', 'country_code'],
    tags=['marts', 'orders', 'finance'],
    post_hook=[
      "GRANT SELECT ON {{ this }} TO ROLE ANALYST_ROLE",
      "GRANT SELECT ON {{ this }} TO ROLE FINANCE_ROLE"
    ]
  )
}}

/*
fact_orders — grain: one row per order.

Incremental merge strategy:
  - On first run, loads all historical orders.
  - On subsequent runs, merges orders from the last 3 days to handle
    late-arriving events (e.g. payment confirmation arriving after order_placed).
*/

with orders as (
    select * from {{ ref('stg_order_events') }}
    where event_type = 'order_placed'

    {% if is_incremental() %}
    -- Lookback 3 days to catch late-arriving payment confirmations
    and event_timestamp >= dateadd(day, -3, current_date())
    {% endif %}
),

users as (
    select user_id, user_sk, customer_segment
    from {{ ref('dim_user') }}
    where dw_is_current = true
),

geo as (
    select country_code, geo_sk
    from {{ ref('dim_geography') }}
),

payments as (
    select payment_method, payment_sk
    from {{ ref('dim_payment_method') }}
),

enriched as (
    select
        o.order_id,
        o.event_id,
        o.event_timestamp                           as order_timestamp,
        to_date(o.event_timestamp)                  as order_date,
        to_number(
            to_char(o.event_timestamp, 'YYYYMMDD')
        )                                           as date_sk,
        o.session_id,
        o.user_id,
        u.user_sk,
        u.customer_segment,
        g.geo_sk,
        o.country_code,
        p.payment_sk,
        o.payment_method,
        o.device_type,
        o.coupon_code,
        o.coupon_code is not null                   as has_coupon,
        o.is_first_order,
        o.subtotal,
        o.tax_amount,
        o.shipping_amount,
        o.discount_amount,
        o.total_amount,
        -- Derived
        case
            when o.subtotal > 0
            then round((o.subtotal - o.discount_amount) / o.subtotal * 100, 2)
            else 0
        end                                         as effective_discount_pct,
        current_timestamp()                         as _loaded_at
    from orders o
    left join users u on o.user_id = u.user_id
    left join geo   g on o.country_code = g.country_code
    left join payments p on o.payment_method = p.payment_method
)

select * from enriched
