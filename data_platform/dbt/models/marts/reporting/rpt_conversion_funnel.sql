{{
  config(
    materialized='table',
    tags=['reporting', 'funnel']
  )
}}

/*
Conversion Funnel — daily funnel metrics.
Shows drop-off at each stage: view → search → cart → checkout → purchase.
Used directly by Power BI Funnel Dashboard.
*/

with sessions as (
    select
        session_date,
        count(distinct session_id)              as total_sessions,
        countif(did_search)                     as sessions_with_search,
        countif(unique_products_viewed > 0)     as sessions_with_views,
        countif(did_add_to_cart)                as sessions_add_to_cart,
        countif(did_checkout)                   as sessions_checkout,
        countif(did_purchase)                   as sessions_purchased
    from {{ ref('fct_sessions') }}
    where session_date >= dateadd(day, -30, current_date())
    group by 1
)

select
    session_date,
    total_sessions,
    sessions_with_views,
    sessions_with_search,
    sessions_add_to_cart,
    sessions_checkout,
    sessions_purchased,
    -- Stage conversion rates (relative to previous stage)
    round(sessions_with_views * 100.0 / nullif(total_sessions, 0), 2)
        as view_rate_pct,
    round(sessions_add_to_cart * 100.0 / nullif(sessions_with_views, 0), 2)
        as view_to_cart_pct,
    round(sessions_checkout * 100.0 / nullif(sessions_add_to_cart, 0), 2)
        as cart_to_checkout_pct,
    round(sessions_purchased * 100.0 / nullif(sessions_checkout, 0), 2)
        as checkout_to_purchase_pct,
    -- Overall funnel conversion
    round(sessions_purchased * 100.0 / nullif(total_sessions, 0), 2)
        as overall_conversion_pct,
    current_timestamp() as refreshed_at
from sessions
order by session_date desc
