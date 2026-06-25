{{
  config(
    materialized='table',
    tags=['reporting', 'executive'],
    post_hook="GRANT SELECT ON {{ this }} TO ROLE REPORTING_ROLE"
  )
}}

/*
Executive Summary — daily KPIs for C-suite dashboard.
Refreshed daily by Airflow. Power BI reads directly from this table.
*/

with daily_orders as (
    select
        order_date,
        country_code,
        count(distinct order_id)                as order_count,
        count(distinct user_id)                 as unique_buyers,
        sum(total_amount)                       as gross_revenue,
        sum(discount_amount)                    as total_discounts,
        sum(total_amount - discount_amount)     as net_revenue,
        avg(total_amount)                       as avg_order_value,
        countif(is_first_order)                 as new_customer_orders,
        countif(not is_first_order)             as repeat_customer_orders
    from {{ ref('fct_orders') }}
    where order_date >= dateadd(day, -90, current_date())
    group by 1, 2
),

daily_sessions as (
    select
        session_date,
        count(distinct session_id)              as total_sessions,
        count(distinct user_id)                 as unique_visitors,
        countif(did_purchase)                   as converting_sessions,
        round(
            countif(did_purchase) * 100.0 / nullif(count(*), 0), 2
        )                                       as conversion_rate_pct,
        avg(duration_seconds)                   as avg_session_duration_sec,
        avg(unique_products_viewed)             as avg_products_per_session
    from {{ ref('fct_sessions') }}
    where session_date >= dateadd(day, -90, current_date())
    group by 1
),

combined as (
    select
        coalesce(o.order_date, s.session_date)  as report_date,
        coalesce(o.country_code, 'ALL')         as country_code,
        -- Revenue metrics
        coalesce(o.order_count, 0)              as order_count,
        coalesce(o.unique_buyers, 0)            as unique_buyers,
        coalesce(o.gross_revenue, 0)            as gross_revenue,
        coalesce(o.net_revenue, 0)              as net_revenue,
        coalesce(o.avg_order_value, 0)          as avg_order_value,
        coalesce(o.new_customer_orders, 0)      as new_customer_orders,
        coalesce(o.repeat_customer_orders, 0)   as repeat_customer_orders,
        -- Session metrics
        coalesce(s.total_sessions, 0)           as total_sessions,
        coalesce(s.unique_visitors, 0)          as unique_visitors,
        coalesce(s.conversion_rate_pct, 0)      as conversion_rate_pct,
        coalesce(s.avg_session_duration_sec, 0) as avg_session_duration_sec,
        -- Derived
        case
            when coalesce(s.total_sessions, 0) > 0
            then round(coalesce(o.order_count, 0) * 100.0 / s.total_sessions, 2)
            else 0
        end                                     as sessions_to_order_pct,
        round(
            coalesce(o.new_customer_orders, 0) * 100.0
            / nullif(coalesce(o.order_count, 0), 0), 2
        )                                       as new_customer_pct,
        current_timestamp()                     as refreshed_at
    from daily_orders o
    full outer join daily_sessions s
        on o.order_date = s.session_date
)

select * from combined
order by report_date desc, country_code
