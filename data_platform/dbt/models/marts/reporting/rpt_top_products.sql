{{
  config(
    materialized='table',
    tags=['reporting', 'products']
  )
}}

with product_views as (
    select
        product_id,
        product_name,
        category,
        price_tier,
        count(event_id)                     as total_views,
        count(distinct user_id)             as unique_viewers,
        avg(view_duration_seconds)          as avg_view_duration_sec,
        countif(image_clicked)              as image_click_count
    from {{ ref('stg_product_events') }}
    where event_timestamp >= dateadd(day, -30, current_date())
    group by 1, 2, 3, 4
),

product_revenue as (
    select
        i.product_id,
        sum(i.quantity)                     as units_sold,
        sum(i.line_total)                   as gross_revenue,
        count(distinct o.order_id)          as orders_containing_product
    from {{ ref('fct_order_items') }} i
    join {{ ref('fct_orders') }} o using (order_id)
    where o.order_date >= dateadd(day, -30, current_date())
    group by 1
),

combined as (
    select
        v.product_id,
        v.product_name,
        v.category,
        v.price_tier,
        coalesce(v.total_views, 0)              as total_views,
        coalesce(v.unique_viewers, 0)           as unique_viewers,
        coalesce(v.avg_view_duration_sec, 0)    as avg_view_duration_sec,
        coalesce(r.units_sold, 0)               as units_sold,
        coalesce(r.gross_revenue, 0)            as gross_revenue,
        coalesce(r.orders_containing_product, 0) as order_count,
        -- View-to-purchase conversion
        case
            when coalesce(v.unique_viewers, 0) > 0
            then round(coalesce(r.orders_containing_product, 0) * 100.0
                       / v.unique_viewers, 2)
            else 0
        end                                     as view_to_purchase_pct,
        -- Revenue per view
        case
            when coalesce(v.total_views, 0) > 0
            then round(coalesce(r.gross_revenue, 0) / v.total_views, 2)
            else 0
        end                                     as revenue_per_view,
        row_number() over (
            partition by v.category
            order by coalesce(r.gross_revenue, 0) desc
        )                                       as rank_in_category,
        row_number() over (
            order by coalesce(r.gross_revenue, 0) desc
        )                                       as overall_rank,
        current_timestamp()                     as refreshed_at
    from product_views v
    left join product_revenue r using (product_id)
)

select * from combined
order by overall_rank
