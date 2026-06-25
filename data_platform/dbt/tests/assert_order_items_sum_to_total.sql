-- Data quality test: sum of order items must equal order subtotal (within rounding).
-- Catches extraction bugs in the items_json flattening logic.

with order_totals as (
    select order_id, subtotal
    from {{ ref('fct_orders') }}
),

item_sums as (
    select order_id, sum(line_total) as items_total
    from {{ ref('fct_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.subtotal,
    i.items_total,
    abs(o.subtotal - i.items_total) as discrepancy
from order_totals o
join item_sums i using (order_id)
where abs(o.subtotal - i.items_total) > 0.05  -- 5-cent tolerance for rounding
