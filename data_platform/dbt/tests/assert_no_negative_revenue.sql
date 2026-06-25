-- Data quality test: no order should have negative total_amount.
-- Negative values indicate a refund that wasn't handled as a separate event.

select order_id, total_amount
from {{ ref('fct_orders') }}
where total_amount < 0
