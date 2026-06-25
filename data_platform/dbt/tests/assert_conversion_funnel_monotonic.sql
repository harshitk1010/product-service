-- Funnel must be monotonically decreasing:
-- sessions_with_views >= sessions_add_to_cart >= sessions_checkout >= sessions_purchased
-- A violation means a session tracking bug (e.g. checkout events without a prior view).

select
    session_date,
    sessions_with_views,
    sessions_add_to_cart,
    sessions_checkout,
    sessions_purchased
from {{ ref('rpt_conversion_funnel') }}
where
    sessions_add_to_cart > sessions_with_views
    or sessions_checkout > sessions_add_to_cart
    or sessions_purchased > sessions_checkout
