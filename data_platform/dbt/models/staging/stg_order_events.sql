{{
  config(
    materialized='view',
    tags=['staging', 'orders']
  )
}}

/*
Staging layer for order events.
Responsibilities:
  - Rename columns to snake_case standard
  - Cast data types explicitly
  - Apply basic deduplication (idempotent Kafka delivery can create dupes)
  - Filter out obviously corrupt records
  - Flatten the items VARIANT column into a lateral join
*/

with source as (
    select * from {{ source('raw_events', 'order_events') }}
),

deduplicated as (
    select *,
        row_number() over (
            partition by event_id
            order by _loaded_at asc
        ) as row_num
    from source
    where event_id is not null
      and order_id is not null
      and total_amount > 0
      and timestamp >= dateadd(day, -{{ var('lookback_days') }}, current_date())
),

cleaned as (
    select
        event_id,
        event_type,
        timestamp::timestamp_ntz                as event_timestamp,
        session_id,
        user_id,
        order_id,
        lower(trim(device_type))                as device_type,
        upper(trim(country))                    as country_code,
        subtotal::number(12,2)                  as subtotal,
        tax_amount::number(10,2)                as tax_amount,
        shipping_amount::number(10,2)           as shipping_amount,
        discount_amount::number(10,2)           as discount_amount,
        total_amount::number(12,2)              as total_amount,
        upper(trim(currency))                   as currency,
        lower(trim(payment_method))             as payment_method,
        coupon_code,
        coalesce(is_first_order, false)         as is_first_order,
        items_json,
        _loaded_at
    from deduplicated
    where row_num = 1
)

select * from cleaned
