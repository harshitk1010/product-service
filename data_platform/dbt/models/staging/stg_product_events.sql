{{
  config(
    materialized='view',
    tags=['staging', 'products']
  )
}}

with source as (
    select * from {{ source('raw_events', 'product_events') }}
),

deduplicated as (
    select *,
        row_number() over (
            partition by event_id
            order by _loaded_at asc
        ) as row_num
    from source
    where event_id is not null
      and user_id is not null
      and product_id is not null
),

cleaned as (
    select
        event_id,
        event_type,
        timestamp::timestamp_ntz            as event_timestamp,
        session_id,
        user_id,
        lower(trim(device_type))            as device_type,
        upper(trim(country))                as country_code,
        upper(trim(product_id))             as product_id,
        trim(product_name)                  as product_name,
        initcap(trim(category))             as category,
        initcap(trim(subcategory))          as subcategory,
        price::number(10,2)                 as price,
        case
            when price < 25  then 'budget'
            when price < 100 then 'mid-range'
            when price < 500 then 'premium'
            else 'luxury'
        end                                 as price_tier,
        upper(trim(currency))               as currency,
        trim(brand)                         as brand,
        view_duration_seconds::int          as view_duration_seconds,
        coalesce(image_clicked, false)      as image_clicked,
        recommendation_source,
        _loaded_at
    from deduplicated
    where row_num = 1
)

select * from cleaned
