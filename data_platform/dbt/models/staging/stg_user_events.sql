{{
  config(
    materialized='view',
    tags=['staging', 'users']
  )
}}

with source as (
    select * from {{ source('raw_events', 'user_events') }}
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
),

cleaned as (
    select
        event_id,
        event_type,
        timestamp::timestamp_ntz                    as event_timestamp,
        session_id,
        user_id,
        lower(trim(device_type))                    as device_type,
        lower(trim(email))                          as email,
        initcap(trim(first_name))                   as first_name,
        initcap(trim(last_name))                    as last_name,
        trim(first_name) || ' ' || trim(last_name)  as full_name,
        date_of_birth::date                         as date_of_birth,
        datediff('year', date_of_birth::date, current_date()) as age,
        case
            when datediff('year', date_of_birth::date, current_date()) < 25 then '18-24'
            when datediff('year', date_of_birth::date, current_date()) < 35 then '25-34'
            when datediff('year', date_of_birth::date, current_date()) < 45 then '35-44'
            when datediff('year', date_of_birth::date, current_date()) < 55 then '45-54'
            else '55+'
        end                                         as age_bucket,
        upper(trim(gender))                         as gender,
        upper(trim(country))                        as country_code,
        city,
        lower(trim(referral_source))                as referral_source,
        coalesce(marketing_opt_in, false)           as marketing_opt_in,
        _loaded_at
    from deduplicated
    where row_num = 1
      and email like '%@%.%'  -- basic email format check
)

select * from cleaned
