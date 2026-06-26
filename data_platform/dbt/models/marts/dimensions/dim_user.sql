{{
  config(
    materialized='table',
    tags=['marts', 'dimensions', 'scd2'],
    post_hook="ALTER TABLE {{ this }} CLUSTER BY (user_id, dw_is_current)"
  )
}}

/*
dim_user — SCD Type 2 implementation.
Strategy: snapshot via dbt snapshots (see snapshots/snap_users.sql).
This model reads from the snapshot and adds business-derived attributes.

SCD Type 2 means: when a user updates their email or address, we close
the old record (dw_effective_to = now, dw_is_current = false) and insert
a new one. This preserves history for historical analysis.
*/

with user_snapshot as (
    select * from {{ ref('snap_users') }}
),

enriched as (
    select
        user_id,
        email,
        first_name,
        last_name,
        full_name,
        date_of_birth,
        age_bucket,
        gender,
        country_code,
        city,
        referral_source,
        marketing_opt_in,
        -- Business-derived segment (computed at snapshot time, not current)
        case
            when dbt_valid_from >= dateadd(day, -30, current_date())
            then 'new'
            when dbt_valid_from >= dateadd(day, -90, current_date())
            then 'returning'
            else 'established'
        end                             as customer_segment,
        -- SCD2 metadata
        dbt_valid_from                  as dw_effective_from,
        dbt_valid_to                    as dw_effective_to,
        dbt_valid_to is null            as dw_is_current,
        current_timestamp()             as dw_updated_at
    from user_snapshot
)

select
    {{ dbt_utils.generate_surrogate_key(['user_id', 'dw_effective_from']) }} as user_sk,
    *
from enriched
