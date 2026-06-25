{% snapshot snap_users %}

{{
    config(
      target_database='ANALYTICS_DB',
      target_schema='snapshots',
      unique_key='user_id',
      strategy='check',
      check_cols=[
          'email', 'first_name', 'last_name', 'date_of_birth',
          'gender', 'country_code', 'city', 'marketing_opt_in'
      ],
      invalidate_hard_deletes=True
    )
}}

/*
SCD Type 2 snapshot for user dimension.
Tracks changes to any of the check_cols — when a user updates their
email or address, dbt closes the old record and opens a new one.

dbt snapshot columns added automatically:
  dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to
*/

select
    user_id,
    email,
    first_name,
    last_name,
    trim(first_name) || ' ' || trim(last_name)  as full_name,
    date_of_birth,
    age_bucket,
    gender,
    country_code,
    city,
    referral_source,
    marketing_opt_in,
    _loaded_at                                   as source_loaded_at
from {{ ref('stg_user_events') }}
where event_type = 'user_registered'

{% endsnapshot %}
