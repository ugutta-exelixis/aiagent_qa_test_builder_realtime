{% snapshot ctms_snapshots__study_site_monitor %}


{{
    config(
      unique_key='hash_key',
      strategy='check',
      check_cols='all',
      target_schema='koios_snapshots'
    )
}}


select * from {{ ref('ctms_stg__study_site_monitor')}}


{% endsnapshot %}