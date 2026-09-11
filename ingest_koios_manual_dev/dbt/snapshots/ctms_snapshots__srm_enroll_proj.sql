{% snapshot ctms_snapshots__srm_enroll_proj %}


{{
    config(
      unique_key='hash_key',
      strategy='check',
      check_cols='all',
      target_schema='koios_snapshots'
    )
}}


select * from {{ ref('ctms_stg__srm_enroll_proj')}}


{% endsnapshot %}