{% snapshot ctms_snapshots__address_association %}


{{
    config(
      unique_key='hash_key',
      strategy='check',
      check_cols='all',
      target_schema='koios_snapshots'
    )
}}


select * from {{ ref('ctms_stg__address_association')}}


{% endsnapshot %}