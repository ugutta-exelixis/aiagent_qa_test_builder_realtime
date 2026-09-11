{% snapshot ctms_snapshots__subject_visit_summary_report %}


{{
    config(
      unique_key='hash_key',
      strategy='check',
      check_cols='all',
      target_schema='koios_snapshots'
    )
}}


select * from {{ref('ctms_stg__subject_visit_summary_report')}}


{% endsnapshot %}