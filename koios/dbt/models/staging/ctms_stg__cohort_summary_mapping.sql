with ctms_stg__cohort_summary_mapping as (
    select *
    FROM {{ source ('koios_raw', 'cohort_summary_mapping') }}
    where partition_date = (select max(partition_date) from {{ source ('koios_raw', 'cohort_summary_mapping') }})
)

select * from ctms_stg__cohort_summary_mapping