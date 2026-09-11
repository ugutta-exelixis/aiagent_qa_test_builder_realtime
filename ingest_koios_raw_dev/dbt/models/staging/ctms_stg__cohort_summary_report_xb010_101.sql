with ctms_stg__cohort_summary_report_xb010_101 as (
    select *
    FROM {{ source ('koios_raw', 'prancer_cohort_summary_report_xb010_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'prancer_cohort_summary_report_xb010_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'prancer_cohort_summary_report_xb010_101') }})
)

select * from ctms_stg__cohort_summary_report_xb010_101