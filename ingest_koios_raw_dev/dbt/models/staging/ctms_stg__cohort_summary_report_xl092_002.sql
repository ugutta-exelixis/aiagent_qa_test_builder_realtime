with ctms_stg__cohort_summary_report_xl092_002 as (
    select *
    FROM {{ source ('koios_raw', 'prancer_cohort_summary_report_xl092_002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'prancer_cohort_summary_report_xl092_002') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'prancer_cohort_summary_report_xl092_002') }})
)

select * from ctms_stg__cohort_summary_report_xl092_002