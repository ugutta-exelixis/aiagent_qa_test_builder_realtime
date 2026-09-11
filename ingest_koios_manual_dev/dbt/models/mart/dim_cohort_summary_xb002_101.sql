with dim_cohort_summary_xl092_002 as (
    select distinct * from {{ref ('ctms_stg__cohort_summary_report_xb002_101')}}
)

select * from dim_cohort_summary_xl092_002