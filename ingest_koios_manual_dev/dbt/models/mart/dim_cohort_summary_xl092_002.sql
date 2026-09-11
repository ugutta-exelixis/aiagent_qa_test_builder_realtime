with dim_cohort_summary_xl092_002 as (
    select distinct * from {{ref ('ctms_stg__cohort_summary_report_xl092_002')}}
)

select * from dim_cohort_summary_xl092_002