with ctms_stg__cohort_summary_report_xb002_101 as (
    select 
    "cohort group"
    ,case 
        when "cohort group" = 'NSCLC' then 'Cohort B'
        else split_part("cohort description",':',1) 
    end as cohort
    ,"cohort description"
    ,"cohort status"
    ,"screening cap"
    ,"enrollment cap"
    ,"total screened"
    ,"total enrolled"
    ,"dosing scheme"
    ,"dose level"
    ,partition_date
    ,last_modified_date
    ,study_id
    FROM {{ source ('koios_raw', 'xb002_101_cohort_summary_report') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xb002_101_cohort_summary_report') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xb002_101_cohort_summary_report') }})
)

select * from ctms_stg__cohort_summary_report_xb002_101