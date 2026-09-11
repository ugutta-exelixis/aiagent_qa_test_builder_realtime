with ctms_stg__cohort_summary_report as(
     select 
    "cohort group"	
    ,cohort	
    ,"cohort description"	
    ,"cohort status"	
    ,null as "rank"
    ,"screening cap"	
    ,"enrollment cap"	
    ,"total screened"	
    ,"total enrolled"	
    ,"dosing scheme"	
    ,"dose level"	
    ,null as "list of approved countries" 
    ,partition_date	as load_date 
    ,study_id
    FROM {{ source ('koios_raw', 'xb002_101_cohort_summary_report') }}
        where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xb002_101_cohort_summary_report') }})
        and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xb002_101_cohort_summary_report') }})
union all	
    select 
    "cohort group"
    ,cohort	
    ,"cohort description"	
    ,"cohort status"	
    ,"rank"	
    ,"screening cap"	
    ,"enrollment cap"	
    ,"total screened"	
    ,"total enrolled"	
    ,"dosing scheme"	
    ,"dose level"	
    ,"list of approved countries"	
    ,partition_date	as load_date
    ,study_id
    FROM {{ source ('koios_raw', 'prancer_cohort_summary_report_xl092_002') }}
        where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'prancer_cohort_summary_report_xl092_002') }})
        and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'prancer_cohort_summary_report_xl092_002') }})
union all 
    select 
    "cohort group"	
    ,cohort	
    ,"cohort description"	
    ,"cohort status"	
    ,null as "rank"
    ,"screening cap"	
    ,"enrollment cap"	
    ,"total screened"	
    ,"total enrolled"	
    ,"dosing scheme"	
    ,"dose level"	
    ,null as "list of approved countries" 
    ,partition_date	as load_date 
    ,study_id
    FROM {{ source ('koios_raw', 'xl092-009_cohort_summary_report') }}
        where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-009_cohort_summary_report') }})
        and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-009_cohort_summary_report') }})
)

select * from ctms_stg__cohort_summary_report
