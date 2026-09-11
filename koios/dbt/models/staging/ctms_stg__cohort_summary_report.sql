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
    -- ,"enrollment cap"
    ,case 
        when cohort in('12_A1','12_A2','13_A1', '13_A2') then '80'
        else "enrollment cap"
    end as "enrollment cap"		
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
union all 
    select 
    case when "cohort group" = 'Single-Agent' then 'SOLID TUMORS'
    else "cohort group" end as "cohort group"
    -- ,cohort	
     ,case when cohort = 'A11' and "cohort description" = 'Cohort A5b: 2.8 mg/kg' then 'A5b'	
     else cohort
     end as cohort 
    ,"cohort description"	
    ,"cohort status"	
    ,"rank"
    ,"screening cap"	
    ,"enrollment cap"	
    ,"total screened"	
    ,"total enrolled"	
    ,"dosing scheme"	
    ,null as "dose level"	
    ,null as "list of approved countries" 
    ,partition_date	as load_date 
    ,study_id
    FROM {{ source ('koios_raw', 'prancer_cohort_summary_report_xb010_101') }}
        where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'prancer_cohort_summary_report_xb010_101') }})
        and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'prancer_cohort_summary_report_xb010_101') }})
    union all
    select distinct
    "cohort group"
    ,cohort	
    ,"cohort description"	
    ,"cohort status"	
    ,0 as "rank"
    ,0 as "screening cap"	
    ,"enrollment cap"	
    ,0 as "total screened"	
    ,"total enrolled"	
    ,"dosing scheme"	
    ,"dose level"	
    ,null as "list of approved countries" 
    ,partition_date	as load_date 
    ,study_id
    FROM {{ source ('koios_raw', 'prancer_cohort_summary_report_xl495_101') }}
        where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'prancer_cohort_summary_report_xl495_101') }})
        and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'prancer_cohort_summary_report_xl495_101') }})
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
    ,"current dose level" as "dose level"	
    ,null as "list of approved countries" 
    ,partition_date	as load_date 
    ,study_id
    FROM {{ source ('koios_raw', 'xl309-101_cohort_summary_report') }}
        where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl309-101_cohort_summary_report') }})
        and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl309-101_cohort_summary_report') }})
union all 
    select 
    "cohort group"	
    ,cohort 
    ,"cohort description"	
    ,"cohort status"	
    ,null as "rank"
    ,null as "screening cap"	
    ,"enrollment cap"	
    ,null as "total screened"	
    ,"total enrolled"	
    ,"dosing scheme"	
    ,"dose level" as "dose level"	
    ,null as "list of approved countries" 
    ,partition_date	as load_date 
    ,study_id
    FROM {{ source ('koios_raw', 'cohort_summary_report_xb628-101') }}
        where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'cohort_summary_report_xb628-101') }})
        and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'cohort_summary_report_xb628-101') }})

union all 
    select 
    "cohort group"
    ,cohort	
    ,"cohort description"	
    ,"cohort status"	
    ,null as "rank"
    ,null as "screening cap"	
    ,"enrollment cap"	
    ,"total screened"	
    ,"total enrolled"	
    ,"dosing scheme"	
    ,"dose level"	
    ,null as "list of approved countries" 
    ,partition_date	as load_date 
    ,study_id
    FROM {{ source ('koios_raw', 'cohort_summary_report_xb371-101') }}
        where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'cohort_summary_report_xb371-101') }})
        and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'cohort_summary_report_xb371-101') }})

union all 
    select 
    "cohort group"
    ,cohort	
    ,concat('Cohort ', cohort, ': ', "cohort description") as "cohort description"	
    ,"cohort status"	
    ,null as "rank"
    ,null as "screening cap"	
    ,"enrollment cap"	
    ,null as "total screened"	
    ,"total enrolled"	
    ,null as "dosing scheme"	
    ,"allowed dose levels" as "dose level"	
    ,null as "list of approved countries" 
    ,partition_date	as load_date 
    ,study_id
    FROM {{ source ('koios_raw', 'cohort_summary_report_xl092-011') }}
        where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'cohort_summary_report_xl092-011') }})
        and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'cohort_summary_report_xl092-011') }})


)

select * from ctms_stg__cohort_summary_report
