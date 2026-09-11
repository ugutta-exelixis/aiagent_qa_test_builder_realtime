with dim_cohort_summary as (
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
    ,load_date
    ,study_id
from 
{{ref ('ctms_stg__cohort_summary_report')}}
)
select * from dim_cohort_summary