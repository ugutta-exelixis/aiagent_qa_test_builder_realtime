with subject_summary_report as (
select
study_id
,studysite
,investigator
,country
,subject
,cohort as cohort_desc
,case 
    when split_part(cohort,':',1) like 'Cohort B%' then 'Cohort B'
    when split_part(cohort,':',1) like 'Cohort F%' then 'Cohort F'
    when split_part(cohort,':',1) like 'COHORT H%' then 'Cohort H'
    else split_part(cohort,':',1)
end as cohort
,split_part(cohort,':',2) as treatment_arm
,"study phase"
,"tumor type"
,Null as "documented ras status"
,Null as "liver metastasis"
,Null as region
,status
,"last scheduled visit" as "last visit"
,"last visit date [local]" as "last visit date"
,"next expected visit"
,"next expected visit date [local]" as "next expected visit date"
,partition_date as load_date
,'XB002_' || upper("xb002 initial dose level") as dose_level_treatment_arm_1
,Null as dose_level_treatment_arm_2
,'XB002_' || upper("xb002 initial dose level") as dose_level_treatment_arm
,md5(study_id||studysite||investigator||country||subject) as hash_key
,last_modified_date as IRT_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'subject_summary_report_xb002_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_summary_report_xb002_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_summary_report_xb002_101') }})
union all
select
study_id
,studysite
,investigator
,country
,subject
,cohort as cohort_desc
,cohort
,null as treatment_arm
,Null as "study phase"
,"tumor type"
,Null as "documented ras status"
,Null as "liver metastasis"
,Null as region
,status
,"last visit"
,"last visit date"
,"next expected visit"
,"next expected visit date"
,partition_date as load_date
,Null as dose_level_treatment_arm_1
,Null as dose_level_treatment_arm_2
,Null as dose_level_treatment_arm
,md5(study_id||studysite||investigator||country||subject) as hash_key
,last_modified_date as IRT_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'subject_summary_report_xl092_001') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_summary_report_xl092_001') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_summary_report_xl092_001') }})
union all
select
study_id
,studysite
,investigator
,country
,subject
,cohort as cohort_desc
,split_part(cohort,' ',1) || ' ' || split_part(cohort,' ',2) as cohort
,replace (cohort, split_part(cohort,' ',1) || ' ' || split_part(cohort,' ',2), '') as treatment_arm
,"study phase"
,"tumor type"
,Null as "documented ras status"
,Null as "liver metastasis"
,Null as region
,status
,"last scheduled visit" as "last visit"
,"last visit date [local]" as "last visit date"
,"next expected visit"
,"next expected visit date [local]" as "next expected visit date"
,partition_date as load_date
,'XL092_' || upper("xl092 initial dose level") as dose_level_treatment_arm_1
,Null as dose_level_treatment_arm_2
,'XL092_' || upper("xl092 initial dose level") as dose_level_treatment_arm
,md5(study_id||studysite||investigator||country||subject) as hash_key
,last_modified_date as IRT_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'subject_summary_report_xl092_002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_summary_report_xl092_002') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_summary_report_xl092_002') }})
union all
select
study_id
,studysite
,investigator
,country
,subject
,Null as cohort_desc
,Null as Cohort
,Null as treatment_arm
,Null as "study phase"
,Null as "tumor type"
,"documented ras status"
,"liver metastasis"
,region
,status
,"last scheduled visit" as "last visit"
,"last visit date [local]" as "last visit date"
,"next expected visit"
,"next expected visit date [local]" as "next expected visit date"
,partition_date as load_date
,Null as dose_level_treatment_arm_1
,Null as dose_level_treatment_arm_2
,Null as dose_level_treatment_arm
,md5(study_id||studysite||investigator||country||subject) as hash_key
,last_modified_date as IRT_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'subject_summary_report_xl092_303') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_summary_report_xl092_303') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_summary_report_xl092_303') }})
union all
select
study_id
,studysite
,investigator
,country
,subject
,Null as cohort_desc
,Null as Cohort
,Null as treatment_arm
,Null as "study phase"
,Null as "tumor type"
,null as "documented ras status"
,"liver metastasis"
,null as region
,status
,"last scheduled visit" as "last visit"
,"last visit date"
,"next expected subject record" as "next expected visit"
,"next expected subject record date" as "next expected visit date"
,partition_date as load_date
,Null as dose_level_treatment_arm_1
,Null as dose_level_treatment_arm_2
,Null as dose_level_treatment_arm
,md5(study_id||studysite||investigator||country||subject) as hash_key
,last_modified_date as IRT_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'subject_summary_report_xl184_315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_summary_report_xl184_315') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_summary_report_xl184_315') }})
union all
select
study_id
,studysite
,investigator
,country
,subject
,Null as cohort_desc
,Null as Cohort
,Null as treatment_arm
,Null as "study phase"
,Null as "tumor type"
,null as "documented ras status"
,Null as "liver metastasis"
,null as region
,status
,"last scheduled visit" as "last visit"
,"last visit date [local]" as "last visit date"
,"next expected visit" as "next expected visit"
,"next expected visit date [local]" as "next expected visit date"
,partition_date as load_date
,Null as dose_level_treatment_arm_1
,Null as dose_level_treatment_arm_2
,Null as dose_level_treatment_arm
,md5(study_id||studysite||investigator||country||subject) as hash_key
,last_modified_date as IRT_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'subject_summary_report_xl092-304') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_summary_report_xl092-304') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_summary_report_xl092-304') }})


union all

select 
cast(study_id as varchar)
,case when "site number" ='2124' then '1841'
else "site number" 
end as "site number"
,null as pi_name
,cast(country as varchar)
,cast("subject number" as varchar) as subject
,cast("enrolled cohort" as varchar) as cohort_desc
,Trim(split_part("enrolled cohort",'-',1)) as cohort
,split_part("enrolled cohort",'-',2) as treatment_arm
,Null as "study phase"
,case 
	when Trim(split_part("enrolled cohort",'-',1)) like '%Cohort A%' then 'Advanced Solid Tumors'
	when Trim(split_part("enrolled cohort",'-',1)) like '%Cohort B%' then '1L RCC'
	when Trim(split_part("enrolled cohort",'-',1)) like '%Cohort 1%' then '2L+RCC'
	when Trim(split_part("enrolled cohort",'-',1)) like '%Cohort 2%' then '1L RCC'
	when Trim(split_part("enrolled cohort",'-',1)) is null then 'Advanced Solid Tumors'
    else null
end as "tumor type"
,null as "documented ras status"
,Null as "liver metastasis"
,null as region
,case when status = 'Screened' then 'Screening'
else status
end as status
,null as "last visit"
,"status change" as "last visit date"
,null as "next expected visit"
,null as "next expected visit date"
,partition_date as load_date
,case 
	when "current zanzalintinib dose" is null then null
	else concat("current zanzalintinib dose",' ','zanzalintinib')
end as dose_level_treatment_arm_1
,case 
	when "current ab521 dose" is null then null
	else concat("current ab521 dose",' ','ab521')
end as dose_level_treatment_arm_2
,case
    when "current zanzalintinib dose" is null and "current ab521 dose" is null then null
    when "current zanzalintinib dose" is null then concat(trim("current ab521 dose"),' ','ab521')
    when "current ab521 dose" is null then concat(trim("current zanzalintinib dose"),' ','zanzalintinib')
    else CONCAT(concat(trim("current zanzalintinib dose"),' ','zanzalintinib'), '+', concat(trim("current ab521 dose"),' ','ab521'))
end as dose_level_treatment_arm
,md5(study_id||"site number"||''||country||"subject number") as hash_key
,last_modified_date as IRT_Data_Refreshed_Date
from 
{{ source ('koios_raw', 'cenduit_subject_detail_xl092-009') }} 
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'cenduit_subject_detail_xl092-009') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'cenduit_subject_detail_xl092-009') }})


union all

select 
    cast(study_id as varchar)
    ,"site number"
    ,null as pi_name
    ,case when country = 'KOREA (THE REPUBLIC OF)' then 'SOUTH KOREA'
    when country = 'CZECHIA' then 'CZECH REPUBLIC'
    when country = 'UNITED STATES OF AMERICA' then 'UNITED STATES'
    else cast(country as varchar)
    end as  country
    ,cast("subject number" as varchar) as subject
    ,null as cohort_desc
    ,null as cohort
    ,null as treatment_arm
    ,Null as "study phase"
    ,null as "tumor type"
    ,null as "documented ras status"
    ,Null as "liver metastasis"
    ,null as region
    ,case 
        when status = 'Screened' then 'Screening'
        else status
    end as status
    ,null as "last visit"
    ,"status change" as "last visit date"
    ,null as "next expected visit"
    ,null as "next expected visit date"
    ,partition_date as load_date
    ,case 
        when "zanzalintinib/placebo current dose" is null then null
        else concat("zanzalintinib/placebo current dose",' ','zanzalintinib')
    end as dose_level_treatment_arm_1
    ,null as dose_level_treatment_arm_2
    ,case 
        when "zanzalintinib/placebo current dose" is null then null
        else concat("zanzalintinib/placebo current dose",' ','zanzalintinib')
    end as dose_level_treatment_arm
    ,md5(study_id||"site number"||''||country||"subject number") as hash_key
    ,last_modified_date as IRT_Data_Refreshed_Date
from 
{{ source ('koios_raw', 'cenduit_subject_detail_xl092-305') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'cenduit_subject_detail_xl092-305') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'cenduit_subject_detail_xl092-305') }})
)

select * from subject_summary_report