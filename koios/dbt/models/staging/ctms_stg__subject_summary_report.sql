with subject_summary_report as (
select
study_id
,cast ("study site" as varchar) as studysite
,investigator
,country
,cast (subject as varchar) as subject
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
,"last visit" as "last visit"
,"last visit date [local]" as "last visit date"
,"next expected visit"
,"next expected visit date [local]" as "next expected visit date"
,partition_date as load_date
,'XB002_' || upper("xb002 initial dose level") as dose_level_treatment_arm_1
,Null as dose_level_treatment_arm_2
,'XB002_' || upper("xb002 initial dose level") as dose_level_treatment_arm
,md5(study_id||"study site"||investigator||country||subject) as hash_key
,last_modified_date as IRT_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'subject_summary_report_xb002_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_summary_report_xb002_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_summary_report_xb002_101') }})
union all
select
study_id
,cast (studysite as varchar) as studysite
,investigator
,country
,cast (subject as varchar) as subject
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
,cast (studysite as varchar) as studysite
,investigator
,country
,cast (subject as varchar) as subject
,cohort as cohort_desc
,case 
when "last visit date [local]" is Null or "last visit date [local]" = '' THEN split_part(cohort,' ',1) || ' ' || split_part(cohort,' ',2)
When lower("last visit date [local]") !~ '^[0-9]{1,2}-[a-z]{3}-[0-9]{4}$' then split_part(cohort,' ',1) || ' ' || split_part(cohort,' ',2)
when TO_DATE("last visit date [local]", 'DD-Mon-YYYY') < TO_DATE('08-Feb-2024','DD-Mon-YYYY') and "tumor type" = 'ccRCC 1L' and cohort is null then 'Cohort 1'
when TO_DATE("last visit date [local]", 'DD-Mon-YYYY') >= TO_DATE('08-Feb-2024','DD-Mon-YYYY') and TO_DATE("last visit date [local]", 'DD-Mon-YYYY') <= TO_DATE('21-Feb-2025','DD-Mon-YYYY') and "tumor type" = 'ccRCC 1L' and cohort is null then 'Cohort 13'
when TO_DATE("last visit date [local]", 'DD-Mon-YYYY') > TO_DATE('21-Feb-2025','DD-Mon-YYYY') and "tumor type" = 'ccRCC 1L' and cohort is null then 'Cohort 14'
else split_part(cohort,' ',1) || ' ' || split_part(cohort,' ',2)
END as cohort
,replace (cohort, split_part(cohort,' ',1) || ' ' || split_part(cohort,' ',2), '') as treatment_arm
,"study phase"
,"tumor type"
,Null as "documented ras status"
,Null as "liver metastasis"
,Null as region
,status
,"last visit" as "last visit"
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
,cast (studysite as varchar) as studysite
,investigator
,country
,cast (subject as varchar) as subject
,Null as cohort_desc
,Null as Cohort
,Null as treatment_arm
,Null as "study phase"
,Null as "tumor type"
,"documented ras status"
-- ,"liver metastasis"
,"presence of liver metastasis" as "liver metastasis"
,region
,status
,"last visit" as "last visit"
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
,cast (studysite as varchar) as studysite
,investigator
,country
,cast (subject as varchar) as subject
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
,cast("study site" as varchar) as studysite
,investigator
,case 
    when upper(country)='UNITED STATES OF AMERICA' then  'UNITED STATES'
    when upper(country)='CZECHIA' then  'CZECH REPUBLIC'
    else upper(country)
end AS country
,cast (subject as varchar) as subject
,Null as cohort_desc
,Null as Cohort
,Null as treatment_arm
,Null as "study phase"
,Null as "tumor type"
,null as "documented ras status"
,Null as "liver metastasis"
,null as region
,status
,"last visit" as "last visit"
,"last visit date [local]" as "last visit date"
,"next expected visit" as "next expected visit"
,"next expected visit date [local]" as "next expected visit date"
,partition_date as load_date
,Null as dose_level_treatment_arm_1
,Null as dose_level_treatment_arm_2
,Null as dose_level_treatment_arm
,md5(study_id||"study site"||investigator||country||subject) as hash_key
,last_modified_date as IRT_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'subject_summary_report_xl092-304') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_summary_report_xl092-304') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_summary_report_xl092-304') }})


union all

select 
cast(study_id as varchar)
,cast(case when "site number" ='2124' then '1841'
else "site number" 
end as  varchar) as studysite
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
    ,cast("site number" as varchar ) as studysite
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

union all

select
study_id,
cast (studysite as varchar) as studysite,
investigator,
country,
cast (subject as varchar) as subject,
Null as cohort_desc,
cohort as Cohort,
Null as treatment_arm,
"study phase",
"tumor type",
null as "documented ras status",
null as "liver metastasis",
null as region,
status,
"last scheduled visit" as "last visit",
"last visit date [local]" as "last visit date",
"next expected visit",
"next expected visit date [local]" as "next expected visit date",
partition_date as load_date,
Null as dose_level_treatment_arm_1,
Null as dose_level_treatment_arm_2,
Null as dose_level_treatment_arm,
md5(study_id||studysite||investigator||country||subject) as hash_key,
last_modified_date as IRT_Data_Refreshed_Date
FROM    
{{ source ('koios_raw', 'subject_summary_report_xl114_101') }}    
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_summary_report_xl114_101') }})    
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_summary_report_xl114_101') }} )


union all
select
study_id,
cast("study site" as varchar) as studysite,
investigator,
country,
cast (a.subject as varchar) as subject,
case when coalesce(a.cohort,b.cohort) in('Not available','Screen Fail') then a.cohort 
else coalesce(a.cohort,b.cohort)
end  as cohort_desc,
case when  coalesce(a."study phase", b."Study Phase") = 'Expansion' then (array_to_string(
           (regexp_split_to_array(coalesce(a.cohort,b.cohort), ' '))[:2],' '))
	 -- when split_part(cohort,':',1) = 'Cohort A5b' then 'Cohort A11'
else split_part(a.cohort,':',1)
end as cohort,
-- split_part(coalesce(a.cohort ,b.cohort),':',2) as treatment_arm,
case when coalesce(a.cohort,b.cohort) in('Not available','Screen Fail') then a.cohort
else split_part(coalesce(a.cohort ,b.cohort),':',2)
end as treatment_arm,
coalesce(a."study phase", b."Study Phase") as study_phase,
case when coalesce(a."study phase", b."Study Phase") <> 'Expansion' then 'SOLID TUMORS'
   when coalesce(a."study phase", b."Study Phase") = 'Expansion' and coalesce(a.cohort,b.cohort) is not null 
    then 
        case 
            when array_to_string(
                    (regexp_split_to_array(coalesce(a.cohort,b.cohort), ' '))[3:7],
                    ' '
                ) = 'Hormone Receptor-Positive Breast Cancer (non-randomized):'
            then 'Hormone Receptor-Positive Breast Cancer'
            
            else array_to_string(
                    (regexp_split_to_array(coalesce(a.cohort,b.cohort), ' '))[3:7],
                    ' '
                )
            end
  else null
end as "tumor type" ,
null as "documented ras status",
null as "liver metastasis",
null as region,
a.status,
"last visit" as "last visit",
"last visit date" as "last visit date",
"next expected visit",
"next expected visit date" as "next expected visit date",
partition_date as load_date,
Null as dose_level_treatment_arm_1,
Null as dose_level_treatment_arm_2,
'XB010_' || upper(split_part(a.cohort, ':',2)) as dose_level_treatment_arm,
md5(study_id||"study site"||investigator||country||a.subject) as hash_key,
a.last_modified_date as IRT_Data_Refreshed_Date
FROM    
{{ source ('koios_raw', 'subject_summary_report_xb010_101') }}  a
left join {{source('koios_raw','xb010-101_study_phase_ref')}}  b
on a.subject = b.subject
where a.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_summary_report_xb010_101') }})    
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_summary_report_xb010_101') }} )

union all

select
study_id,
cast("study site" as varchar) as studysite,
investigator,
country,
cast(subject as varchar) as subject,
cohort as cohort_desc,
split_part(cohort,' ',1) as Cohort,
split_part(cohort,' ',2)|| ' ' ||split_part(cohort,' ',3) as treatment_arm,
'Escalation' as "study phase",
'SOLID TUMORS' as "tumor type",
null as "documented ras status",
null as "liver metastasis",
null as region,
case when status = 'Screened' then 'Screening'
when status = 'Screen Failed' then 'Screen Failure'
else status
end as status,
"last visit" as "last visit",
"last visit date" as "last visit date",
"next expected visit",
"next expected visit date" as "next expected visit date",
partition_date as load_date,
Null as dose_level_treatment_arm_1,
Null as dose_level_treatment_arm_2,
split_part(cohort,' ',2)|| ' ' ||split_part(cohort,' ',3) as dose_level_treatment_arm,
md5(study_id||"study site"||investigator||country||subject) as hash_key,
last_modified_date as IRT_Data_Refreshed_Date
FROM    
{{ source ('koios_raw', 'subject_summary_report_xl495_101') }}    
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_summary_report_xl495_101') }})    
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_summary_report_xl495_101') }} )
union all
select
study_id
-- ,'0'||"study site" as studysite
,cast("study site" as varchar) as studysite
,investigator
,country
,cast(subject as varchar) as subject
-- ,'0'||substring(subject,1,3)||'-'||'0'||substring(subject,4,6) as subject
,cohort as cohort_desc
,split_part(cohort,':',1)  as cohort
,"treatment arm" as "treatment arm" 
--,split_part(cohort,':',2) as treatment_arm
,case when "study stage" = 'Escalation Stage' then 'Escalation' 
else "study stage"
end as "study phase"
,"tumor type"
,Null as "documented ras status"
,Null as "liver metastasis"
,Null as region
,case when status = 'Screened' then 'Screening'
when status = 'Screen Failed' then 'Screen Failure'
else status
end as status
,"last visit" as "last visit"
,"last visit date" as "last visit date"
,"next expected visit"
,"next expected visit date" as "next expected visit date"
,partition_date as load_date
-- ,'XL309_' || upper("xl309 current dose") ||  coalesce((' + Olaparib_' || upper("olaparib current dose")), '' )as dose_level_treatment_arm_1
,split_part(cohort,':',2) as dose_level_treatment_arm_1
,Null as dose_level_treatment_arm_2
-- ,'XL309_' || upper("xl309 current dose") ||  coalesce((' + Olaparib_' || upper("olaparib current dose")), '' ) as dose_level_treatment_arm
,split_part(cohort,':',2) as dose_level_treatment_arm,md5(study_id||"study site"||investigator||country||subject) as hash_key
,last_modified_date as IRT_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'subject_summary_report_xl309-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_summary_report_xl309-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_summary_report_xl309-101') }})
union all

select
study_id
,cast("study site" as varchar) as studysite
,investigator
,country
,cast(subject as varchar) as subject
,cohort as cohort_desc
,split_part(cohort,':',1)  as cohort
,split_part(cohort,':',2)  as "treatment arm" 
,case when stage = 'Escalation Stage' then 'Escalation' 
else stage
end as "study phase"
,case 
    when stage is not null then 'Advanced Solid Tumor' 
    else null 
end as "tumor type"
,Null as "documented ras status"
,Null as "liver metastasis"
,Null as region
,case when status = 'Screened' then 'Screening'
when status = 'Screen Failed' then 'Screen Failure'
else status
end as status
,"last visit" as "last visit"
,"last visit date" as "last visit date"
,"next expected visit"
,"next expected visit date" as "next expected visit date"
,partition_date as load_date
,split_part(cohort,' ',4) as dose_level_treatment_arm_1
,Null as dose_level_treatment_arm_2
,"current dose" as dose_level_treatment_arm
,md5(study_id||"study site"||investigator||country||subject) as hash_key
,last_modified_date as IRT_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'subject_summary_report_xb628-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_summary_report_xb628-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_summary_report_xb628-101') }})

union all

select
  study_id,
  cast("study site" as varchar) as studysite,
  investigator,
  country,
  cast(subject as varchar) as subject,
  "tumor grade" as cohort_desc,
  "tumor site" as cohort,
  Null as "treatment arm",
  null as "study phase",
  "tumor site" as "tumor type",
  Null as "documented ras status",
  Null as "liver metastasis",
  Null as region,
  case 
    when status = 'Screened' then 'Screening'
    when status = 'Screen Failed' then 'Screen Failure'
    when status = 'Crossover Discontinued' then 'Off study'
    when status = 'Crossover' then 'On Treatment'
    else status
  end as status,
  "last visit" as "last visit",
  "last visit date" as "last visit date",
  "next expected visit",
  "next expected visit date" as "next expected visit date",
  partition_date as load_date,
  Null as dose_level_treatment_arm_1,
  Null as dose_level_treatment_arm_2,
  Null as dose_level_treatment_arm,
  md5(study_id||"study site"||investigator||country||subject) as hash_key,
  last_modified_date as IRT_Data_Refreshed_Date
from {{ source ('koios_raw', 'subject_summary_report_xl092-311') }}
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_summary_report_xl092-311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_summary_report_xl092-311') }})

union all

select
study_id,
cast ("study site" as varchar ) as studysite,
investigator,
country,
cast(subject as varchar) as subject,
cohort as cohort_desc,
split_part(cohort,':',1) as cohort,
split_part(cohort,':',2) as treatment_arm,
case when "phase" is null then 'Escalation'
else "phase" end as "study phase",
'Advanced Solid Tumor' as "tumor type",
null as "documented ras status",
null as "liver metastasis",
null as region,
case when status = 'Screened' then 'Screening'
when status = 'Screen Failed' then 'Screen Failure'
else status
end as status,
"last visit" as "last visit",
"last visit date" as "last visit date",
"next expected visit",
"next expected visit date" as "next expected visit date",
partition_date as load_date,
Null as dose_level_treatment_arm_1,
Null as dose_level_treatment_arm_2,
'XB371_' || upper(split_part(cohort, ':',2)) as dose_level_treatment_arm,
md5(study_id||"study site"||investigator||country||subject) as hash_key,
last_modified_date as IRT_Data_Refreshed_Date
FROM    
{{ source ('koios_raw', 'subject_summary_report_xb371-101') }}    
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_summary_report_xb371-101') }})    
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_summary_report_xb371-101') }} )

union all
select
study_id,
cast ("study site" as varchar ) as studysite,
"investigator name" as investigator,
country,
cast("participant id" as varchar) as subject,
null as cohort_desc,
null as cohort,
null as treatment_arm,
null as "study phase",
null as "tumor type",
null as "documented ras status",
null as "liver metastasis",
null as region,
case when "participant status" = 'Screened' then 'Screening'
when "participant status"  = 'Screen Failed' then 'Screen Failure'
else "participant status" 
end as status,
"latest scheduled visit" as "last visit",
"latest scheduled visit date" as "last visit date",
"next expected visit",
"next expected visit date" as "next expected visit date",
partition_date as load_date,
Null as dose_level_treatment_arm_1,
Null as dose_level_treatment_arm_2,
null as dose_level_treatment_arm,
md5(study_id||"study site"||"investigator name"||country||"participant id") as hash_key,
last_modified_date as IRT_Data_Refreshed_Date
FROM    
{{ source ('koios_raw', 'subject_summary_report_xl092-201') }}    
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_summary_report_xl092-201') }})    
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_summary_report_xl092-201') }} )


union all
select
study_id,
cast ("study site" as varchar ) as studysite,
"investigator name" as investigator,
country,
cast("participant id" as varchar) as subject,
case
    when lower(trim("cohort description")) = '40mg adult equivalent' then concat('Cohort ','1',':',' ',"cohort description")
    when lower(trim("cohort description")) = '20mg adult equivalent' then concat('Cohort ','''-1',':',' ',"cohort description")
    when lower(trim("cohort description")) = '60mg adult equivalent' then concat('Cohort ','2',':',' ',"cohort description")
    when lower(trim("cohort description")) = '100mg adult equivalent' then concat('Cohort ','3',':',' ',"cohort description")
    else "cohort description"
end as cohort_desc,
case
    when lower(trim("cohort description")) = '40mg adult equivalent' then concat('Cohort ','1' )
    when lower(trim("cohort description")) = '20mg adult equivalent' then concat('Cohort ','''-1' )
    when lower(trim("cohort description")) = '60mg adult equivalent' then concat('Cohort ','2')
    when lower(trim("cohort description")) = '100mg adult equivalent' then concat('Cohort ','3' )
    else null
end as cohort,
split_part("cohort description",' ',1) as treatment_arm,
'Escalation' as "study phase",
'Solid Tumor' as "tumor type",
null as "documented ras status",
null as "liver metastasis",
null as region,
case when "participant status" = 'Screened' then 'Screening'
when "participant status"  = 'Screen Failed' then 'Screen Failure'
else "participant status" 
end as status,
"latest scheduled visit" as "last visit",
"latest scheduled visit date [local]" as "last visit date",
"next expected visit",
"next expected visit date" as "next expected visit date",
partition_date as load_date,
Null as dose_level_treatment_arm_1,
Null as dose_level_treatment_arm_2,
'XL092_' || upper(split_part("cohort description", ' ',1)) as dose_level_treatment_arm,
md5(study_id||"study site"||"investigator name"||country||"participant id") as hash_key,
last_modified_date as IRT_Data_Refreshed_Date
FROM    
{{ source ('koios_raw', 'participant_summary_report_xl092-011') }}    
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'participant_summary_report_xl092-011') }})    
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'participant_summary_report_xl092-011') }} )

)
select * from subject_summary_report
