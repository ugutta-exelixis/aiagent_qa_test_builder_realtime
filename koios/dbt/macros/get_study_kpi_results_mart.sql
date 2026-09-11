{% macro get_study_kpi_results_mart(study_id) %}


{% if study_id in ['XB002-101'] %}
select study_id,
'All'  as status,
'Total Screened' as KPI_Name,
study_phase as study_phase,
count(distinct participant_number)
,current_date as load_date 
from {{ref('dim_participant')}}
where study_id ='{{ study_id }}'
and participant_number is not null
and participant_number not in ('-','--','`---','VOID','OID.','1262','Void','OID1','OID2','0000')
and participant_status_master !='In-Active'
group by study_id,study_phase


{% else %}
select study_id,
'All'  as status,
'Total Screened' as KPI_Name,
study_phase as study_phase,
count(distinct participant_number)
,current_date as load_date 
from {{ref('dim_participant')}}
where study_id ='{{ study_id }}'
and participant_number is not null
and participant_status_irt is not null
and participant_number not in ('-','--','`---','VOID','OID.','1262','Void','OID1','OID2','0000')
and participant_status_master !='In-Active'
group by study_id,study_phase
{% endif %}
union all
 
 
{% if study_id in ['XB010-101','XB628-101','XB371-101'] %}
select
    dp.study_id,
    'Screening' as status,
    'In Screening in Last 30 Days' as kpi_name,
    dp.study_phase,
    count(distinct dp.participant_id_sk) as participant_count,
    current_date as load_date
from ( 
    select distinct
        fp.participant_id_sk
    from {{ ref('fct_participant_visit') }} fp
    where fp.visit_description in ('Screening', 'In Screening')
      and fp.study_id_sk = md5('{{ study_id }}')
      and (current_date - cast(fp.date_of_visit_actual as date)) < 31
) screening_participants 
join {{ ref('dim_participant') }} dp
  on screening_participants.participant_id_sk = dp.participant_id_sk 
 and dp.study_id_sk = md5('{{ study_id }}')
group by
    dp.study_id,
    dp.study_phase

{% elif study_id in ['XL092-002'] %}

select fp.study_id ,
    fp.visit_description,
    'In Screening in Last 30 Days' as KPI_Name,
    'Expansion' as study_phase,
    count(distinct dp.participant_id_sk)
    ,current_date as load_date 
    from {{ref('fct_participant_visit')}} fp
    left join {{ref('dim_participant')}} dp on fp.participant_id_sk = dp.participant_id_sk  
    where fp.visit_description in ('Screening','In Screening') 
    and fp.study_id_sk  =md5('{{ study_id }}')
    and (current_date  - cast(fp.date_of_visit_actual as date))<31
    group by fp.study_id ,fp.visit_description
 
 
{% else %}
    select fp.study_id ,
    fp.visit_description,
    'In Screening in Last 30 Days' as KPI_Name,
    study_phase as study_phase,
    count(distinct dp.participant_id_sk)
    ,current_date as load_date 
    from {{ref('fct_participant_visit')}} fp
    left join {{ref('dim_participant')}} dp on fp.participant_id_sk = dp.participant_id_sk  
    where fp.visit_description in ('Screening','In Screening') 
    and fp.study_id_sk  =md5('{{ study_id }}')
    and (current_date  - cast(fp.date_of_visit_actual as date))<31
    group by study_phase, fp.study_id ,fp.visit_description
{%endif%}


union all


{% if study_id in [ 'XB628-101','XL309-101'] %}
select fp.study_id,
fp.visit_description as status,
'Enrolled in last 30 days' as KPI_Name,
'Escalation' as study_phase,
count(distinct fp.participant_id_sk)
,current_date as load_date 
from {{ref('fct_participant_visit')}} fp
left join {{ref('dim_participant')}} dp 
on fp.participant_id_sk = dp.participant_id_sk  
left join {{ref('fct_study_country')}} fsc 
on fsc.study_country_id_sk = fp.study_country_id_sk
and fsc.study_id_sk = fp.study_id_sk
where fp.visit_description = 'Randomization'
and fp.study_id_sk  = md5('{{ study_id }}')
and (current_date  - cast(fp.date_of_visit_actual as date))<31
and study_phase in ('Escalation')
group by fp.study_id,study_phase,fp.visit_description
{% else %}
select fp.study_id,
fp.visit_description as status,
'Enrolled in Last 30 days' as KPI_Name,
dp.study_phase as  study_phase,
count(distinct fp.participant_id_sk)
,current_date as load_date 
from {{ref('fct_participant_visit')}} fp
left join {{ref('dim_participant')}} dp 
on fp.participant_id_sk = dp.participant_id_sk  
and dp.study_id_sk = fp.study_id_sk
where fp.visit_description = 'Randomization'
and fp.study_id_sk  = md5('{{ study_id }}')
and (current_date  - cast(fp.date_of_visit_actual as date))<31
group by fp.study_id,study_phase,fp.visit_description
{% endif %}
union all
select study_id,
country_status,
'Country Activation Status Forecast' as KPI_Name,
null as study_phase,
count(*) as count
,current_date as load_date 
from {{ref('dim_study_country')}}
where study_id = '{{study_id}}' and country_status in ('Active','Planned','Approved')
group by country_status, study_id
 
union all
select study_id,
participant_status_master as status,
'Participant Status' as KPI_Name,
study_phase,
count(*)
,current_date as load_date 
from {{ref('dim_participant')}}
where study_id = '{{study_id}}'
and participant_status_master not in ('Screen Failure Rollback Requested','In-Active')
group by study_id,study_phase,participant_status_master
union all

select study_id,
null as status,
'Missing pages' as KPI_Name,
Null as study_phase,
sum(missing_pgs) as count
,current_date as load_date 
from {{ref('dim_participant')}} dp 
where study_id='{{study_id}}'
and study_id not in ('XL092-311','XL309-101','XB628-101','XB371-101','XL092-201')
group by study_id 
union all
select studyid,
query_status as status,
'Open queries' as KPI_Name,
null as study_phase,
count(distinct query_id_sk)
,current_date as load_date 
from {{ref('dim_queries')}}
where query_status ='Opened'
and studyid = '{{study_id}}'
group by query_status, studyid
union all
select studyid,
query_status as status,
'Queries closed' as KPI_Name,
null as study_phase,
count(distinct query_id_sk)
,current_date as load_date 
from {{ref('dim_queries')}}
where query_status ='Closed'
and studyid = '{{study_id}}'
group by query_status, studyid
union all
select '{{study_id}}' as study_id,
query_status,
'Open in 30-60 days' as KPI_Name,
Null as study_phase,
count(*)
,current_date as load_date 
from {{ref('fct_queries')}}
where query_status = 'Opened'
and study_id_sk = md5('{{study_id}}')
and ( current_date  - cast(opened_date_id as date))>31
and ( current_date  - cast(opened_date_id as date))<=61
group by query_status
union all
select '{{study_id}}' as study_id,
query_status,
'Open in 60-90 days' as KPI_Name,
Null as study_phase,
count(*) 
,current_date as load_date  
from koios_mart.fct_queries
where query_status = 'Opened'
and study_id_sk = md5('{{study_id}}')
and ( current_date  - cast(opened_date_id as date))>61
and ( current_date  - cast(opened_date_id as date))<=91
group by query_status
union all
select '{{study_id}}' as study_id,
query_status,
'Open in 90 days' as KPI_Name,
Null as study_phase,
count(*)
,current_date as load_date 
from koios_mart.fct_queries
where query_status = 'Opened'
and study_id_sk = md5('{{study_id}}')
and ( current_date  - cast(opened_date_id as date))>91
group by query_status
union all
select study_id,
'Total Discontinued' as status,
'Cohort Summary Report' as KPI_Name,
Null as study_phase,
count(distinct participant_id_sk)
,current_date as load_date 
from {{ref('dim_participant')}}
where study_id ='{{study_id}}'
and participant_status_irt in ('Discontinued')
and cohort_status is not null
group by study_id,participant_status_irt
union all
select study_id,
'Total Enrolled' as status,
'Cohort Summary Report' as KPI_Name,
null as study_phase,
count(distinct participant_id_sk)
,current_date as load_date 
from {{ref('dim_participant')}}
where study_id ='{{study_id}}'
and participant_status_master in ('Randomized','In Follow-up','Off Study','On Treatment')
and participant_status_irt in ('Enrolled','Discontinued')
and cohort_status is not null
group by study_id
union all

{%  if study_id in ['XL309-101','XL495-101','XB010-101','XB628-101','XB371-101','XL092-201'] %}

select study_id,
'Screen Failed' as status,
'Cohort Summary Report' as KPI_Name,
null as study_phase,
count(*)
,current_date as load_date 
from {{ref('dim_participant')}}
where study_id ='{{study_id}}'
and participant_status_irt in('Screen Failure' ,'Screen Failed')
group by study_id

{% else %}
select study_id,
'Screen Failed' as status,
'Cohort Summary Report' as KPI_Name,
null as study_phase,
count(*)
,current_date as load_date 
from {{ref('dim_participant')}}
where study_id ='{{study_id}}'
and participant_status_irt in('Screen Failure' ,'Screen Failed')
and cohort_status is not null
group by study_id
{%endif%}
union all

select dp.study_id,
'In_Screenig' as status,
'Cohort Summary Report' as KPI_Name,
null as study_phase,
count(distinct dp.participant_id) 
,current_date as load_date 
from {{ref('dim_participant')}} dp
where dp.participant_status_master  in ('In Screening')
and participant_status_irt is not null
and cohort_status is not null
and dp.study_id ='{{study_id}}'
group by dp.study_id

union all
{%  if study_id in ['XL309-101','XL495-101','XB010-101','XB628-101','XB371-101','XL092-201'] %}
select study_id,
'Total Screened' as status,
'Cohort Summary Report' as KPI_Name,
null as study_phase,
count(distinct participant_number)
,current_date as load_date 
from {{ref('dim_participant')}}
where study_id ='{{study_id}}'
--and cohort_status is not null
and study_phase is not null
and participant_status_irt in ('Enrolled','Discontinued','Screen Failure','In Screening', 'Screening','Screened','Screen Failed')
group by study_id
{% else %}
select study_id,
'Total Screened' as status,
'Cohort Summary Report' as KPI_Name,
null as study_phase,
count(distinct participant_number)
,current_date as load_date 
from {{ref('dim_participant')}}
where study_id ='{{study_id}}'
and participant_status_irt in ('Enrolled','Discontinued','Screen Failure','In Screening', 'Screening','Screened','Screen Failed')
and cohort_status is not null
--and study_phase is not null
group by study_id
{% endif %}

union all
{% if study_id in ['XL309-101','XL495-101','XB010-101','XB628-101','XB371-101']%}
select study_id,
'Lookup' as status,
'Planned Enrollment' as KPI_Name,
study_phase,
count(distinct participant_id_sk)
,current_date as load_date 
from {{ref('dim_participant')}}
where study_id ='{{study_id}}'
and participant_status_master in ('Randomized','On Treatment','Off Study','In Follow-up')
group by study_phase, study_id
{% else %}
select study_id,
'Lookup' as status,
'Planned Enrollment' as kpi_name,
study_phase,
sum(forecasted) as count
,current_date as load_date 
from koios_mart.lkp_sites_enrollment_forecast 
where study_id='{{study_id}}' and milestone_name='Enrollment' 
and month_end_date in(select max(month_end_date) from koios_mart.lkp_sites_enrollment_forecast 
where milestone_name='Enrollment' and study_id='{{study_id}}') 
group by study_id, month_end_date,milestone_name,study_phase
{%endif%} 
union all

{% if study_id in ['XL309-101']%}
select study_id,
'Lookup' as status,
'Planned countries' as KPI_Name,
null as study_phase,
count(*) as count
,current_date as load_date 
from koios_mart.dim_study_country
where study_id = '{{study_id}}' and country_status in ('Active','Planned','Approved')
group by country_status, study_id
{% else %}
select study_id,
'Lookup' as status,
'Planned countries' as KPI_Name,
null as study_phase,
count(*) as count
,current_date as load_date 
from koios_mart.dim_study_country
where study_id = '{{study_id}}' and country_status in ('Active','Approved','Planned')
group by country_status, study_id
{%endif%} 

union all
select study_id,
'Lookup' as status,
'Planned sites' as kpi_name,
null as study_phase,
sum(forecasted) as count
,current_date as load_date 
from koios_mart.lkp_sites_enrollment_forecast 
where study_id='{{study_id}}' and milestone_name ='Site Activation' 
and month_end_date in(select max(month_end_date) from koios_mart.lkp_sites_enrollment_forecast 
where milestone_name ='Site Activation' and study_id='{{study_id}}')
group by study_id, month_end_date,milestone_name,study_phase

union all

select
    study_id,
    participant_status_master as status,
    '#Active on IP' as kpi_name,
    study_phase,
    count(distinct participant_id_sk) as count,
    current_date as load_date
from {{ ref('dim_participant') }}
where study_id = '{{ study_id }}'
  and participant_status_master = 'On Treatment'
group by study_id, study_phase,participant_status_master

union all

select
    study_id, 
     participant_status_master as status,
    '#Active on Study' as kpi_name,
    study_phase,
    count(distinct participant_id_sk) as count,
    current_date as load_date
from {{ ref('dim_participant') }}
where study_id = '{{ study_id }}'
  and participant_status_master in ('On Treatment','Randomized','In Follow-up')
group by study_id, study_phase,participant_status_master

union all
select 
study_id
,null as  status
,'Enrollment Progress Actual' as kpi_name
,study_phase as study_phase
,count(*) as count
,current_date as load_date
from {{ ref('dim_participant') }}
where study_id = '{{ study_id }}'
and participant_status_master in ('Randomized','On Treatment','Off Study','In Follow-up')
group by study_id,study_phase

union all

select study_id,
null as status,
'Site Activation Status Actual' as kpi_name,
null as study_phase,
count(*) as count,
current_date as load_date
from {{ ref('dim_study_site') }}
where study_id = '{{ study_id }}'
and study_site_status in ('Activated', 'Active Enrolling', 'Enrollment Open','Enrollment Closed','Closed','Close Out Ready', 'Enrollment On-Hold','Site Closed') 
and site_type = 'Main' 
and site_number not in ('TRIO')
group by study_id,study_phase

union all

{% if study_id in ['XL092-305'] %}
select study_id
,'Active' as status
,'Country Activation Status Actual' as kpi_name
,Null as study_phase
,count(*) as count
,current_date as load_date
from {{ ref('dim_study_country') }}
where study_id = '{{ study_id }}'
and country_status in ('Active','Enrollment Closed')
group by study_id 
{% else %}
select study_id
,country_status as status
,'Country Activation Status Actual' as kpi_name
,Null as study_phase
,count(*) as count
,current_date as load_date
from {{ ref('dim_study_country') }}
where study_id = '{{ study_id }}'
and country_status in ('Active','Enrollment Closed')
group by study_id, country_status
{% endif %}

union all
select study_id
,status 
,studysite as kpi_name
,Null as study_phase
,count(distinct subject) as count
,current_date as load_date 
from {{ source('koios_staging', 'ctms_stg__subject_summary_report') }}
where studysite='9999'      --'9999' is dummy site
and study_id='{{ study_id }}'
group by study_id,studysite,status
{% endmacro %}