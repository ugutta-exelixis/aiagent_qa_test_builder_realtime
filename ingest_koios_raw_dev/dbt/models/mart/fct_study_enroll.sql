with enroll_forecast as(
    select distinct
	study_protocol_number as study_id_ef,   
    md5(study_protocol_number) as study_id_sk_ef,
    'ef' as src_ef,
	country_name as country_name_ef,
	to_date(month_end_date,'DD-MON_YYY')as month_end_date_ef,
	SUM(cast(planned_subjects as integer)) as forecasted_participants,
    md5(study_protocol_number||CAST(month_end_date AS DATE)||country_name) as enroll_forecast_sk
from {{ref ('ctms_stg__enroll_forecast_hist')}}
group by study_id_ef,study_id_sk_ef,country_name,month_end_date
),
srm_enroll_proj as(
    select distinct 
    study_protocol_number as study_id_se,
    md5(study_protocol_number) as study_id_sk_se,
    'se' as src_se,
    country_name as country_name_se,
    SUM(planned_subjects) as projected_participants,
    cast(month_end_date as date) as month_end_date_se,
    md5(study_protocol_number||CAST(month_end_date AS DATE)||country_name) as srm_enroll_proj_sk
    from {{ref ('ctms_stg__srm_enroll_proj_hist')}}
    group by study_id_se,country_name,cast(month_end_date as date)
   ),
actual_participant_enrollment as(
select sv.study_id as study_id_ae,
md5(sv.study_id) as study_id_sk_ae,
'ae' as src_ae,
sv.country as country_name_ae,
(date_trunc('month', to_date(sv."actual date",'DD-MON-YY')) + interval '1 month - 1 day')::date as month_end_date_ae,
count(distinct RIGHT(sv.subject,4)) as actual_enroll_participants,
md5(sv.study_id||(date_trunc('month', to_date(sv."actual date",'DD-MON-YY')) + interval '1 month - 1 day')::date||sv.country) as actual_participant_enrollment_sk
from {{ref('ctms_stg__subject_visit_summary_report')}} sv
inner join {{ref('ctms_stg__subject_summary')}} ss 
on RIGHT(ss."subject number",4) = RIGHT(sv.subject,4)
and ss.studyid = sv.study_id
and sv.study_id <> 'XB002-101'
and sv."visit description" = 'Randomization'
and ss."subject status" in ('Off Study', 'Randomized', 'On Treatment', 'In Follow-up') 
group by sv.study_id,sv.country, month_end_date_ae

union

select ev.study_id as study_id_ae, 
md5(ev.study_id) as study_id_sk_ae,
'ae' as src_ae,
cs.country as country_name_ae,
date_trunc('MONTH', ev.date_of_visit_scheduled) + INTERVAL '1 month'-INTERVAL '1 day' AS month_end_date_ae,
count(distinct right(ev.participant_number,4)) as actual_enroll_participants,
md5(ev.study_id||date_trunc('MONTH', ev.date_of_visit_scheduled) + INTERVAL '1 month'-INTERVAL '1 day'||cs.country) as actual_participant_enrollment_sk
--min(ev.date_of_visit_scheduled) as actual_date
from {{ref('ctms_stg__early_phase_subject_visit')}} ev,
{{ref('ctms_stg__early_phase_subject_status')}} es,
{{ref('ctms_stg__subject_summary')}} cs
where right(ev.participant_number,4) = right(es.subject,4)
and ev.study_id = es.studyid
and ev.study_id = 'XB002-101'
and right(es.subject,4) = right(cs."subject number",4)
and ev.study_id = cs.studyid
--and participant_id = '1008'
and ev.visit_description = 'Day 1'
and es.participant_status in ('Enrolled')
group by ev.study_id, cs.country,month_end_date_ae

),
screen_failed_participants as(
	select ss.studyid as study_id_sf, 
    md5(ss.studyid) as study_id_sk_sf,
    'sf' as src_sf,
    ss.country as country_name_sf,
(date_trunc('month', to_date(sv."actual date",'DD-MON-YY')) + interval '1 month - 1 day')::date as month_end_date_sf,
count(distinct RIGHT(sv.subject,4)) as screen_failed_participants,
md5(studyid||(date_trunc('month', to_date(sv."actual date",'DD-MON-YY')) + interval '1 month - 1 day')::date||ss.country) as screen_failed_participants_sk
from {{ref('ctms_stg__subject_summary')}} ss
inner join {{ref('ctms_stg__subject_visit_summary_report')}} sv
on RIGHT(ss."subject number",4) = RIGHT(sv.subject,4)
and ss.studyid = sv.study_id
and sv."visit description" = 'Screen Failure'
and ss."subject status" in ('Screen Failure' , 'Screen Failed')
and RIGHT(sv.subject,4) is not null
group by studyid,ss.country, month_end_date_sf

),
/*
fct_study_enroll as (
    select distinct
    ef.study_id_sk,
    ef.country_name,
    ef.month_end_date,
    ef.forecasted_participants,
    ep.projected_participants,
    ape.actual_enroll_participants,
    sfp.screen_failed_participants
   from enroll_forecast as ef
    left outer join srm_enroll_proj as ep
    on ef.enroll_forecast_sk=ep.srm_enroll_proj_sk
    full outer join actual_participant_enrollment as ape
    on ef.enroll_forecast_sk=ape.actual_participant_enrollment_sk
    full outer join screen_failed_participants as sfp
    on ef.enroll_forecast_sk=sfp.screen_failed_participants_sk
    */


enroll_master as (
    select distinct enroll_forecast_sk as join_key from enroll_forecast
    union
    select distinct srm_enroll_proj_sk as join_key from srm_enroll_proj
    union
    select distinct actual_participant_enrollment_sk as join_key from actual_participant_enrollment
    union
    select distinct screen_failed_participants_sk as join_key from screen_failed_participants
),
study_enroll as (
    select * from enroll_master m
left join enroll_forecast ef
on m.join_key = ef.enroll_forecast_sk
left join srm_enroll_proj se
on m.join_key = se.srm_enroll_proj_sk
left join actual_participant_enrollment ae
on m.join_key = ae.actual_participant_enrollment_sk
left join screen_failed_participants sf
on m.join_key = sf.screen_failed_participants_sk
),

/*
fct_study_enroll as (
    select distinct
    case when src_ef = 'ef' then study_id_ef
    when src_se = 'se' then study_id_se
    when src_ae = 'ae' then study_id_ae
    else study_id_sf end as study_id,
    case when src_ef = 'ef' then study_id_sk_ef
    when src_se = 'se' then study_id_sk_se
    when src_ae = 'ae' then study_id_sk_ae
    else study_id_sk_sf end as study_id_sk,
    case when src_ef = 'ef' then country_name_ef
    when src_se = 'se' then country_name_se
    when src_ae = 'ae' then country_name_ae
    else country_name_sf end as country_name,
    case when src_ef = 'ef' then month_end_date_ef
    when src_se = 'se' then month_end_date_se
    when src_ae = 'ae' then month_end_date_ae
    else month_end_date_sf end as month_end_date,
    forecasted_participants,
    projected_participants,
    actual_enroll_participants,
    screen_failed_participants
    from study_enroll

)
*/
fct_study_enroll as (
    select distinct
    study_id_ef as study_id,
    study_id_sk_ef as study_id_sk,
    country_name_ef as country_name,
    month_end_date_ef as month_end_date,
    forecasted_participants,
    projected_participants,
    actual_enroll_participants,
    screen_failed_participants
    from study_enroll
    where src_ef = 'ef'
    union
    select distinct
    study_id_se as study_id,
    study_id_sk_se as study_id_sk,
    country_name_se as country_name,
    month_end_date_se as month_end_date,
    forecasted_participants,
    projected_participants,
    actual_enroll_participants,
    screen_failed_participants
    from study_enroll
    where src_se = 'se'
    union
    select distinct
    study_id_ae as study_id,
    study_id_sk_ae as study_id_sk,
    country_name_ae as country_name,
    month_end_date_ae as month_end_date,
    forecasted_participants,
    projected_participants,
    actual_enroll_participants,
    screen_failed_participants
    from study_enroll
    where src_ae = 'ae'
    union
    select distinct
    study_id_sf as study_id,
    study_id_sk_sf as study_id_sk,
    country_name_sf as country_name,
    month_end_date_sf as month_end_date,
    forecasted_participants,
    projected_participants,
    actual_enroll_participants,
    screen_failed_participants
    from study_enroll
    where src_sf = 'sf'
)

select * from fct_study_enroll