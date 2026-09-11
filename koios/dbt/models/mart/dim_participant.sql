with study_milestone as(
    select distinct
    md5(study_protocol_number || parent_id ) as study_milestone_sk,
    MAX(milestone_date) as enrollment_date,
    parent_id,
    study_protocol_number as study_id
    from {{ref ('ctms_stg__study_milestone')}}
    where parent_dataset = 'SUBJECT'
    group by study_id,parent_id
),
study_phase as(
select
study_id, right(subject,4) as participant_id, status as participant_status,
case when status = 'Screen Failure' and "tumor type" = 'Solid Tumor' then 'Escalation'
when status = 'In Screening' and "tumor type" = 'Solid Tumor' then 'Escalation'
when status in ('In Screening', 'Screening') and "tumor type" <> 'Solid Tumor' then 'Expansion'
when status like 'Screen Failure%' and "tumor type" <> 'Solid Tumor' then 'Expansion'
else "study phase" end as study_phase
from {{ref ('ctms_stg__subject_summary_report')}}
where study_id = 'XL092-002'
union
select studyid as study_id
,participant_id
,participant_status
,participant_type as study_phase
from {{ref ('ctms_stg__early_phase_subject_status')}}
where studyid = 'XL092-002'
and studyid || participant_id not in
(select study_id || right(subject,4) from {{ref ('ctms_stg__subject_summary_report')}}
where study_id = 'XL092-002')
union
select studyid as study_id
,participant_id
,participant_status
,participant_type as study_phase
from {{ref ('ctms_stg__early_phase_subject_status')}}
where studyid = 'XB002-101'
union
select distinct studyid as study_id
,right("subject number",4) as participant_id
,"subject status" as participant_status
,"study stage" as study_phase
from {{ref ('ctms_stg__subject_summary')}}
where studyid = 'XB002-101'
and studyid||right("subject number",4) not in
(select distinct studyid||participant_id from {{ref ('ctms_stg__early_phase_subject_status')}})
union 
select distinct study_id
,right("subject",4) as participant_id
,status as participant_status
,case 
	when "tumor type" in ('Advanced Solid Tumors','1L RCC') and cohort in ('Cohort A','Cohort B') then 'Escalation'
	when "tumor type" in ('2L+RCC','1L RCC') and cohort in ('Cohort 1','Cohort 2') then 'Expansion'
    when status in ('Screening', 'Screen Failure') and "tumor type" in ('Advanced Solid Tumors') then 'Escalation'
	else "study phase"
end as study_phase
from {{ref ('ctms_stg__subject_summary_report')}}
where study_id = 'XL092-009'
union 
select distinct studyid as study_id
,right("subject number",4) as participant_id
,"subject status" as participant_status
,'Escalation' as study_phase
from {{ref ('ctms_stg__subject_summary')}}
where studyid = 'XL092-009'
and studyid||right("subject number",4) not in
(select distinct study_id||right("subject",4) from {{ref ('ctms_stg__subject_summary_report')}})
union 
select distinct study_id
,right("subject",4) as participant_id
,status as participant_status
,"study phase" as study_phase
from {{ref ('ctms_stg__subject_summary_report')}}
where study_id = 'XB010-101'
union
select distinct study_id
,right("subject",4) as participant_id
,status as participant_status
,"study phase" as study_phase
from {{ref ('ctms_stg__subject_summary_report')}}
where study_id = 'XL495-101'
union 
select
study_id, right(subject,6) as participant_id, status as participant_status,
case when status in ('Screen Failure','Screening') then coalesce("study phase",'Escalation')
else "study phase" 
end as study_phase
-- "study phase" as study_phase
from {{ref ('ctms_stg__subject_summary_report')}}
where study_id in ('XL309-101','XB628-101')
union
select
study_id, right(subject,4) as participant_id, status as participant_status,
"study phase" as study_phase
from {{ref ('ctms_stg__subject_summary_report')}}
where study_id in ('XB371-101','XL092-011')
),
cohort as (
    select
        study_id,
        right(subject,4) as participant_id,
        cohort_desc,
        cohort,
        treatment_arm,
        dose_level_treatment_arm,
        "tumor type",
        status  
    from {{ref ('ctms_stg__subject_summary_report')}}
    where study_id in ('XB002-101','XL092-002','XL092-009', 'XL092-311','XB371-101','XL092-011')
    -- and status not in ('Screen Failure', 'Screening')
    union
    select
        studyid as study_id,
        right("subject number",4) as participant_id,
        case when cohort_desc like 'COHORT A%%: SOLID TUMORS' then 'COHORT A: SOLID TUMORS' else cohort_desc end as cohort_desc,
        case when cohort like 'COHORT A%%' then 'COHORT A' else cohort end as cohort,
        treatment_arm,
        dose_level_treatment_arm,
        "tumor_type" as "tumor type",
        "subject status" as status  
    from  {{ref ('ctms_stg__subject_summary')}}
    where studyid = 'XB002-101'
    and "subject status" in ('Off Study', 'Randomized', 'On Treatment', 'In Follow-up')
    and studyid||right("subject number",4) not in
    (select study_id||right(subject,4) from {{ref ('ctms_stg__subject_summary_report')}})
    union
    select
        study_id,
        right(subject,4) as participant_id,
        case when "liver metastasis" = 'Yes'then 'liver_metastasis'
        when "liver metastasis" = 'No'then 'Non_liver_metastasis'
        else "liver metastasis" end as cohort_desc,
        case when "liver metastasis" = 'Yes'then 'liver_metastasis'
        when "liver metastasis" = 'No'then 'Non_liver_metastasis'
        else "liver metastasis" end as cohort,
        treatment_arm,
        dose_level_treatment_arm,
        "tumor type" ,
        status
    from {{ref ('ctms_stg__subject_summary_report')}}
    where study_id ='XL092-303'
    and status not in ('Screening', 'Screen Failure')
    union 
    select
        studyid as study_id,
        right("subject number",4) as participant_id,
        cohort_desc,
        cohort,
        treatment_arm,
        dose_level_treatment_arm,
        "tumor_type" as "tumor type",
        "subject status" as status  
    from  {{ref ('ctms_stg__subject_summary')}}
    where studyid = 'XL092-009'
    and "subject status" in ('Off Study', 'Randomized', 'On Treatment', 'In Follow-up')
    and studyid||right("subject number",4) not in
    (select study_id||right(subject,4) from {{ref ('ctms_stg__subject_summary_report')}})
    union
    select
        sr.study_id,
        right(sr.subject,4) as participant_id,
        sr.cohort_desc,
        sr.cohort,
        sr.treatment_arm,
        sr.dose_level_treatment_arm,
        sr."tumor type",
        sr.status  
    from {{ref ('ctms_stg__subject_summary_report')}} sr
    left join {{ref ('ctms_stg__subject_summary')}} ss
    on sr.study_id = ss.studyid 
    and right(sr.subject,4) = right(ss."subject number",4) 
    where study_id in ('XB010-101')
    union
    select
        sr.study_id,
        right(sr.subject,4) as participant_id,
        sr.cohort_desc,
        sr.cohort,
        sr.treatment_arm,
        sr.dose_level_treatment_arm,
        ss."tumor_type",
        sr.status  
    from {{ref ('ctms_stg__subject_summary_report')}} sr
    left join {{ref ('ctms_stg__subject_summary')}} ss
    on sr.study_id = ss.studyid 
    and right(sr.subject,4) = right(ss."subject number",4) 
    where study_id in ('XL495-101')
    union
    select
        study_id,
        right(subject,6) as participant_id,
        cohort_desc,
        cohort,
        treatment_arm,
        dose_level_treatment_arm,
        "tumor type",
        status  
    from {{ref ('ctms_stg__subject_summary_report')}}
    where study_id in ('XL309-101','XB628-101')
),
study_subject as(
    select distinct
    RIGHT(subject_number,4) AS participant_id_pra,
    study_site_id as site_number_pra,
    md5(study_protocol_number) as study_id_sk_pra,
    md5(RIGHT(subject_number,4))as participant_id_sk_pra,
    md5(study_protocol_number || subject_id ) AS study_subject_sk_pra,
    md5(study_protocol_number || RIGHT(subject_number,4)) AS study_subject_sk2_pra,
    study_protocol_number as study_id_pra,
    'pra' as source_pra,
    RIGHT(subject_number,4) as participant_number_pra,
    randomization_id,
    enrollment_id,
    subject_status as participant_status_pra,
    subject_status_reason as participant_status_reason_pra,
    to_date(load_date, 'YYYY-MM-DD') as load_date
    from {{ref ('ctms_stg__study_subject')}}
),
dim_participant_pra as(
select distinct
    ss.participant_id_pra,
    ss.site_number_pra,
    ss.study_id_sk_pra,
    ss.participant_id_sk_pra,
    ss.study_subject_sk_pra,
    ss.study_subject_sk2_pra,
    ss.study_id_pra,
    ss.participant_number_pra,
    ss.randomization_id,
    ss.enrollment_id,
    ss.participant_status_pra,
    ss.participant_status_reason_pra,
    to_date(sm.enrollment_date,'DD-MON-YYYY') as enrollment_date,
    ss.source_pra,
    ss.load_date as load_date_pra
    from study_subject ss
    left join study_milestone sm
    on sm.study_milestone_sk = ss.study_subject_sk_pra
   ),
subject_summary as(
    select distinct
    RIGHT("subject number",4) AS participant_id_edc,
    left(site,4) as site_number_edc,
    RIGHT("subject number",4) as participant_number_edc,
    md5(RIGHT("subject number",4))as participant_id_sk_edc,
    md5((studyid || RIGHT("subject number",4) )) as study_subject_sk_edc,
    "subject number" as subject_number_edc,
    md5(studyid || RIGHT("subject number",4)) as subject_summary_sk_edc,
    race,
    ethnic,
    'edc' as source_edc,
    studyid as study_id_edc,
    md5(studyid) as study_id_sk_edc,
    "subject status" as participant_status_edc,
    RIGHT("subject number",4) AS subject_number,
    load_date as load_date_edc
    from {{ref ('ctms_stg__subject_summary')}}
),
subject_summary_report as(
    select distinct
    case when study_id in ('XL309-101') THEN RIGHT(subject,6)
    ELSE RIGHT(subject,4) END AS participant_id_irt,
    studysite as site_number_irt,
    case when study_id in ('XL309-101') THEN RIGHT(subject,6)
    ELSE RIGHT(subject,4) END as participant_number_irt,
    case when study_id in ('XL309-101') THEN MD5(RIGHT(subject, 6))
    ELSE MD5(RIGHT(subject, 4)) 
    END as participant_id_sk_irt,
    case when study_id in ('XL309-101') THEN md5(study_id || RIGHT(subject,6))
    ELSE md5(study_id || RIGHT(subject,4))
    END  as subject_summary_report_sk_irt,
    subject as subject_number_irt,
    md5(study_id || RIGHT(subject,4)) as study_subject_sk_irt,
    'irt' as source_irt,
    md5(study_id) as study_id_sk_irt,
    study_id as study_id_irt,
    status as participant_status_irt,
    case when study_id in ('XL309-101') THEN MD5(RIGHT(subject, 6))
    ELSE MD5(RIGHT(subject, 4)) 
    END   AS subject,
    load_date as load_date_irt
    from {{ref ('ctms_stg__subject_summary_report')}}
),
participant_master as (
    select study_subject_sk_pra as study_subject_sk
    from dim_participant_pra
    union
    select subject_summary_sk_edc as study_subject_sk
    from subject_summary
    union
    select subject_summary_report_sk_irt as study_subject_sk
    from subject_summary_report
    ),
dim_participant_full as (
    select distinct
     * from participant_master pm
    left join subject_summary ss
     on pm.study_subject_sk = ss.subject_summary_sk_edc
     left join dim_participant_pra dp
     on pm.study_subject_sk = dp.study_subject_sk_pra
     left join subject_summary_report ssr
     on pm.study_subject_sk = ssr.subject_summary_report_sk_irt
),
dim_participant as (
select participant_id_sk_edc as participant_id_sk
,site_number_edc as site_number
,study_subject_sk_edc as study_subject_sk  
,participant_id_edc  as participant_id
,participant_number_edc  as participant_number
--,subject_number_edc    as subject_number
,subject_summary_sk_edc as subject_summary_sk
,race  
,ethnic
,enrollment_date
,study_id_edc as study_id  
,study_id_sk_edc as study_id_sk
,participant_status_pra
,participant_status_edc as participant_status_edc
,participant_status_irt
,participant_status_reason_pra as participant_status_reason
--,subject_number  
,cast(load_date_edc as date) as load_date
from dim_participant_full
where source_edc = 'edc'
--and study_id_edc = 'XL092-303'
union all
select participant_id_sk_pra as participant_id_sk
,site_number_pra as site_number
,study_subject_sk2_pra as study_subject_sk  
,participant_id_pra  as participant_id
,RIGHT(participant_number_pra,4) as participant_number
--,subject_number_pra    as subject_number
,study_subject_sk   as subject_summary_sk
,race  
, ethnic    
,enrollment_date
,study_id_pra as study_id  
,study_id_sk_pra as study_id_sk
,participant_status_pra as participant_status_pra
,participant_status_edc
,participant_status_irt
,participant_status_reason_pra as participant_status_reason
--,subject_number_irt as subject_number
,cast(load_date_pra as date) as load_date
from dim_participant_full
where source_pra = 'pra'
--and study_id_pra = 'XL092-303'
union all
select participant_id_sk_irt as participant_id_sk
,site_number_irt as site_number
,study_subject_sk_irt as study_subject_sk  
,participant_id_irt  as participant_id
,participant_number_irt  as participant_number
--,subject_number_irt    as subject_number
,subject_summary_report_sk_irt  as subject_summary_sk
,race  
,ethnic
,enrollment_date
,study_id_irt as study_id  
,study_id_sk_irt as study_id_sk
,participant_status_pra
,participant_status_edc
,participant_status_irt as participant_status_irt
,participant_status_reason_pra as participant_status_reason
--,subject_number  
,cast(load_date_irt as date) as load_date
from dim_participant_full
where source_irt = 'irt'
),
dim_participant_final as (
    select distinct dp.participant_id,dp.participant_id_sk,dp.study_subject_sk,dp.participant_number,dp.study_id,dp.study_id_sk
,max(race) as race
,max(ethnic) as ethnic
,max(enrollment_date) as enrollment_date
,max(participant_status_pra) as participant_status_pra
,max(participant_status_edc) as participant_status_edc
,max(participant_status_irt) as participant_status_irt
,max(participant_status_reason) as participant_status_reason
from dim_participant dp
where dp.participant_id not in (select participant_id
from dim_participant dpp
where 
--ssr.participant_id_irt = dp.participant_id
--and 
dpp.study_id = dp.study_id
and coalesce(site_number,'') = '9999')
group by participant_id,participant_id_sk,study_subject_sk,participant_number,study_id,study_id_sk
),
dim_participant_type as (
    select distinct dpf.participant_id
    ,dpf.participant_id_sk
    ,dpf.study_subject_sk
    ,dpf.participant_number
    ,dpf.study_id
    ,dpf.study_id_sk
    ,dpf.race
    ,dpf.ethnic
    ,dpf.enrollment_date
    ,dpf.participant_status_pra
    ,dpf.participant_status_edc
    ,dpf.participant_status_irt
    ,dpf.participant_status_reason
    ,sp.study_phase
    from dim_participant_final dpf
    left join study_phase sp
    on dpf.participant_id = sp.participant_id
    and dpf.study_id = sp.study_id
),
dim_participant_cohort as (
    select distinct dpf.participant_id
    ,dpf.participant_id_sk
    ,dpf.study_subject_sk
    ,dpf.participant_number
    ,dpf.study_id
    ,dpf.study_id_sk
    ,dpf.race
    ,dpf.ethnic
    ,dpf.enrollment_date
    ,dpf.participant_status_pra
    ,dpf.participant_status_edc
    ,dpf.participant_status_irt
    ,dpf.participant_status_reason
    ,dpf.study_phase
    ,case when dpf.study_id = 'XB002-101' and dpf.study_phase = 'Escalation' then 'SOLID TUMORS'
          when dpf.study_id = 'XL495-101' and dpf.study_phase = 'Escalation' then 'SOLID TUMORS' 
          else  c."tumor type" end as tumor_type
    ,c.cohort_desc
    ,case  when dpf.study_id = 'XB002-101' and dpf.study_phase = 'Escalation' then 'COHORT A' 
           when dpf.study_id = 'XL495-101' then 'Cohort ' || split_part(c.cohort, ' ' ,1)
           else c.cohort end as cohort
    ,c.treatment_arm
    ,c.dose_level_treatment_arm
    from dim_participant_type dpf
    left join cohort c
    on dpf.participant_id = c.participant_id
    and dpf.study_id = c.study_id
), 
dim_participant_cohort_map as (
	select distinct d.*,null as cohort_map  from dim_participant_cohort d
	where d.cohort is not null
	union
	select distinct d.*,null as cohort_map  from dim_participant_cohort d
	where d.cohort is null and study_id not in ('XL092-002','XB002-101','XL092-009','XL495-101')
	union
	select distinct d.*,c.cohort as cohort_map  from dim_participant_cohort d,
	{{ref ('ctms_stg__cohort_summary_mapping')}} c
	where  d.tumor_type = c."tumor type"
    and d.study_id = 'XB002-101'
	and d.study_id = c.study_id and d.study_phase = c."study phase"
	and d.cohort is null
    union
    select distinct d.*,c.cohort as cohort_map  from dim_participant_cohort d,
	{{ref ('ctms_stg__cohort_summary_mapping')}} c
	where  d.tumor_type = c."tumor type" 
    and d.study_id = 'XL092-002' and d.study_phase = 'Expansion'
	and d.study_id = c.study_id --and d.study_phase = c."study phase"
	and d.cohort is null
    union
	select distinct d.*,null as cohort_map  from dim_participant_cohort d
	where d.study_id = 'XL092-002' and d.study_phase = 'Escalation' or (d.study_phase is null and d.tumor_type = 'Solid Tumor')
	union
    select distinct d.*,null as cohort_map  from dim_participant_cohort d
	where d.study_id = 'XL495-101' and d.study_phase = 'Escalation'
    union
	select distinct d.*,c.cohort as cohort_map  from dim_participant_cohort d,
	{{ref ('ctms_stg__cohort_summary_mapping')}} c
	where  d.tumor_type = c."tumor type"
    and d.study_id in ('XB002-101')
	and d.study_id = c.study_id and d.study_phase is null
	and d.cohort is null and d.tumor_type <> 'Solid Tumor'
	union
	select distinct d.*,null as cohort_map  from dim_participant_cohort d
	where  d.study_id in ('XL092-002')
	and d.study_phase is null
	and d.cohort is null and d.tumor_type is null
    union
    select distinct d.*,c.cohort as cohort_map  from dim_participant_cohort d,
	{{ref ('ctms_stg__cohort_summary_mapping')}} c
	where  d.tumor_type = c."tumor type" 
    and d.study_id = 'XL092-009' and d.study_phase = 'Expansion'
	and d.study_id = c.study_id 
	and d.cohort is null
    union
	select distinct d.*,'Cohort A' as cohort_map  from dim_participant_cohort d
	where d.study_id in ('XL092-009')
	and d.cohort is null and d.tumor_type is null
	union 
	select distinct d.*,c.cohort as cohort_map  from dim_participant_cohort d,
	{{ref ('ctms_stg__cohort_summary_mapping')}} c
	where  d.tumor_type = c."tumor type"
    and d.study_id = 'XL092-009'
	and d.study_id = c.study_id and d.study_phase = c."study phase"
    and d.cohort is null
    union
    select distinct d.*,c.cohort as cohort_map  from dim_participant_cohort d,
    {{ref ('ctms_stg__cohort_summary_mapping')}} c
    where  d.tumor_type = c."tumor type"
        and d.study_id = 'XB010-101'
    and d.study_id = c.study_id and d.study_phase = c."study phase"
    and d.cohort is null and d.participant_status_irt = c.status
    union
    select distinct d.*,c.cohort as cohort_map  from dim_participant_cohort d,
	{{ref ('ctms_stg__cohort_summary_mapping')}} c
	where  d.tumor_type = c."tumor type"
    and d.study_id = 'XL309-101'
	and d.study_id = c.study_id and d.study_phase = c."study phase"
	and c.status = d.participant_status_irt
    and d.cohort is null
),
-- dim_participant_cohort_map as (
-- 	select distinct d.*,null as cohort_map  from dim_participant_cohort d
-- 	where d.cohort is not null
-- 	union
-- 	select distinct d.*,null as cohort_map  from dim_participant_cohort d
-- 	where d.cohort is null and study_id not in ('XL092-002','XB002-101')
-- 	union
-- 	select distinct d.*,c.cohort as cohort_map  from dim_participant_cohort d,
-- 	koios_raw.cohort_summary_mapping c
-- 	where  d.tumor_type = c."tumor type"
--     and d.study_id = 'XB002-101'
-- 	and d.study_id = c.study_id and d.study_phase = c."study phase"
-- 	and d.cohort is null
--     union
--     select distinct d.*,c.cohort as cohort_map  from dim_participant_cohort d,
-- 	koios_raw.cohort_summary_mapping c
-- 	where  d.tumor_type = c."tumor type" 
--     and d.study_id = 'XL092-002' and d.study_phase = 'Expansion'
-- 	and d.study_id = c.study_id and d.study_phase = c."study phase"
-- 	and d.cohort is null
--     union
-- 	select distinct d.*,null as cohort_map  from dim_participant_cohort d
-- 	where d.study_id = 'XL092-002' and d.study_phase = 'Escalation'
-- 	--group by d.study_id,d.study_phase,coalesce (participant_status_edc,participant_status_irt),tumor_type,c.cohort
-- 	--order by d.study_id,d.study_phase,tumor_type,coalesce (participant_status_edc,participant_status_irt);
-- )
dim_participant_cohort_map_final as (
    select distinct dpf.participant_id
    ,dpf.participant_id_sk
    ,dpf.study_subject_sk
    ,dpf.participant_number
    ,dpf.study_id
    ,dpf.study_id_sk
    ,dpf.race
    ,dpf.ethnic
    ,dpf.enrollment_date
    ,dpf.participant_status_pra
    ,dpf.participant_status_edc
    ,dpf.participant_status_irt
    ,dpf.participant_status_reason
    ,dpf.study_phase
    ,dpf.tumor_type
    ,dpf.cohort_desc
    ,case when dpf.cohort is null and dpf.cohort_map is not null then dpf.cohort_map
    when dpf.study_id = 'XB002-101' and dpf.tumor_type in ('Gastric Cancer and Gastro-esophageal Junction','Melanoma','Thyroid') then 'Cohort M'
    else dpf.cohort end as cohort
    ,dpf.treatment_arm
    ,dpf.dose_level_treatment_arm
	from dim_participant_cohort_map dpf
),
cohort_status as (
select study_id, cs.cohort, 
case when 'Open' in (select "cohort status" 
from (select distinct split_part("cohort description",':',1) as cohort, "cohort status" 
from {{ref ('ctms_stg__cohort_summary_report')}}
where study_id = 'XB002-101'
order by split_part("cohort description",':',1)) cf where cf.cohort = cs.cohort)
then 'Open'
else 'Closed'
end as "cohort status"
from (select distinct study_id, split_part("cohort description",':',1) as cohort, "cohort status" 
from {{ref ('ctms_stg__cohort_summary_report')}}
where study_id = 'XB002-101'
order by split_part("cohort description",':',1)) cs
union
select study_id, cs.cohort, 
case when 'Open' in 
(
select "cohort status" 
from 
(select 
distinct split_part("cohort description",' ',1) || ' ' || split_part("cohort description",' ',2) as cohort, "cohort status" 
from {{ref ('ctms_stg__cohort_summary_report')}}
where study_id = 'XL092-002'
order by split_part("cohort description",' ',1) || ' ' || split_part("cohort description",' ',2)
) cf where cf.cohort = cs.cohort
)
then 'Open'
else 'Closed'
end as "cohort status"
from (select distinct study_id, split_part("cohort description",' ',1) || ' ' || split_part("cohort description",' ',2) as cohort, "cohort status" 
from {{ref ('ctms_stg__cohort_summary_report')}}
where study_id = 'XL092-002'
order by split_part("cohort description",' ',1) || ' ' || split_part("cohort description",' ',2)) cs
union 
select distinct  study_id, cs.cohort, 
case when 'Open' in 
(
select trim("cohort status")
from 
(select 
distinct split_part("cohort description",' ',1) || ' ' || split_part("cohort description",' ',2) as cohort, "cohort status" 
from {{ref ('ctms_stg__cohort_summary_report')}}
where study_id = 'XL092-009'
order by split_part("cohort description",' ',1) || ' ' || split_part("cohort description",' ',2)
) cf where cf.cohort = cs.cohort
)
then 'Open'
else 'Closed'
end as "cohort status"
from (select distinct study_id, split_part("cohort description",' ',1) || ' ' || split_part("cohort description",' ',2) as cohort, "cohort status" 
from {{ref ('ctms_stg__cohort_summary_report')}}
where study_id = 'XL092-009'
and ("cohort status" = 'Open' or "cohort status" is not null)
order by split_part("cohort description",' ',1) || ' ' || split_part("cohort description",' ',2),"cohort status" desc) cs
union 
select distinct  study_id, cs.cohort, 
case when 'Open' in 
(
select trim("cohort status")
from 
(select 
distinct 
-- split_part("cohort description",':',1) as cohort, 
case when cohort ilike 'EXP%' then (array_to_string((regexp_split_to_array("cohort description", ' '))[:2],' '))
else split_part("cohort description",':',1) 
end as cohort,
"cohort status" 
from {{ref ('ctms_stg__cohort_summary_report')}}
where study_id = 'XB010-101'
order by 
-- split_part("cohort description",':',1)
case when cohort ilike 'EXP%' then (array_to_string((regexp_split_to_array("cohort description", ' '))[:2],' '))
else split_part("cohort description",':',1) 
end
) cf where cf.cohort = cs.cohort
)
then 'Open'
else 'Closed'
end as "cohort status"
from (select distinct study_id, 
-- split_part("cohort description",':',1) as cohort,
case when cohort ilike 'EXP%' then (array_to_string((regexp_split_to_array("cohort description", ' '))[:2],' '))
else split_part("cohort description",':',1) 
end as cohort,
 "cohort status" 
from {{ref ('ctms_stg__cohort_summary_report')}}
where study_id = 'XB010-101'
and ("cohort status" = 'Open' or "cohort status" is not null)
order by 
-- split_part("cohort description",':',1)
case when cohort ilike 'EXP%' then (array_to_string((regexp_split_to_array("cohort description", ' '))[:2],' '))
else split_part("cohort description",':',1) 
end
,"cohort status" desc) cs
union
select study_id, 'COHORT '||cs.cohort, 
case when 'Open' in (select "cohort status" 
from (select distinct split_part("cohort description",' ',1) as cohort, "cohort status" 
from {{ref ('ctms_stg__cohort_summary_report')}}
where study_id = 'XL495-101'
order by split_part("cohort description",' ',1)) cf where cf.cohort = cs.cohort)
then 'Open'
else 'Closed'
end as "cohort status"
from (select distinct study_id, split_part("cohort description",' ',1) as cohort, "cohort status" 
from {{ref ('ctms_stg__cohort_summary_report')}}
where study_id = 'XL495-101'
order by split_part("cohort description",' ',1)) cs
union 
select distinct  study_id, TRIM(cs.cohort), 
case when 'Open' in 
(
select trim("cohort status")
from 
(select 
distinct replace(split_part("cohort description",' ',1) || ' ' || split_part("cohort description",' ',2),':','') as cohort
, "cohort status" 
from {{ref ('ctms_stg__cohort_summary_report')}}
where study_id in ('XL309-101','XB628-101','XB371-101','XL092-011')
order by replace(split_part("cohort description",' ',1) || ' ' || split_part("cohort description",' ',2),':','')
) cf where cf.cohort = cs.cohort
)
then 'Open'
else 'Closed'
end as "cohort status"
from (select distinct study_id, replace(split_part("cohort description",' ',1) || ' ' || split_part("cohort description",' ',2),':','') as cohort
, "cohort status" 
from {{ref ('ctms_stg__cohort_summary_report')}}
where study_id in ('XL309-101','XB628-101','XB371-101','XL092-011')
and ("cohort status" = 'Open' or "cohort status" is not null)
order by replace(split_part("cohort description",' ',1) || ' ' || split_part("cohort description",' ',2),':',''),"cohort status" desc) cs 
),
missing_pages as (
    select 
    right("subject number",4) as participant_id ,
    md5(studyid ||right("subject number",4)) AS study_subject_sk_mp,
    md5(RIGHT("subject number",4))as participant_id_sk_mp,
    md5(studyid) as study_id_sk_mp,
    "total expected pgs" as total_expected_pgs,
    "missing pgs (entry not started)" as missing_pgs,
    "entered pgs" as entered_pgs
    from {{ref ('ctms_stg__subject_summary')}}
),
Final_data as(
select distinct dpf.participant_id
    ,dpf.participant_id_sk
    ,dpf.study_subject_sk
    ,dpf.participant_number
    ,dpf.study_id
    ,dpf.study_id_sk
    ,dpf.race
    ,dpf.ethnic
    ,dpf.enrollment_date
    ,dpf.participant_status_pra
    ,dpf.participant_status_edc
    ,dpf.participant_status_irt
    ,case 
	 	when upper(dpf.participant_status_irt) is not distinct from upper(lkp.participant_status_irt)
    	and upper(dpf.participant_status_edc) is not distinct from upper(lkp.participant_status_edc)
    	then lkp.participant_status
    	else dpf.participant_status_irt 
    	end as participant_status_master
    ,dpf.participant_status_reason
    ,dpf.study_phase
    ,dpf.tumor_type
    ,dpf.cohort_desc
    ,dpf.cohort
    ,case when dpf.study_id = 'XB002-101' and dpf.study_phase = 'Escalation' and dpf.tumor_type = 'SOLID TUMORS' then 'Open'
    when dpf.study_id in ('XB002-101', 'XL092-002','XL092-009','XB010-101','XL495-101','XL309-101','XB628-101','XL092-311','XB371-101','XL092-011') then cs."cohort status"
    else null end as cohort_status
    ,dpf.treatment_arm
    ,dpf.dose_level_treatment_arm
    ,mp.total_expected_pgs
    ,mp.entered_pgs
    ,mp.missing_pgs
	from dim_participant_cohort_map_final dpf
	left join cohort_status cs
	on lower(dpf.cohort) = lower(cs.cohort) and dpf.study_id = cs.study_id
    left join missing_pages mp
    on dpf.study_id_sk = mp.study_id_sk_mp and dpf.study_subject_sk = mp.study_subject_sk_mp and dpf.participant_id_sk = mp.participant_id_sk_mp
	left join {{source('koios_staging','participant_status_master')}} lkp
    on upper(dpf.participant_status_irt) is not distinct from upper(lkp.participant_status_irt)
    and upper(dpf.participant_status_edc) is not distinct from upper(lkp.participant_status_edc)
    )
	select * from Final_data
