with subject_visit_summary_report as(
    select distinct
   case when study_id = 'XL092-304' then to_date("scheduled date",'DD Mon YYY')
   else cast("scheduled date" as date) end as date_of_visit_scheduled,
   case when study_id = 'XL092-304' then to_date("actual date",'DD Mon YYY') 
   else cast("actual date" as date) end as date_of_visit_actual,
    "visit description" as visit_description,
    MD5(RIGHT(subject, 4)) as participant_id_sk,
    md5(RIGHT(subject,4)) as participant_number_sk,
    RIGHT(subject,4) as participant_number,
    case when study_id in('XL092-009','XL092-305') then studysite
    when study_id not in('XL092-009','XL092-305') and length(subject) = 9 then split_part(subject,'-',1) 
    when study_id not in('XL092-009','XL092-305') and length(subject) > 9 then split_part(subject,'-',2) else null end as site_id,
    /*MD5(case when length(subject) = 9 then split_part(subject,'-',1) 
    when length(subject) > 9 then split_part(subject,'-',2) else null end) as study_site_id_sk,*/
    subject,
    country,
    study_id,
    md5(study_id) as study_id_sk
    from {{ref ('ctms_stg__subject_visit_summary_report')}}
),
early_phase_data as(
select distinct
a.date_of_visit_scheduled,
a.date_of_visit_actual,
a.visit_description,
MD5(RIGHT(a.participant_number ,4)) as participant_id_sk,
MD5(RIGHT(a.participant_number,4)) as participant_number_sk,
RIGHT(a.participant_number,4) as participant_number,
MD5(a.study_id||split_part(a.participant_number,'-',2)) as study_site_id_sk,
b.study_country_id,
a.study_id,
md5(a.study_id) as study_id_sk
from {{ref('ctms_stg__early_phase_subject_visit')}} a,
{{ref('ctms_stg__study_site')}} b
where a.site_number = b.site_number
and a.study_id = b.study_protocol_number
),
study_country as (
    select 
    study_protocol_number as study_id,
    country_name,
    study_country_id
    from {{ref('ctms_stg__study_country')}}
),
subject_summary_by_visit as(
    select distinct
    country,
    RIGHT("subject number", 4) as participant_number,
    md5(RIGHT("subject number", 4)) as participant_id_sk,
    md5(RIGHT("subject number", 4)) as participant_number_sk,
    case when studyid in('XL092-009','XL092-305') 
    then md5(studyid||substring(site,0,5))
    else md5(studyid||substring("subject number",8,4))
    end as study_site_id_sk,
    md5(studyid) as study_id_sk,
    md5(sc.study_country_id||ss.studyid) as study_country_id_sk,
    ss.studyid as study_id,
    to_date("date of first icf",'DD-MON-YY') as date_of_first_icf,
    to_date("enroll or screen fail date",'DD-MON-YY') as randomization_or_screen_fail_date,
    "protocol version randomized" as protocol_version_randomized,
    "treatment arm" as treatment_arm,
    "most recent visit" as most_recent_visit_type,
    to_date("most recent date of visit",'DD-MON-YY') as most_recent_date_of_visit,
    to_date("eotxb date",'DD-MON-YY') as xl092__eot_date,
    to_date("nivolumab eot date",'DD-MON-YY') as nivolumab_eot_date,
    to_date("sunitinib eot date",'DD-MON-YY') as sunitinib_eot_date,
    to_date("death date",'DD-MON-YY') as death_date,
    to_date("eorfup date",'DD-MON-YY') as eoruf_date,
    to_date("eos date",'DD-MON-YY') as eos_date,
    "eos reason" as eos_reason,
    "visit name" as visit_name
from {{ref ('ctms_stg__subject_summary_by_visit')}} ss
left join {{ref('ctms_stg__study_country')}} sc
on ss.country = sc.country_name
and ss.studyid = sc.study_protocol_number
),

fct_part_visit_early_phase as (
    select distinct
    ep.participant_id_sk
    ,ep.study_site_id_sk
    ,ep.study_country_id
    ,ep.study_id
    ,ep.participant_number_sk
    ,ep.participant_number
    ,md5(ep.study_country_id||ep.study_id) as study_country_id_sk
    ,ep.date_of_visit_scheduled,
    ep.date_of_visit_actual,
    ep.visit_description,
    sv.date_of_first_icf,
    sv.randomization_or_screen_fail_date,
    sv.protocol_version_randomized,
    sv.treatment_arm,
    sv.most_recent_visit_type,
    sv.most_recent_date_of_visit,
    sv.xl092__eot_date,
    sv.nivolumab_eot_date,
    sv.sunitinib_eot_date,
    sv.death_date,
    sv.eoruf_date,
    sv.eos_date,
    sv.eos_reason
from early_phase_data as ep
left join subject_summary_by_visit  as sv
on ep.participant_number=sv.participant_number
and ep.study_id = sv.study_id
),

fct_part_visit as (
    select distinct
    sv.participant_id_sk as participant_id_sk_sv
    ,sr.participant_id_sk as participant_id_sk_sr
    ,sv.study_site_id_sk as study_site_id_sk_sv
    ,md5(sr.study_id||sr.site_id) as study_site_id_sk_sr
    ,sc.study_country_id
    ,sr.study_id as study_id_sr
    ,sv.study_id as study_id_sv
    ,sr.participant_number_sk as participant_number_sk_sr
    ,sv.participant_number_sk as participant_number_sk_sv
    ,sr.participant_number as participant_number_sr
    ,sv.participant_number as participant_number_sv
    ,md5(sc.study_country_id||sr.study_id) as study_country_id_sk_sr
    ,md5(sc.study_country_id||sv.study_id) as study_country_id_sk_sv
    ,sr.date_of_visit_scheduled,
    sr.date_of_visit_actual,
    sr.visit_description,
    sv.date_of_first_icf,
    sv.randomization_or_screen_fail_date,
    sv.protocol_version_randomized,
    sv.treatment_arm,
    sv.most_recent_visit_type,
    sv.most_recent_date_of_visit,
    sv.xl092__eot_date,
    sv.nivolumab_eot_date,
    sv.sunitinib_eot_date,
    sv.death_date,
    sv.eoruf_date,
    sv.eos_date,
    sv.eos_reason
from subject_visit_summary_report as sr
left join study_country sc
on sr.country = sc.country_name
and sr.study_id = sc.study_id
left join subject_summary_by_visit  as sv
on sr.participant_id_sk=sv.participant_number_sk
and sr.study_id_sk = sv.study_id_sk
),

fct_participant_visit as (
select
participant_id_sk_sr as participant_id_sk
,study_site_id_sk_sr as study_site_id_sk
,md5(study_id_sr) as study_id_sk
,study_id_sr as study_id
,participant_number_sk_sr as participant_number_sk
,participant_number_sr as participant_number
,study_country_id_sk_sr as study_country_id_sk
,cast(date_of_visit_scheduled as date) as date_of_visit_scheduled,
cast(date_of_visit_actual as date) as date_of_visit_actual,
visit_description,
date_of_first_icf,
randomization_or_screen_fail_date,
protocol_version_randomized,
treatment_arm,
most_recent_visit_type,
most_recent_date_of_visit,
xl092__eot_date,
nivolumab_eot_date,
sunitinib_eot_date,
death_date,
eoruf_date,
eos_date,
eos_reason
from fct_part_visit
where participant_id_sk_sr is not null
union
select distinct
participant_id_sk
,study_site_id_sk
,md5(study_id) as study_id_sk
,study_id
,participant_number_sk
,participant_number
,study_country_id_sk
,cast(date_of_visit_actual as date) as date_of_visit_scheduled,
cast(date_of_visit_scheduled as date) as date_of_visit_actual,
visit_description,
date_of_first_icf,
randomization_or_screen_fail_date,
protocol_version_randomized,
treatment_arm,
most_recent_visit_type,
most_recent_date_of_visit,
xl092__eot_date,
nivolumab_eot_date,
sunitinib_eot_date,
death_date,
eoruf_date,
eos_date,
eos_reason
from fct_part_visit_early_phase
where  concat(study_id,participant_number) not in (select concat(study_id_sr,participant_number_sr) from fct_part_visit where participant_id_sk_sr is not null)
),

fct_participant_visit_final as (
select 
participant_id_sk
,study_site_id_sk
,md5(study_id) as study_id_sk
,study_id
,participant_number_sk
,participant_number
,study_country_id_sk
,cast(date_of_visit_scheduled as date) as date_of_visit_scheduled,
date_of_visit_actual,
visit_description,
date_of_first_icf,
randomization_or_screen_fail_date,
protocol_version_randomized,
treatment_arm,
most_recent_visit_type,
most_recent_date_of_visit,
xl092__eot_date,
nivolumab_eot_date,
sunitinib_eot_date,
death_date,
eoruf_date,
eos_date,
eos_reason
from fct_participant_visit
union
select
participant_id_sk
,study_site_id_sk
,md5(study_id) as study_id_sk
,study_id
,participant_number_sk
,participant_number
,study_country_id_sk
,Null as date_of_visit_scheduled,
Null as date_of_visit_actual,
visit_name as visit_description,
date_of_first_icf,
randomization_or_screen_fail_date,
protocol_version_randomized,
treatment_arm,
most_recent_visit_type,
most_recent_date_of_visit,
xl092__eot_date,
nivolumab_eot_date,
sunitinib_eot_date,
death_date,
eoruf_date,
eos_date,
eos_reason
from subject_summary_by_visit
where concat(study_id,participant_number) not in (select distinct concat(study_id,participant_number) from fct_participant_visit)
)

select distinct * from fct_participant_visit_final
