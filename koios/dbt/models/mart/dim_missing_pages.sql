with dim_missing_pages as 
(
select 
cast(gen_random_uuid() as text) as page_id_sk
,mp.studyid as study_id
,sc.study_country_id
,right(mp.subject_number,4) as participant_number
,MD5(RIGHT(mp.subject_number, 4)) as participant_id_sk
,MD5(sc.study_country_id||studyid) as study_country_id_sk
,MD5(studyid||left(mp.site,4)) as study_site_id_sk
,MD5(mp.studyid) as study_id_sk
,left(mp.site,4) as site_number
,mp.subject_status
,mp.study_stage
,case when mp.enroll_or_screen_fail_date is not null then to_date(mp.enroll_or_screen_fail_date,'DDMONYY') else Null end as enroll_or_screen_fail_date
,mp.cohort
,mp.treatment_arm
,mp.visit_name
,mp.page_name
,mp.log_line
,case when mp.expected_date is not null then to_date(mp.expected_date,'DDMONYY') else Null end as expected_date
,mp.comments
,mp.days_overdue
,mp.overdue_timeframe
,mp.unique_id as page_id
from {{ref('ctms_stg__missing_pages')}} mp 
left join {{ref('ctms_stg__study_country')}} sc 
on mp.studyid = sc.study_protocol_number 
and mp.country = sc.country_name 
)
select * from dim_missing_pages
