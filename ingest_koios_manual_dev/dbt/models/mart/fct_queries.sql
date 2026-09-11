with fct_queries as(
    select
	query_id_sk,
	query_key,
	MD5(RIGHT(subject_number, 4)) as participant_id_sk,
	md5(study_country_id||studyid) as study_country_id_sk,
	MD5(studyid||left(site,4)) as study_site_id_sk,
	md5(studyid) as study_id_sk,
	query_status,
	visit_name,
	page_name,
	page_link,
	field_oid,
	response,
	issued_by_name,
	query_recipient,
	opened_date_id,
	answered_date_id,
	answered_by,
	cancelled_date_id,
	cancelled_by,
	closed_date_id,
	closed_by,
	last_opened_query_text,
	load_date
from {{ref ('dim_queries')}}
)
select * from fct_queries