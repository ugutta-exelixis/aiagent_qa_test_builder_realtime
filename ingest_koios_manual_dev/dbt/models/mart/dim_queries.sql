
with dim_queries as(
select
cast(gen_random_uuid() as text) as query_id_sk
,sc.study_country_id
,md5(qd.query_id) as query_key
,qd.query_id
,qd.subject_number
,qd.studyid
,qd.country
,qd.site
,qd.query_status
,qd.visit_name
,qd.page_name
,qd.page_link
,qd.field_oid
,qd.query_response as response
,qd.issued_by_name
,qd.query_recipient
,case when qd.opened_date is not null then to_date(qd.opened_date,'DDMONYY') else Null end as opened_date_id
,case when qd.answered_date is not null then to_date(qd.answered_date,'DDMONYY') else Null end as answered_date_id
,qd.answered_by
,case when qd.cancelled_date is not null then to_date(qd.cancelled_date,'DDMONYY') else Null end as cancelled_date_id
,qd.cancelled_by
,case when qd.closed_date is not null then to_date(qd.closed_date,'DDMONYY') else Null end as closed_date_id
,qd.closed_by
,qd.last_opened_query_text
,to_date(qd.load_date,'YYYY-MM-DD') as load_date
from {{ref ('ctms_stg__query_detail')}} qd
left join {{ref('ctms_stg__study_country')}} sc
on qd.studyid = sc.study_protocol_number
and qd.country = sc.country_name
)
select * from dim_queries