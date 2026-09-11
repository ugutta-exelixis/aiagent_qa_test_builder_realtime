with fct_site_milestone as (
    select distinct
	md5(study_protocol_number) as study_id_sk,
	parent_id,
	md5(parent_id) as study_site_id_sk,
	milestone_name,
	milestone_position,
	to_date(milestone_date,'DD-MON-YY') as milestone_date
from {{ref ('ctms_stg__study_milestone')}}
where parent_dataset = 'SITE'
and milestone_date is not null 
)
select * from fct_site_milestone
