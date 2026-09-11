with fct_study_milestone as (
    select distinct
    md5(study_protocol_number) as study_id_sk,
    milestone_name,
    milestone_position,
    milestone_date,
    load_date
from {{ref ('ctms_stg__study_milestone')}}
where parent_dataset = 'STUDY'
)
select * from fct_study_milestone