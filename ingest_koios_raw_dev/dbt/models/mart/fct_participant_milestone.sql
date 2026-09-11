with fct_participant_milestone as(
    select  distinct
    md5(study_protocol_number) as study_id_sk,
    md5(parent_id) as participant_id_sk,
    null as study_country_id_sk,
    milestone_name,
    milestone_position,
    to_date(milestone_date,'DD-MON-YY') as milestone_date,
    null as cohort,
    load_date
  from {{ref ('ctms_stg__study_milestone')}}
  where parent_dataset = 'SUBJECT'
)
select * from fct_participant_milestone