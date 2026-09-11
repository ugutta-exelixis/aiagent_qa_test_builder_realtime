{{ config(materialized='view',
    indexes=[{'default': True}]) }}

with vw_participant_milestone as(
    select distinct
      study_id_sk,
      participant_id_sk,
      milestone_name,
      max(case when milestone_position = 'Target' then milestone_date end) as target,
      max(case when milestone_position = 'Actual' then milestone_date end) as actual,
      max(case when milestone_position = 'Projected' then milestone_date end) as projected
from {{ref ('fct_participant_milestone')}}
group by study_id_sk,participant_id_sk,milestone_name
)
select * from vw_participant_milestone