{{ config(materialized='view',
    indexes=[{'default': True}]) }}

with vw_study_milestone as(
    select distinct
      study_id_sk,
      max(case when milestone_position = 'Target' then CAST(milestone_date as date) end) as target,
      max(case when milestone_position = 'Actual' then CAST(milestone_date as date) end) as actual,
      max(case when milestone_position = 'Projected' then CAST(milestone_date as date) end) as projected
from {{ref ('fct_study_milestone')}}
group by study_id_sk,milestone_name

)
select * from vw_study_milestone