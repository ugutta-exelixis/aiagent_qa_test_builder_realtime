{{ config(materialized='view',
    indexes=[{'default': True}]) }}

with vw_site_milestone as(
    select distinct 
      study_id_sk,
      parent_id,
      study_site_id_sk,
      milestone_name,
      max(case when milestone_position = 'Target' then milestone_date end) as target,
      max(case when milestone_position = 'Actual' then milestone_date end) as actual,
      max(case when milestone_position = 'Projected' then milestone_date end) as projected
from {{ref ('fct_site_milestone')}}
group by study_id_sk,study_site_id_sk,milestone_name,parent_id
)
select * from vw_site_milestone