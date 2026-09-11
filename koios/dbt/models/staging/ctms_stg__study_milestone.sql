with ctms_stg__study_milestone as (
select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_position as varchar) as  milestone_position
,cast(milestone_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||parent_id||milestone_name||milestone_position||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_milestone_xb002_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xb002_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xb002_101') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_position as varchar) as  milestone_position
,cast(milestone_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||parent_id||milestone_name||milestone_position||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_milestone_xl092-002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl092-002') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl092-002') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_position as varchar) as  milestone_position
,cast(milestone_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||parent_id||milestone_name||milestone_position||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_milestone_xl092-303') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl092-303') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl092-303') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_position as varchar) as  milestone_position
,cast(milestone_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||parent_id||milestone_name||milestone_position||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_milestone_xl092_001') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl092_001') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl092_001') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_position as varchar) as  milestone_position
,cast(milestone_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||parent_id||milestone_name||milestone_position||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_milestone_xl102_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl102_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl102_101') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_position as varchar) as  milestone_position
,cast(milestone_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||parent_id||milestone_name||milestone_position||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_milestone_xl114-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl114-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl114-101') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_position as varchar) as  milestone_position
,cast(milestone_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||parent_id||milestone_name||milestone_position||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_milestone_xl184_021') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl184_021') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl184_021') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_position as varchar) as  milestone_position
,cast(milestone_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||parent_id||milestone_name||milestone_position||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_milestone_xl184_311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl184_311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl184_311') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_position as varchar) as  milestone_position
,cast(milestone_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||parent_id||milestone_name||milestone_position||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_milestone_xl184_312') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl184_312') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl184_312') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_position as varchar) as  milestone_position
,cast(milestone_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||parent_id||milestone_name||milestone_position||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_milestone_xl184_315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl184_315') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl184_315') }})

UNION ALL

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_position as varchar) as  milestone_position
,cast(milestone_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||parent_id||milestone_name||milestone_position||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_milestone_xb010-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xb010-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xb010-101') }})


UNION ALL

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_position as varchar) as  milestone_position
,cast(milestone_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||parent_id||milestone_name||milestone_position||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_milestone_xl495-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl495-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl495-101') }})

-- UNION ALL

-- select distinct 
-- 	study_id as study_protocol_number,
--     'SITE' as parent_dataset,
-- 	"site number" as  parent_id,
-- 	'Site Activation' as milestone_name,
-- 	'Actual' as milestone_position,
-- 	case when "actual site activation date" = '00-Jan-00'
-- 	then NULL
-- 	else cast("actual site activation date" as text)
--         END  as milestone_date,
--     "partition_date" as load_date
--     ,md5(study_id||"site number"||'Site Activation'||'Actual'||"actual site activation date") as hash_key
-- from 
-- {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }}
-- where "actual site activation date"<>'00-Jan-00'
-- and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }})
--     and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }})

-- union all

-- select distinct 
-- 	study_id as study_protocol_number,
--     'SITE' as parent_dataset,
-- 	"site number" as  parent_id,
-- 	'Qualified' as milestone_name,
-- 	'Actual' as milestone_position,
-- 	"actual site selected date" as milestone_date,
--     "partition_date" as load_date,
--     md5(study_id||"site number"||'Qualified'||'Actual'||"actual site selected date") as hash_key
-- 	from 
-- 	{{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }} 
-- 	where "actual site selected date" is not null
--     and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }})
--     and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }})

-- union all

-- select distinct 
-- 	study_id as study_protocol_number,
--     'SITE' as parent_dataset,
-- 	cast("study site number" as varchar) as  parent_id,
-- 	'Site Activation' as milestone_name,
-- 	'Actual' as milestone_position,
-- 	CASE
--             WHEN "actual site ready to enroll" = 'DROPPED' THEN NULL
--             WHEN "actual site ready to enroll" ~~ '%-%-%'::text THEN (("actual site ready to enroll")::text)
--             ELSE NULL
--         END as milestone_date,
--     "partition_date" as load_date
--     ,md5(study_id||"study site number"||'Site Activation'||'Actual'||"actual site ready to enroll") as hash_key
-- from 
-- {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }}
-- where "actual site ready to enroll"<>'DROPPED'
-- and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }})
--     and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }})

-- -- Union all

-- select distinct 
-- 	study_id as study_protocol_number,
--     'SITE' as parent_dataset,
-- 	cast("study site number" as varchar) as  parent_id,
-- 	'Qualified' as milestone_name,
-- 	'Actual' as milestone_position,
-- 	"actual site selected date" as milestone_date,
--     "partition_date" as load_date,
--     md5(study_id||"study site number"||'Qualified'||'Actual'||"actual site selected date") as hash_key
-- 	from 
-- 	{{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }}
-- 	where "actual site selected date" is not null
--     and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }})
--     and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }})

-- UNION ALL

-- select distinct 
-- 	study_id as study_protocol_number,
--     'SITE' as parent_dataset,
-- 	cast("study site number"as varchar) as  parent_id,
-- 	'Site Activation' as milestone_name,
-- 	'Actual' as milestone_position,
-- 	case when "actual site ready to enroll" = '00-Jan-00'
-- 	then NULL
-- 	else "actual site ready to enroll"
--     END  as milestone_date,
--     "partition_date" as load_date
--     ,md5(study_id||"study site number"||'Site Activation'||'Actual'||"actual site ready to enroll") as hash_key
-- from 
-- {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }}
-- where "actual site ready to enroll"<>'00-Jan-00'
-- and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }})
--     and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }})

-- union all

-- select distinct 
-- 	study_id as study_protocol_number,
--     'SITE' as parent_dataset,
-- 	cast("study site number"as varchar) as  parent_id,
-- 	'Qualified' as milestone_name,
-- 	'Actual' as milestone_position,
-- 	"actual site selected date" as milestone_date,
--     "partition_date" as load_date,
--     md5(study_id||"study site number"||'Qualified'||'Actual'||"actual site selected date") as hash_key
-- 	from 
-- 	{{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }}
-- 	where "actual site selected date" is not null
--     and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }})
--     and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }})
union all
select
cast(clinical_study_source_id as varchar) as  study_protocol_number
,'SITE' as  parent_dataset
,cast(study_site_source_id as varchar) as  parent_id
,case   
    when milestone_name = 'Actual Site Ready to Enroll' then 'Site Activation'
    when milestone_name = 'Actual Site Selection' then 'Qualified'
    when milestone_name = 'Actual Site Closure' then 'Closed'
    else milestone_name
end as  milestone_name
,cast(milestone_type as varchar) as  milestone_position
-- ,to_char(to_date(milestone_date , 'dd/mm/yyyy'), 'DD-MON-YYYY') as  milestone_date
,case when milestone_date ~'^\d{2}/\d{2}/\d{4}$'
then TO_CHAR(TO_DATE(milestone_date, 'MM/DD/YYYY'),'DD-MON-YYYY')        
else null
end as milestone_date 
,partition_date as load_date
,last_modified_date
,md5(clinical_study_source_id||study_site_source_id||milestone_name||milestone_type||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'q_exe_sitemilestone_xl092-304') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_sitemilestone_xl092-304') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_sitemilestone_xl092-304') }})
union all
select
cast(clinical_study_source_id as varchar) as  study_protocol_number
,'STUDY' as  parent_dataset
,null as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_type as varchar) as  milestone_position
-- ,to_char(to_date(milestone_date , 'dd/mm/yyyy'), 'DD-MON-YYYY') as  milestone_date
,case when milestone_date ~'^\d{2}/\d{2}/\d{4}$'
then TO_CHAR(TO_DATE(milestone_date, 'MM/DD/YYYY'),'DD-MON-YYYY')        
else null
end as milestone_date 
,partition_date as load_date
,last_modified_date
,md5(clinical_study_source_id||''||milestone_name||milestone_type||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'q_exe_studymilestone_xl092-304') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studymilestone_xl092-304') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studymilestone_xl092-304') }})

union all
select
cast(clinical_study_source_id as varchar) as  study_protocol_number
,'SITE' as  parent_dataset
,cast(study_site_source_id as varchar) as  parent_id
,case   
    when milestone_name = 'Actual Site Ready to Enroll' then 'Site Activation'
    when milestone_name = 'Actual Site Selection' then 'Qualified'
    when milestone_name = 'Actual Site Closure' then 'Closed'
    else milestone_name
end as  milestone_name
,cast(milestone_type as varchar) as  milestone_position
-- ,to_char(to_date(milestone_date , 'dd/mm/yyyy'), 'DD-MON-YYYY') as  milestone_date
,case when milestone_date ~'^\d{2}/\d{2}/\d{4}$'
then TO_CHAR(TO_DATE(milestone_date, 'MM/DD/YYYY'),'DD-MON-YYYY')        
else null
end as milestone_date 
,partition_date as load_date
,last_modified_date
,md5(clinical_study_source_id||study_site_source_id||milestone_name||milestone_type||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'q_exe_sitemilestone_xl092-305') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_sitemilestone_xl092-305') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_sitemilestone_xl092-305') }})
union all
select
cast(clinical_study_source_id as varchar) as  study_protocol_number
,'STUDY' as  parent_dataset
,null as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_type as varchar) as  milestone_position
-- ,to_char(to_date(milestone_date , 'dd/mm/yyyy'), 'DD-MON-YYYY') as  milestone_date
,case when milestone_date ~'^\d{2}/\d{2}/\d{4}$'
then TO_CHAR(TO_DATE(milestone_date, 'MM/DD/YYYY'),'DD-MON-YYYY')        
else null
end as milestone_date 
,partition_date as load_date
,last_modified_date
,md5(clinical_study_source_id||''||milestone_name||milestone_type||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'q_exe_studymilestone_xl092-305') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studymilestone_xl092-305') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studymilestone_xl092-305') }})

union all
select
cast(clinical_study_source_id as varchar) as  study_protocol_number
,'SITE' as  parent_dataset
,cast(study_site_source_id as varchar) as  parent_id
,case   
    when milestone_name = 'Actual Site Ready to Enroll' then 'Site Activation'
    when milestone_name = 'Actual Site Selection' then 'Qualified'
    when milestone_name = 'Actual Site Closure' then 'Closed'
    else milestone_name
end as  milestone_name
,cast(milestone_type as varchar) as  milestone_position
-- ,to_char(to_date(milestone_date , 'dd/mm/yyyy'), 'DD-MON-YYYY') as  milestone_date
,case when milestone_date ~'^\d{2}/\d{2}/\d{4}$'
then TO_CHAR(TO_DATE(milestone_date, 'MM/DD/YYYY'),'DD-MON-YYYY')        
else null
end as milestone_date 
,partition_date as load_date
,last_modified_date
,md5(clinical_study_source_id||study_site_source_id||milestone_name||milestone_type||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'q_exe_sitemilestone_xl092-009') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_sitemilestone_xl092-009') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_sitemilestone_xl092-009') }})
union all
select
cast(clinical_study_source_id as varchar) as  study_protocol_number
,'STUDY' as  parent_dataset
,null as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_type as varchar) as  milestone_position
-- ,to_char(to_date(milestone_date , 'dd/mm/yyyy'), 'DD-MON-YYYY') as  milestone_date
,case when milestone_date ~'^\d{2}/\d{2}/\d{4}$'
then TO_CHAR(TO_DATE(milestone_date, 'MM/DD/YYYY'),'DD-MON-YYYY')        
else null
end as milestone_date 
,partition_date as load_date
,last_modified_date
,md5(clinical_study_source_id||''||milestone_name||milestone_type||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'q_exe_studymilestone_xl092-009') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studymilestone_xl092-009') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studymilestone_xl092-009') }})

    UNION ALL

select distinct 
	study_id as study_protocol_number,
    'SITE' as parent_dataset,
	cast("site number" as varchar) as  parent_id,
	'Site Activation' as milestone_name,
	'Actual' as milestone_position,
	case when length("site activation date")> 9 or length("site activation date")< 8 
    then NULL
    else "site activation date" 
    end as milestone_date,
    "partition_date" as load_date
    ,last_modified_date
    ,md5(study_id||"site number"||'Site Activation'||'Actual'||"site activation date") as hash_key
from 
{{ source ('koios_raw', 'site_contact_details_actual_xl309-101') }}
where "site activation date"is not null and length("site activation date")>=8
and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl309-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl309-101') }})

union all

select distinct 
	study_id as study_protocol_number,
    'SITE' as parent_dataset,
	CAST("site number" as varchar) as  parent_id,
	'Qualified' as milestone_name,
	'Actual' as milestone_position,
	case when length("site selection date")> 9 or length("site selection date")< 8  then null
    else "site selection date" 
    end as milestone_date,
    "partition_date" as load_date
    ,last_modified_date
    ,md5(study_id||"site number"||'Qualified'||'Actual'||"site selection date") as hash_key
from 
{{ source ('koios_raw', 'site_contact_details_actual_xl309-101') }}
where "site selection date" is not null and length("site selection date")>=8
and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl309-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl309-101') }})
union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_position as varchar) as  milestone_position
,cast(milestone_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||parent_id||milestone_name||milestone_position||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_milestone_xb628-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xb628-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xb628-101') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_position as varchar) as  milestone_position
,cast(milestone_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||parent_id||milestone_name||milestone_position||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_milestone_xl092-311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl092-311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl092-311') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_position as varchar) as  milestone_position
,cast(milestone_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||parent_id||milestone_name||milestone_position||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_milestone_xb371-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xb371-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xb371-101') }})

union all
select
cast(study_protocol_number as varchar) as  study_protocol_number
,'SITE' as  parent_dataset
,cast(study_site_id as varchar) as  parent_id
,'Qualified' as  milestone_name
,'Actual' as  milestone_position
,cast(status_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||'SITE'||'Qualified'||'Actual'||status_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_status_hist_xb371-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_status_hist_xb371-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_status_hist_xb371-101') }})
    and study_site_status in ('Selected')

union all

select
study_id as study_protocol_number
--cast(study_id as varchar) as  study_protocol_number
,'SITE' as  parent_dataset
,cast("parexel site reference" as varchar) as  parent_id
,'Site Activation' as  milestone_name
,'Actual' as  milestone_position
,cast("site activation actual date" as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_id||'SITE'||'Site Activation'||'Actual'||"site activation actual date") as hash_key
FROM
    {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }}
    where last_modified_date=(select max(last_modified_date) from {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }})
    and partition_date=(select max(partition_date) from {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }})
    and "site activation actual date" is not null

union all

select
study_id as study_protocol_number
--cast(study_id as varchar) as  study_protocol_number
,'SITE' as  parent_dataset
,cast("parexel site reference" as varchar) as  parent_id
,'Qualified' as  milestone_name
,'Actual' as  milestone_position
,cast("selected date" as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_id||'SITE'||'Qualified'||'Actual'||"selected date") as hash_key
FROM
    {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }}
    where last_modified_date=(select max(last_modified_date) from {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }})
    and partition_date=(select max(partition_date) from {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }})
 

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_position as varchar) as  milestone_position
,cast(milestone_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||parent_id||milestone_name||milestone_position||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_milestone_xl092-202') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl092-202') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl092-202') }})

union all
select
cast(study_protocol_number as varchar) as  study_protocol_number
,'SITE' as  parent_dataset
,cast(study_site_id as varchar) as  parent_id
,'Qualified' as  milestone_name
,'Actual' as  milestone_position
,cast(status_date as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||'SITE'||'Qualified'||'Actual'||status_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_status_hist_xl092-202') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_status_hist_xl092-202') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_status_hist_xl092-202') }})
    and study_site_status in ('Selected')

union all
select
study_id as study_protocol_number
--cast(study_id as varchar) as  study_protocol_number
,'SITE' as  parent_dataset
,cast(ID as varchar) as  parent_id
,'Site Activation' as  milestone_name
,'Actual' as  milestone_position
,cast("startup - activation date" as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_id||'SITE'||'Site Activation'||'Actual'||"startup - activation date") as hash_key
FROM
    {{ source ('koios_raw', 'site_information_report_xl092-011') }}
    where last_modified_date=(select max(last_modified_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})
    and partition_date=(select max(partition_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})
    and "startup - activation date" is not null

union all

select
study_id as study_protocol_number
--cast(study_id as varchar) as  study_protocol_number
,'SITE' as  parent_dataset
,cast(ID as varchar) as  parent_id
,'Qualified' as  milestone_name
,'Actual' as  milestone_position
,cast("startup - activation date - planned" as varchar) as  milestone_date
,partition_date as load_date
,last_modified_date
,md5(study_id||'SITE'||'Qualified'||'Actual'||"startup - activation date - planned") as hash_key
FROM
    {{ source ('koios_raw', 'site_information_report_xl092-011') }}
    where last_modified_date=(select max(last_modified_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})
    and partition_date=(select max(partition_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})


)

select * from ctms_stg__study_milestone
