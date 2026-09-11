with ctms_stg__study_milestone as (
select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(milestone_name as varchar) as  milestone_name
,cast(milestone_position as varchar) as  milestone_position
,cast(milestone_date as varchar) as  milestone_date
,partition_date as load_date
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
,md5(study_protocol_number||parent_id||milestone_name||milestone_position||milestone_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_milestone_xl184_315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl184_315') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_milestone_xl184_315') }})

UNION ALL

select distinct 
	study_id as study_protocol_number,
    'SITE' as parent_dataset,
	"site number" as  parent_id,
	'Site Activation' as milestone_name,
	'Actual' as milestone_position,
	case when "actual site activation date" = '00-Jan-00'
	then NULL
	else cast("actual site activation date" as text)
        END  as milestone_date,
    "partition_date" as load_date
    ,md5(study_id||"site number"||'Site Activation'||'Actual'||"actual site activation date") as hash_key
from 
{{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }}
where "actual site activation date"<>'00-Jan-00'
and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }})

union all

select distinct 
	study_id as study_protocol_number,
    'SITE' as parent_dataset,
	"site number" as  parent_id,
	'Qualified' as milestone_name,
	'Actual' as milestone_position,
	"actual site selected date" as milestone_date,
    "partition_date" as load_date,
    md5(study_id||"site number"||'Qualified'||'Actual'||"actual site selected date") as hash_key
	from 
	{{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }} 
	where "actual site selected date" is not null
    and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }})

union all

select distinct 
	study_id as study_protocol_number,
    'SITE' as parent_dataset,
	cast("study site number" as varchar) as  parent_id,
	'Site Activation' as milestone_name,
	'Actual' as milestone_position,
	CASE
            WHEN "actual site ready to enroll" = 'DROPPED' THEN NULL
            WHEN "actual site ready to enroll" ~~ '%-%-%'::text THEN (("actual site ready to enroll")::text)
            ELSE NULL
        END as milestone_date,
    "partition_date" as load_date
    ,md5(study_id||"study site number"||'Site Activation'||'Actual'||"actual site ready to enroll") as hash_key
from 
{{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }}
where "actual site ready to enroll"<>'DROPPED'
and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }})

Union all

select distinct 
	study_id as study_protocol_number,
    'SITE' as parent_dataset,
	cast("study site number" as varchar) as  parent_id,
	'Qualified' as milestone_name,
	'Actual' as milestone_position,
	"actual site selected date" as milestone_date,
    "partition_date" as load_date,
    md5(study_id||"study site number"||'Qualified'||'Actual'||"actual site selected date") as hash_key
	from 
	{{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }}
	where "actual site selected date" is not null
    and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }})

UNION ALL

select distinct 
	study_id as study_protocol_number,
    'SITE' as parent_dataset,
	cast("study site number"as varchar) as  parent_id,
	'Site Activation' as milestone_name,
	'Actual' as milestone_position,
	case when "actual site ready to enroll" = '00-Jan-00'
	then NULL
	else "actual site ready to enroll"
    END  as milestone_date,
    "partition_date" as load_date
    ,md5(study_id||"study site number"||'Site Activation'||'Actual'||"actual site ready to enroll") as hash_key
from 
{{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }}
where "actual site ready to enroll"<>'00-Jan-00'
and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }})

union all

select distinct 
	study_id as study_protocol_number,
    'SITE' as parent_dataset,
	cast("study site number"as varchar) as  parent_id,
	'Qualified' as milestone_name,
	'Actual' as milestone_position,
	"actual site selected date" as milestone_date,
    "partition_date" as load_date,
    md5(study_id||"study site number"||'Qualified'||'Actual'||"actual site selected date") as hash_key
	from 
	{{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }}
	where "actual site selected date" is not null
    and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }})

)

select * from ctms_stg__study_milestone