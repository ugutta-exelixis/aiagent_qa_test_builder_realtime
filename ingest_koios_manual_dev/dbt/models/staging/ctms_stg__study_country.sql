with ctms_stg__study_country as (
select distinct 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,REPLACE(country_name, 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xb002_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xb002_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xb002_101') }})

union

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,REPLACE(country_name, 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xl092-002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl092-002') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl092-002') }})

union

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,REPLACE(country_name, 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xl092-303') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl092-303') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl092-303') }})

union

select distinct 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,REPLACE(country_name, 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xl092_001') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl092_001') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl092_001') }})

union

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,REPLACE(country_name, 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xl102_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl102_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl102_101') }})

union

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,REPLACE(country_name, 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xl114-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl114-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl114-101') }})

union

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,REPLACE(country_name, 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xl184_021') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl184_021') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl184_021') }})

union

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,REPLACE(country_name, 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xl184_311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl184_311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl184_311') }})

union

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,REPLACE(country_name, 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xl184_312') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl184_312') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl184_312') }})

union

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,REPLACE(country_name, 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xl184_315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl184_315') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl184_315') }})

UNION 

select distinct 
study_id  as study_protocol_number
,null as study_country_id
,case
	when upper(country) = 'KOREA, REPUBLIC OF' then 'SOUTH KOREA'
	when upper(country) = 'TAIWAN, REPUBLIC OF CHINA' then 'TAIWAN' 
	else upper(country)
end as country_name
,case 
	when country in (select distinct country
        FROM {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }}
            where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }})
            and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }})
            and "current study site status" in ('Enrollment Open')) then 'Active'
		else 'Approved' end
    as country_status
,partition_date as load_date
,md5(study_id||md5(country)) as hash_key
from {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }}
where country is not null
and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }})

UNION

SELECT distinct
	study_id as study_protocol_number,
	null as study_country_id,
	case
		when upper("standard country name") = 'KOREA, REPUBLIC OF' then 'SOUTH KOREA'
		when upper("standard country name") = 'TAIWAN, REPUBLIC OF CHINA' then 'TAIWAN' 
	else upper("standard country name")
	end as country_name,
	case 
		when "standard country name" in (select distinct "standard country name"
        FROM koios_raw."site_contact_details_actual_xl092-304"
            where last_modified_date = (select max(last_modified_date) from koios_raw."site_contact_details_actual_xl092-304")
            and partition_date = (select max(partition_date) from koios_raw."site_contact_details_actual_xl092-304")
            and "current study site status" in ('Enrollment Open')) then 'Active'
		else 'Approved' end  
	as country_status,
    partition_date as load_date,
	md5(study_id||md5("standard country name")) as hash_key
    FROM {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }})

union 

select distinct 
study_id  as study_protocol_number
,null as study_country_id
,case
	when upper("standard country name") = 'KOREA, REPUBLIC OF' then 'SOUTH KOREA'
	when upper("standard country name") = 'TAIWAN, REPUBLIC OF CHINA' then 'TAIWAN' 
	else upper("standard country name")
end as country_name
,case 
	when "standard country name" in (select distinct "standard country name"
        FROM 
        {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }}
            where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }})
            and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }})
            and "current study site status" in ('Enrollment Open')) then 'Active'
		else 'Approved' end
    as country_status
,partition_date as load_date
,md5(study_id||md5("standard country name")) as hash_key
from {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }}
where "standard country name" is not null
and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }})

),
country_region_mapping as 
(
    select distinct
    country as country_name
    ,cast(1000+dense_rank() over (order by country) as varchar) as study_country_id
    ,region as country_region
    from {{ ref('stg_country_region_mapping') }}
)

select 
sc.study_protocol_number
,sc.study_country_id	
,sc.country_name	
,sc.country_status	
,sc.load_date	
,sc.hash_key
from ctms_stg__study_country sc
where study_country_id is not null
union 
select 
sc.study_protocol_number
,rm.study_country_id	
,sc.country_name	
,sc.country_status	
,sc.load_date	
,sc.hash_key
from ctms_stg__study_country sc
LEFT join country_region_mapping rm 
on sc.country_name = rm.country_name
where sc.study_country_id is null