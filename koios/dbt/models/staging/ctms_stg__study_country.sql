with ctms_stg__study_country as (
select distinct 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xb002_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xb002_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xb002_101') }})

union

select distinct 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xb010-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xb010-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xb010-101') }})

union

select distinct 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,REPLACE(country_name, 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xl495-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl495-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl495-101') }})

union

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,REPLACE(country_name, 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,last_modified_date
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
,last_modified_date
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
,last_modified_date
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
,last_modified_date
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
,last_modified_date
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
,last_modified_date
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
,last_modified_date
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
,last_modified_date
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
,last_modified_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xl184_315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl184_315') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl184_315') }})
union
select distinct
cast(sc.clinical_study_source_id as varchar) as  study_protocol_number
,null as  study_country_id
,case
        when upper(sc.study_region) = 'KOREA, REPUBLIC OF' then 'SOUTH KOREA'
        when upper(sc.study_region) = 'UNITED STATES OF AMERICA' then 'UNITED STATES'
        when upper(sc.study_region) = 'TAIWAN, REPUBLIC OF CHINA' then 'TAIWAN' 
        else upper(sc.study_region)
end as country_name
,case 
    when COUNT(case when s.site_status in('Enrollment Open','Enrollment On-Hold') or s.site_status = 'Enrollment Closed'  then 1 end)
        over(partition by sc.study_country_source_id) > 0 then 'Active'
    when COUNT( case when s.site_status = 'Selected' then 1 end ) 
        over(partition by sc.study_country_source_id) > 0 then 'Approved'
    when COUNT ( case when s.site_status = 'Dropped' then 1 end)
        over(partition by sc.study_country_source_id) > 0 then 'Terminated'
    else coalesce(s.site_status, sc.study_region_status)
end as country_status
,sc.partition_date as load_date
,sc.last_modified_date
,md5(sc.clinical_study_source_id||sc.study_country_source_id) as hash_key
FROM
    {{ source ('koios_raw', 'q_exe_studycountry_xl092-304') }} sc
    left join(select * from {{ source ('koios_raw', 'q_exe_site_xl092-304') }} s
    where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_site_xl092-304') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_site_xl092-304') }})
    ) as s
    on sc.study_country_source_id = s.study_country_source_id
    where sc.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-304') }})
    and sc.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-304') }})

union
select distinct
cast(sc.clinical_study_source_id as varchar) as  study_protocol_number
,null as  study_country_id
,case
        when upper(sc.study_region) = 'KOREA, REPUBLIC OF' then 'SOUTH KOREA'
        when upper(sc.study_region) = 'UNITED STATES OF AMERICA' then 'UNITED STATES'
        when upper(sc.study_region) = 'TAIWAN, REPUBLIC OF CHINA' then 'TAIWAN' 
        else upper(sc.study_region)
end as country_name
,case 
    when COUNT(case when s.site_status in('Enrollment Open','Enrollment On-Hold') or s.site_status = 'Enrollment Closed'  then 1 end)
        over(partition by sc.study_country_source_id) > 0 then 'Active'
    when COUNT( case when s.site_status = 'Selected' then 1 end ) 
        over(partition by sc.study_country_source_id) > 0 then 'Approved'
	when COUNT ( case when s.site_status in('Closed') then 1 end)
        over(partition by sc.study_country_source_id) > 0 then 'Enrollment Closed'
    when COUNT ( case when s.site_status = 'Dropped' then 1 end)
        over(partition by sc.study_country_source_id) > 0 then 'Terminated'
    else coalesce(s.site_status, sc.study_region_status)
end as country_status
,sc.partition_date as load_date
,sc.last_modified_date
,md5(sc.clinical_study_source_id||sc.study_country_source_id) as hash_key
FROM
    {{ source ('koios_raw', 'q_exe_studycountry_xl092-305') }} sc
    left join(select * from {{ source ('koios_raw', 'q_exe_site_xl092-305') }} s
    where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_site_xl092-305') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_site_xl092-305') }})
    ) as s
    on sc.study_country_source_id = s.study_country_source_id
    where sc.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-305') }})
    and sc.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-305') }})

union
select distinct
cast(sc.clinical_study_source_id as varchar) as  study_protocol_number
,null as  study_country_id
,case
        when upper(sc.study_region) = 'KOREA, REPUBLIC OF' then 'SOUTH KOREA'
        when upper(sc.study_region) = 'UNITED STATES OF AMERICA' then 'UNITED STATES'
        when upper(sc.study_region) = 'TAIWAN, REPUBLIC OF CHINA' then 'TAIWAN' 
        else upper(sc.study_region)
end as country_name
,case 
    when COUNT(case when s.site_status in('Enrollment Open','Enrollment On-Hold') or s.site_status = 'Enrollment Closed'  then 1 end)
        over(partition by sc.study_country_source_id) > 0 then 'Active'
    when COUNT( case when s.site_status = 'Selected' then 1 end ) 
        over(partition by sc.study_country_source_id) > 0 then 'Approved'
	when COUNT ( case when s.site_status in('Closed') then 1 end)
        over(partition by sc.study_country_source_id) > 0 then 'Enrollment Closed'
    when COUNT ( case when s.site_status = 'Dropped' then 1 end)
        over(partition by sc.study_country_source_id) > 0 then 'Terminated'
    else coalesce(s.site_status, sc.study_region_status)
end as country_status
,sc.partition_date as load_date
,sc.last_modified_date
,md5(sc.clinical_study_source_id||sc.study_country_source_id) as hash_key
FROM
    {{ source ('koios_raw', 'q_exe_studycountry_xl092-009') }} sc
    left join(select * from {{ source ('koios_raw', 'q_exe_site_xl092-009') }} s
    where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_site_xl092-009') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_site_xl092-009') }})
    ) as s
    on sc.study_country_source_id = s.study_country_source_id
    where sc.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-009') }})
    and sc.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-009') }})

union
select distinct 
study_id  as study_protocol_number
,null as study_country_id
,case when country = 'AUS' then 'AUSTRALIA' 
         when country = 'SPN' then  'SPAIN'
         when country = 'SKOR' then 'SOUTH KOREA'
         when country = 'USA' then 'UNITED STATES'
         when country = 'UK' then 'UNITED KINGDOM'
         when country = 'FRA' then 'FRANCE'
         when country = 'ITA' then 'ITALY'
         else country
    end as country_name
,case 
	when country in (select distinct country
        FROM 
        {{ source ('koios_raw', 'site_contact_details_actual_xl309-101') }}
            where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl309-101') }})
            and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl309-101') }})
            and "site status"  = 'Site Activated') then 'Active'
		else 'Approved' end
    as country_status
,partition_date as load_date
,last_modified_date
,md5(study_id||md5(country)) as hash_key
from {{ source ('koios_raw', 'site_contact_details_actual_xl309-101') }}
where country is not null
and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl309-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl309-101') }})

union all

select distinct 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,REPLACE(country_name, 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xb628-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xb628-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xb628-101') }})

union all

select distinct 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,upper(REPLACE(country_name, 'UNITED STATES OF AMERICA', 'UNITED STATES')) AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,last_modified_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xl092-311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl092-311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl092-311') }})

union all

select distinct 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,case when country_name='KOREA, REPUBLIC OF' then 'SOUTH KOREA'
     when country_name='UNITED STATES OF AMERICA' then 'UNITED STATES'
     else country_name
end as country_name
--REPLACE(country_name, 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,cast(cast(last_modified_date as text) as timestamp) as last_modified_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xl184_313') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl184_313') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl184_313') }})

union all

select distinct 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,case when country_name='KOREA, REPUBLIC OF' then 'SOUTH KOREA'
     when country_name='UNITED STATES OF AMERICA' then 'UNITED STATES'
     else country_name
end as country_name
--REPLACE(country_name, 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(country_status as varchar) as  country_status
,partition_date as load_date
,cast(cast(last_modified_date as text) as timestamp) as last_modified_date
,md5(study_protocol_number||study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xl184_401') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl184_401') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl184_401') }})
union all

select distinct 
cast(sc.study_protocol_number as varchar) as study_protocol_number
,cast(sc.study_country_id as varchar) as study_country_id
,case when upper(sc.country_name)='KOREA  REPUBLIC OF' then 'SOUTH KOREA'
     when upper(sc.country_name)='UNITED STATES OF AMERICA' then 'UNITED STATES'
     when upper(sc.country_name)='TAIWAN  PROVINCE OF CHINA' then 'TAIWAN'
     else upper(sc.country_name)
end as country_name
,case when sc.country_status is null then
    case when COUNT(case when s.study_site_status in('Activated','Selected','Closed','Approved','Submitted','Withdrawn','Eligible for Activation')  then 1 end)
        over(partition by sc.study_country_id) > 0 then 'Active'
        else 'Planned'
    END
        else sc.country_status
    END AS country_status
,sc.partition_date as load_date
,sc.last_modified_date
,md5(sc.study_protocol_number||sc.study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xb371-101') }} sc
    left join(select * from {{ source ('koios_raw', 'pra_exl_study_site_xb371-101') }} s where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xb371-101') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xb371-101') }})
    ) as s
    on sc.study_country_id = s.study_country_id
    where sc.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xb371-101') }})
    and sc.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xb371-101') }})

union all
select distinct 
study_id as study_protocol_number
,null as study_country_id
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,case 
	when country in (select distinct country
        FROM 
        {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }}
            where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }})
            and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }})
            and "site activation actual date" is not null) then 'Active'
		else 'Approved' end
    as country_status
,partition_date as load_date
,last_modified_date
,md5(study_id||md5(country)) as hash_key
from {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }}
where country is not null
and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }})

union all
select distinct 
cast(sc.study_protocol_number as varchar) as study_protocol_number
,cast(sc.study_country_id as varchar) as study_country_id
,case when upper(sc.country_name)='KOREA  REPUBLIC OF' then 'SOUTH KOREA'
     when upper(sc.country_name)='UNITED STATES OF AMERICA' then 'UNITED STATES'
     when upper(sc.country_name)='TAIWAN  PROVINCE OF CHINA' then 'TAIWAN'
     else upper(sc.country_name)
end as country_name
,case when sc.country_status is null then
    case when COUNT(case when s.study_site_status in('Activated','Selected','Closed','Approved','Submitted','Withdrawn','Eligible for Activation')  then 1 end)
        over(partition by sc.study_country_id) > 0 then 'Active'
        else 'Planned'
    END
        else sc.country_status
    END AS country_status
,sc.partition_date as load_date
,sc.last_modified_date
,md5(sc.study_protocol_number||sc.study_country_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_country_xl092-202') }} sc
    left join(select * from {{ source ('koios_raw', 'pra_exl_study_site_xl092-202') }} s where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092-202') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092-202') }})
    ) as s
    on sc.study_country_id = s.study_country_id
    where sc.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl092-202') }})
    and sc.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_country_xl092-202') }})



union all 
select distinct 
study_id as study_protocol_number
,null as study_country_id
,REPLACE(upper("pi - country"), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,case 
	when "pi - country" in (select distinct "pi - country"
        FROM 
        {{ source ('koios_raw', 'site_information_report_xl092-011') }}
            where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})
            and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})
            and "startup - activation date" is not null) then 'Active'
		else 'Approved' end
    as country_status
,partition_date as load_date
,last_modified_date
,md5(study_id||md5("pi - country")) as hash_key
from {{ source ('koios_raw', 'site_information_report_xl092-011') }}
where "pi - country" is not null
and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})

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
,last_modified_date
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
,sc.last_modified_date	
,sc.hash_key
from ctms_stg__study_country sc
LEFT join country_region_mapping rm 
on sc.country_name = rm.country_name
where sc.study_country_id is null
