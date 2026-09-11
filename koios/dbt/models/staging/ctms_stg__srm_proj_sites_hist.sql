with country_region_mapping as 
(
    select distinct
    country as country_name
    ,cast(1000+dense_rank() over (order by country) as varchar) as study_country_id
    ,region as country_region
    from {{ ref('stg_country_region_mapping') }}
),
ctms_stg__srm_proj_sites_hist as (
select 
cast(study_protocol_number as varchar) as  study_protocol_number
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_sites
,partition_date as load_date
,md5(study_protocol_number||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xb002_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xb002_101') }})
    and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xb002_101') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw','pra_exl_srm_proj_sites_xb002_101') }})

union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_sites
,partition_date as load_date
,md5(study_protocol_number||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092_001') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092_001') }})
    and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092_001') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092_001') }})

union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_sites
,partition_date as load_date
,md5(study_protocol_number||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092_002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092_002') }})
    and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092_002') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092_002') }})

union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_sites
,partition_date as load_date
,md5(study_protocol_number||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092_303') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092_303') }})
    and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092_303') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092_303') }})

union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_sites
,partition_date as load_date
,md5(study_protocol_number||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl102_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl102_101') }})
    and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl102_101') }})
   --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl102_101') }})

union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_sites
,partition_date as load_date 
,md5(study_protocol_number||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl114_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl114_101') }})
    and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl114_101') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl114_101') }})

union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_sites
,partition_date as load_date
,md5(study_protocol_number||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl184_021') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl184_021') }})
    and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl184_021') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl184_021') }})

union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_sites
,partition_date as load_date 
,md5(study_protocol_number||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl184_311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl184_311') }})
    and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl184_311') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl184_311') }})

union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_sites
,partition_date as load_date 
,md5(study_protocol_number||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl184_312') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl184_312') }})
    and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl184_312') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl184_312') }})

union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_sites
,partition_date as load_date
,md5(study_protocol_number||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl184_315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl184_315') }})
    and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl184_315') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl184_315') }})
union all

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_sites
,partition_date as load_date
,md5(study_protocol_number||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xb010-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xb010-101') }})
    and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xb010-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xb010-101') }}))
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xb010-101') }})
union all

select distinct
cast(scm.clinical_study_source_id as varchar) as  study_protocol_number
-- ,REPLACE(upper(sc.study_region), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,case
    when upper(sc.study_region) = 'UNITED STATES OF AMERICA' then 'UNITED STATES'
	when upper(sc.study_region) = 'KOREA, REPUBLIC OF' then 'SOUTH KOREA'
	when upper(sc.study_region) = 'TAIWAN, REPUBLIC OF CHINA' then 'TAIWAN' 
	else upper(sc.study_region)
end as country_name
,cast(scm.metric_date as varchar) as  month_end_date
,scm.metric_value as planned_sites
,scm.partition_date as load_date
,md5(scm.clinical_study_source_id||sc.study_region||scm.metric_date) as hash_key
FROM
    {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-304') }} scm
    left join
    (select * from {{ source ('koios_raw', 'q_exe_studycountry_xl092-304') }}sc 
    where sc.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-304') }})
    and sc.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-304') }})) as sc
    on scm.study_country_source_id = sc.study_country_source_id 
    where scm.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-304') }})
    and cast(scm.metric_date as date) = (select max(cast(metric_date as date)) from {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-304') }}
    where scm.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-304') }})
    and scm.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-304') }})
    and metric_type = 'Projected'
    and metric_name = 'Planned Sites Open')
    and scm.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-304') }})
    and scm.metric_name = 'Planned Sites Open'

union all
select distinct
cast(scm.clinical_study_source_id as varchar) as  study_protocol_number
-- ,REPLACE(upper(sc.study_region), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,case
    when upper(sc.study_region) = 'UNITED STATES OF AMERICA' then 'UNITED STATES'
	when upper(sc.study_region) = 'KOREA, REPUBLIC OF' then 'SOUTH KOREA'
	when upper(sc.study_region) = 'TAIWAN, REPUBLIC OF CHINA' then 'TAIWAN' 
	else upper(sc.study_region)
end as country_name
,cast(scm.metric_date as varchar) as  month_end_date
,scm.metric_value as planned_sites
,scm.partition_date as load_date
,md5(scm.clinical_study_source_id||sc.study_region||scm.metric_date) as hash_key
FROM
    {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-305') }} scm
    left join 
    (select * from {{ source ('koios_raw', 'q_exe_studycountry_xl092-305') }} sc 
    where sc.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-305') }})
    and sc.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-305') }})) as sc
    on scm.study_country_source_id = sc.study_country_source_id 
    where scm.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-305') }} scm)
    and cast(scm.metric_date as date) = (select max(cast(metric_date as date)) from {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-305') }} scm
    where scm.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-305') }} scm)
    and scm.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-305') }} scm)
    and metric_type = 'Projected'
    and metric_name = 'Planned Sites Open')
    and scm.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-305') }} scm)
    and scm.metric_name = 'Planned Sites Open'
union all
select distinct
cast(scm.clinical_study_source_id as varchar) as  study_protocol_number
-- ,REPLACE(upper(sc.study_region), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,case
    when upper(sc.study_region) = 'UNITED STATES OF AMERICA' then 'UNITED STATES'
	when upper(sc.study_region) = 'KOREA, REPUBLIC OF' then 'SOUTH KOREA'
	when upper(sc.study_region) = 'TAIWAN, REPUBLIC OF CHINA' then 'TAIWAN' 
	else upper(sc.study_region)
end as country_name
,cast(scm.metric_date as varchar) as  month_end_date
,scm.metric_value as planned_sites
,scm.partition_date as load_date
,md5(scm.clinical_study_source_id||sc.study_region||scm.metric_date) as hash_key
FROM
    {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-009') }} scm
    left join (select * from {{ source ('koios_raw', 'q_exe_studycountry_xl092-009') }} sc 
    where sc.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-009') }})
    and sc.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-009') }})) as sc
    on scm.study_country_source_id = sc.study_country_source_id 
    where scm.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-009') }})
    and cast(scm.metric_date as date) = (select max(cast(metric_date as date)) from {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-009') }}
    where scm.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-009') }})
    and scm.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-009') }})
    and metric_type = 'Projected'
    and metric_name = 'Planned Sites Open')
    and scm.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studycountrymetricbymo_xl092-009') }})
    and scm.metric_name = 'Planned Sites Open'
union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_sites
,partition_date as load_date
,md5(study_protocol_number||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xb628-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xb628-101') }})
    and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xb628-101') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092_303') }})

union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,cast (planned_sites as integer) as planned_sites
,partition_date as load_date
,md5(study_protocol_number||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092-311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092-311') }})
    and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092-311') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092_303') }})

union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,cast (planned_sites as integer) as planned_sites
,partition_date as load_date
,md5(study_protocol_number||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xb371-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xb371-101') }})
    and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xb371-101') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xb371-101') }})


union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,cast (planned_sites as integer) as planned_sites
,partition_date as load_date
,md5(study_protocol_number||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092-202') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092-202') }})
    and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092-202') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_proj_sites_xl092-202') }})


)

select * from ctms_stg__srm_proj_sites_hist