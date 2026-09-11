with ctms_stg__srm_proj_sites_hist as (
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
)

select * from ctms_stg__srm_proj_sites_hist