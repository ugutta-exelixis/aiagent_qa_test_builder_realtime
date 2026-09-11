with ctms_stg__study as (
select distinct
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_title as varchar) as study_title
,cast(phase as varchar) as phase
,cast(therapeutic_area as varchar) as therapeutic_area
,cast(study_type as varchar) as study_type
,cast(unblinded as varchar) as unblinded
,cast(study_drug as varchar) as study_drug
,partition_date as load_date
,md5(study_protocol_number||study_title||phase||therapeutic_area) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'pra_exl_study_xb002-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_xb002-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_xb002-101') }})

UNION all

select distinct
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_title as varchar) as study_title
,cast(phase as varchar) as phase
,cast(therapeutic_area as varchar) as therapeutic_area
,cast(study_type as varchar) as study_type
,cast(unblinded as varchar) as unblinded
,cast(study_drug as varchar) as study_drug
,partition_date as load_date
,md5(study_protocol_number||study_title||phase||therapeutic_area) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'pra_exl_study_xl092-002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_xl092-002') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_xl092-002') }})

UNION all

select distinct
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_title as varchar) as study_title
,cast(phase as varchar) as phase
,cast(therapeutic_area as varchar) as therapeutic_area
,cast(study_type as varchar) as study_type
,cast(unblinded as varchar) as unblinded
,cast(study_drug as varchar) as study_drug
,partition_date as load_date
,md5(study_protocol_number||study_title||phase||therapeutic_area) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'pra_exl_study_xl092-303') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_xl092-303') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_xl092-303') }})

UNION all

select distinct
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_title as varchar) as study_title
,cast(phase as varchar) as phase
,cast(therapeutic_area as varchar) as therapeutic_area
,cast(study_type as varchar) as study_type
,cast(unblinded as varchar) as unblinded
,cast(study_drug as varchar) as study_drug
,partition_date as load_date
,md5(study_protocol_number||study_title||phase||therapeutic_area) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'pra_exl_study_xl092_001') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_xl092_001') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_xl092_001') }})

UNION all

select distinct
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_title as varchar) as study_title
,cast(phase as varchar) as phase
,cast(therapeutic_area as varchar) as therapeutic_area
,cast(study_type as varchar) as study_type
,cast(unblinded as varchar) as unblinded
,cast(study_drug as varchar) as study_drug
,partition_date as load_date
,md5(study_protocol_number||study_title||phase||therapeutic_area) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'pra_exl_study_xl102_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_xl102_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_xl102_101') }})

UNION all

select distinct
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_title as varchar) as study_title
,cast(phase as varchar) as phase
,cast(therapeutic_area as varchar) as therapeutic_area
,cast(study_type as varchar) as study_type
,cast(unblinded as varchar) as unblinded
,cast(study_drug as varchar) as study_drug
,partition_date as load_date
,md5(study_protocol_number||study_title||phase||therapeutic_area) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'pra_exl_study_xl114-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_xl114-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_xl114-101') }})

UNION all

select distinct
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_title as varchar) as study_title
,cast(phase as varchar) as phase
,cast(therapeutic_area as varchar) as therapeutic_area
,cast(study_type as varchar) as study_type
,cast(unblinded as varchar) as unblinded
,cast(study_drug as varchar) as study_drug
,partition_date as load_date
,md5(study_protocol_number||study_title||phase||therapeutic_area) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'pra_exl_study_xl184_021') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_xl184_021') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_xl184_021') }})

UNION all

select distinct
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_title as varchar) as study_title
,cast(phase as varchar) as phase
,cast(therapeutic_area as varchar) as therapeutic_area
,cast(study_type as varchar) as study_type
,cast(unblinded as varchar) as unblinded
,cast(study_drug as varchar) as study_drug
,partition_date as load_date
,md5(study_protocol_number||study_title||phase||therapeutic_area) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'pra_exl_study_xl184_311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_xl184_311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_xl184_311') }})

UNION all

select distinct
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_title as varchar) as study_title
,cast(phase as varchar) as phase
,cast(therapeutic_area as varchar) as therapeutic_area
,cast(study_type as varchar) as study_type
,cast(unblinded as varchar) as unblinded
,cast(study_drug as varchar) as study_drug
,partition_date as load_date
,md5(study_protocol_number||study_title||phase||therapeutic_area) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'pra_exl_study_xl184_312') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_xl184_312') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_xl184_312') }})

UNION all

select distinct
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_title as varchar) as study_title
,cast(phase as varchar) as phase
,cast(therapeutic_area as varchar) as therapeutic_area
,cast(study_type as varchar) as study_type
,cast(unblinded as varchar) as unblinded
,cast(study_drug as varchar) as study_drug
,partition_date as load_date
,md5(study_protocol_number||study_title||phase||therapeutic_area) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'pra_exl_study_xl184_315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_xl184_315') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_xl184_315') }})

union all

select distinct
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_title as varchar) as study_title
,cast(phase as varchar) as phase
,cast(therapeutic_area as varchar) as therapeutic_area
,cast(study_type as varchar) as study_type
,cast(unblinded as varchar) as unblinded
,cast(study_drug as varchar) as study_drug
,partition_date as load_date
,md5(study_protocol_number||study_title||phase||therapeutic_area) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'manual_exl_study_xl092-009') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'manual_exl_study_xl092-009') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'manual_exl_study_xl092-009') }})

)

select * from ctms_stg__study
