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

-- select distinct
-- cast(study_protocol_number as varchar) as study_protocol_number
-- ,cast(study_title as varchar) as study_title
-- ,cast(phase as varchar) as phase
-- ,cast(therapeutic_area as varchar) as therapeutic_area
-- ,cast(study_type as varchar) as study_type
-- ,cast(unblinded as varchar) as unblinded
-- ,cast(study_drug as varchar) as study_drug
-- ,partition_date as load_date
-- ,md5(study_protocol_number||study_title||phase||therapeutic_area) as hash_key
-- ,last_modified_date as PRA_Data_Refreshed_Date
-- FROM
--     {{ source ('koios_raw', 'manual_exl_study_xl092-009') }}
--     where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'manual_exl_study_xl092-009') }})
--     and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'manual_exl_study_xl092-009') }})

-- union all

select distinct
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_title as varchar) as study_title
,cast(phase as varchar) as phase
,'Solid Tumors' as therapeutic_area
,cast(study_type as varchar) as study_type
,cast(unblinded as varchar) as unblinded
,cast(study_drug as varchar) as study_drug
,partition_date as load_date
,md5(study_protocol_number||study_title||phase||therapeutic_area) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'pra_exl_study_xb010-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_xb010-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_xb010-101') }})


union all

select distinct
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_title as varchar) as study_title
,cast(phase as varchar) as phase
,'Solid Tumors' as therapeutic_area
,cast(study_type as varchar) as study_type
,cast(unblinded as varchar) as unblinded
,cast(study_drug as varchar) as study_drug
,partition_date as load_date
,md5(study_protocol_number||study_title||phase||therapeutic_area) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'pra_exl_study_xl495-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_xl495-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_xl495-101') }})
UNION all
select distinct
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_title as varchar) as study_title
,cast(phase as varchar) as phase
,'Solid Tumors' as therapeutic_area
,cast(study_type as varchar) as study_type
,cast(unblinded as varchar) as unblinded
,cast(study_drug as varchar) as study_drug
,partition_date as load_date
,md5(study_protocol_number||study_title||phase||therapeutic_area) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'pra_exl_study_xb628-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_xb628-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_xb628-101') }})

UNION all

select distinct
cast(clinical_study_source_id as varchar) as study_protocol_number
,cast(protocol_title as varchar) as study_title
,'Phase 3' as phase
,'Renal Cell Carcinoma' as therapeutic_area
,'Blinded' as study_type
,null as unblinded
,'XL092' as study_drug
,partition_date as load_date
,md5(clinical_study_source_id||protocol_title||'Phase 3'||'Renal Cell Carcinoma') as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'q_exe_study_xl092-304') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_study_xl092-304') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_study_xl092-304') }})
UNION all

select distinct
cast(clinical_study_source_id as varchar) as study_protocol_number
,cast(protocol_title as varchar) as study_title
,'Phase 3' as phase
,'Head and Neck Squamous Cell Cancer' as therapeutic_area
,'Blinded' as study_type
,null as unblinded
,'XL092' as study_drug
,partition_date as load_date
,md5(clinical_study_source_id||protocol_title||'Phase 3'||'Head and Neck Squamous Cell Cancer') as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'q_exe_study_xl092-305') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_study_xl092-305') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_study_xl092-305') }})

UNION all

select distinct
cast(clinical_study_source_id as varchar) as study_protocol_number
,cast(protocol_title as varchar) as study_title
,'Phase 1' as phase
,'Solid Tumors' as therapeutic_area
,'Open Label' as study_type
,null as unblinded
,'XL092' as study_drug
,partition_date as load_date
,md5(clinical_study_source_id||protocol_title||'Phase 1'||'Solid Tumors') as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'q_exe_study_xl092-009') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_study_xl092-009') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_study_xl092-009') }})

UNION all

select distinct
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_title as varchar) as study_title
,cast(phase as varchar) as phase
,'Solid Tumors' as therapeutic_area
,cast(study_type as varchar) as study_type
,cast(unblinded as varchar) as unblinded
,cast(study_drug as varchar) as study_drug
,partition_date as load_date
,md5(study_protocol_number||study_title||phase||therapeutic_area) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'pra_exl_study_xl092-311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_xl092-311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_xl092-311') }})

union all
select distinct
cast(study_protocol_number as varchar) as study_protocol_number
,null as study_title
,null as phase
,null as therapeutic_area
,null as study_type
,null as unblinded
,null as study_drug
,partition_date as load_date
,md5(study_protocol_number||null||null||null) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'pra_exl_study_xb371-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_xb371-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_xb371-101') }})


union all
select distinct
cast(study_id as varchar) as study_protocol_number
,null as study_title
,null as phase
,null as therapeutic_area
,null as study_type
,null as unblinded
,null as study_drug
,partition_date as load_date
,md5(study_id||null||null||null) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }})

 
union all
select distinct
cast(study_id as varchar) as study_protocol_number
,null as study_title
,null as phase
,null as therapeutic_area
,null as study_type
,null as unblinded
,null as study_drug
,partition_date as load_date
,md5(study_id||null||null||null) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'site_information_report_xl092-011') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})


UNION all

select distinct
cast(study_protocol_number as varchar) as study_protocol_number
,null as study_title
,null as phase
,null as therapeutic_area
,null as study_type
,null as unblinded
,null as study_drug
,partition_date as load_date
,md5(study_protocol_number||null||null||null) as hash_key
,last_modified_date as PRA_Data_Refreshed_Date
FROM
    {{ source ('koios_raw', 'pra_exl_study_xl092-202') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_xl092-202') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_xl092-202') }})


)


select * from ctms_stg__study