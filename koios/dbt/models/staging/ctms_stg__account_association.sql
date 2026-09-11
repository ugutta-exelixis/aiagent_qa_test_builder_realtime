with ctms_stg__account_association as 
(
select 
study_protocol_number
,account_id
,associated_dataset
,associated_record_id
,address_id
,type
,active_flag
,partition_date as load_date
,md5(study_protocol_number||account_id||associated_record_id||active_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_account_association_xb002_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account_association_xb002_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account_association_xb002_101') }})

union all

select 
study_protocol_number
,account_id
,associated_dataset
,associated_record_id
,address_id
,type
,active_flag
,partition_date as load_date
,md5(study_protocol_number||account_id||associated_record_id||active_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_account_association_xl092-002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl092-002') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl092-002') }})

union all

select 
study_protocol_number
,account_id
,associated_dataset
,associated_record_id
,address_id
,type
,active_flag
,partition_date as load_date
,md5(study_protocol_number||account_id||associated_record_id||active_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_account_association_xl092-303') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl092-303') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl092-303') }})

union all

select 
study_protocol_number
,account_id
,associated_dataset
,associated_record_id
,address_id
,type
,active_flag
,partition_date as load_date
,md5(study_protocol_number||account_id||associated_record_id||active_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_account_association_xl092_001') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl092_001') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl092_001') }})

union all

select 
study_protocol_number
,account_id
,associated_dataset
,cast(associated_record_id as varchar) as associated_record_id
,address_id
,type
,active_flag
,partition_date as load_date
,md5(study_protocol_number||account_id||associated_record_id||active_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_account_association_xl102_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl102_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl102_101') }})
     

union all

select 
study_protocol_number
,cast(account_id as varchar) as account_id
,associated_dataset
,cast(associated_record_id as varchar) as associated_record_id
,address_id
,type
,active_flag
,partition_date as load_date
,md5(study_protocol_number||account_id||associated_record_id||active_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_account_association_xl114-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl114-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl114-101') }})

union all

select 
study_protocol_number
,cast(account_id as varchar) as account_id
,associated_dataset
,cast(associated_record_id as varchar) as associated_record_id
,address_id
,type
,active_flag
,partition_date as load_date
,md5(study_protocol_number||account_id||associated_record_id||active_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_account_association_xl184_021') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl184_021') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl184_021') }})

union all

select 
study_protocol_number
,account_id
,associated_dataset
,associated_record_id
,address_id
,type
,active_flag
,partition_date as load_date
,md5(study_protocol_number||account_id||associated_record_id||active_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_account_association_xl184_311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl184_311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl184_311') }})

union all

select 
study_protocol_number
,account_id
,associated_dataset
,associated_record_id
,address_id
,type
,active_flag
,partition_date as load_date
,md5(study_protocol_number||account_id||associated_record_id||active_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_account_association_xl184_312') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl184_312') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl184_312') }})

union all

select 
study_protocol_number
,account_id
,associated_dataset
,associated_record_id
,address_id
,type
,active_flag
,partition_date as load_date
,md5(study_protocol_number||account_id||associated_record_id||active_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_account_association_xl184_315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl184_315') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl184_315') }})

union all

select 
study_protocol_number
,account_id
,associated_dataset
,associated_record_id::text
,address_id
,type
,active_flag
,partition_date as load_date
,md5(study_protocol_number||account_id||associated_record_id||active_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_account_association_xb010-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account_association_xb010-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account_association_xb010-101') }})
union all

select 
study_protocol_number
,account_id
,associated_dataset
,associated_record_id::text
,address_id
,type
,active_flag
,partition_date as load_date
,md5(study_protocol_number||account_id||associated_record_id||active_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_account_association_xl495-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl495-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl495-101') }})

union all

select 
clinical_study_source_id as study_protocol_number
,account_source_id as account_id
,null as associated_dataset
,cast (study_site_source_id as text)as associated_record_id
,null as address_id
,null as type
,primary_account_flag as active_flag
,partition_date as load_date
,md5(clinical_study_source_id||account_source_id||''||primary_account_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'q_exe_siteaccount_xl092-304') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_siteaccount_xl092-304') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_siteaccount_xl092-304') }})

union all

select 
clinical_study_source_id as study_protocol_number
,account_source_id as account_id
,null as associated_dataset
,cast (study_site_source_id as text)as associated_record_id
,null as address_id
,null as type
,primary_account_flag as active_flag
,partition_date as load_date
,md5(clinical_study_source_id||account_source_id||''||primary_account_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'q_exe_siteaccount_xl092-305') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_siteaccount_xl092-305') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_siteaccount_xl092-305') }})
union all

select 
clinical_study_source_id as study_protocol_number
,account_source_id as account_id
,null as associated_dataset
,cast (study_site_source_id as text)as associated_record_id
,null as address_id
,null as type
,primary_account_flag as active_flag
,partition_date as load_date
,md5(clinical_study_source_id||account_source_id||''||primary_account_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'q_exe_siteaccount_xl092-009') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_siteaccount_xl092-009') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_siteaccount_xl092-009') }})


union all

select 
study_protocol_number
,account_id
,associated_dataset
,associated_record_id::text
,address_id
,type
,active_flag
,partition_date as load_date
,md5(study_protocol_number||account_id||associated_record_id||active_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_account_association_xl495-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl495-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl495-101') }})
union all

select 
study_protocol_number
,account_id
,associated_dataset
,associated_record_id::text
,address_id
,type
,active_flag
,partition_date as load_date
,md5(study_protocol_number||account_id||associated_record_id||active_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_account_association_xb628-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account_association_xb628-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account_association_xb628-101') }})

union all

select 
study_protocol_number
,account_id
,associated_dataset
,associated_record_id::text
,address_id
,type
,active_flag
,partition_date as load_date
,md5(study_protocol_number||account_id||associated_record_id||active_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_account_association_xl092-311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl092-311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl092-311') }})

union all

select 
study_protocol_number
,account_id
,associated_dataset
,associated_record_id::text
,address_id
,type
,active_flag
,partition_date as load_date
,md5(study_protocol_number||account_id||associated_record_id||active_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_account_association_xb371-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account_association_xb371-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account_association_xb371-101') }})


union all

select 
study_protocol_number
,account_id
,associated_dataset
,associated_record_id::text
,address_id
,type
,active_flag
,partition_date as load_date
,md5(study_protocol_number||account_id||associated_record_id||active_flag) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_account_association_xl092-202') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl092-202') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account_association_xl092-202') }})
)

select * from ctms_stg__account_association
