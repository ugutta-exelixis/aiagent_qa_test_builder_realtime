
with ctms_stg__address_association as (


select 
study_protocol_number
,cast(address_id as varchar) as address_id
,associated_dataset
,cast(associated_record_id as varchar) as associated_record_id
,type
,partition_date as load_date
,md5(study_protocol_number||address_id||associated_record_id||type) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_address_association_xb002_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_address_association_xb002_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_address_association_xb002_101') }})

union all

select 
study_protocol_number
,cast(address_id as varchar) as address_id
,associated_dataset
,cast(associated_record_id as varchar) as associated_record_id
,type
,partition_date as load_date
,md5(study_protocol_number||address_id||associated_record_id||type) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_address_association_xl092-002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl092-002') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl092-002') }})

union all

select 
study_protocol_number
,cast(address_id as varchar) as address_id
,associated_dataset
,cast(associated_record_id as varchar) as associated_record_id
,type
,partition_date as load_date
,md5(study_protocol_number||address_id||associated_record_id||type) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_address_association_xl092-303') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl092-303') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl092-303') }})

union all

select 
study_protocol_number
,cast(address_id as varchar) as address_id
,associated_dataset
,cast(associated_record_id as varchar) as associated_record_id
,type
,partition_date as load_date
,md5(study_protocol_number||address_id||associated_record_id||type) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_address_association_xl092_001') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl092_001') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl092_001') }})

union all

select 
study_protocol_number
,cast(address_id as varchar) as address_id
,associated_dataset
,cast(associated_record_id as varchar) as associated_record_id
,type
,partition_date as load_date
,md5(study_protocol_number||address_id||associated_record_id||type) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_address_association_xl102_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl102_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl102_101') }})

union all

select 
study_protocol_number
,cast(address_id as varchar) as address_id
,associated_dataset
,cast(associated_record_id as varchar) as associated_record_id
,type
,partition_date as load_date
,md5(study_protocol_number||address_id||associated_record_id||type) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_address_association_xl114-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl114-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl114-101') }})

union all

select 
study_protocol_number
,cast(address_id as varchar) as address_id
,associated_dataset
,cast(associated_record_id as varchar) as associated_record_id
,type
,partition_date as load_date
,md5(study_protocol_number||address_id||associated_record_id||type) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_address_association_xl184_021') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl184_021') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl184_021') }})

union all

select 
study_protocol_number
,cast(address_id as varchar) as address_id
,associated_dataset
,cast(associated_record_id as varchar) as associated_record_id
,type
,partition_date as load_date
,md5(study_protocol_number||address_id||associated_record_id||type) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_address_association_xl184_311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl184_311') }})
     and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl184_311') }})

union all

select 
study_protocol_number
,cast(address_id as varchar) as address_id
,associated_dataset
,cast(associated_record_id as varchar) as associated_record_id
,type
,partition_date as load_date
,md5(study_protocol_number||address_id||associated_record_id||type) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_address_association_xl184_312') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl184_312') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl184_312') }})
union all

select 
study_protocol_number
,cast(address_id as varchar) as address_id
,associated_dataset
,cast(associated_record_id as varchar) as associated_record_id
,type
,partition_date as load_date
,md5(study_protocol_number||address_id||associated_record_id||type) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_address_association_xl184_315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl184_315') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_address_association_xl184_315') }})
union all

select 
study_protocol_number
,cast(address_id as varchar) as address_id
,associated_dataset
,cast(associated_record_id as varchar) as associated_record_id
,type
,partition_date as load_date
,md5(study_protocol_number||address_id||associated_record_id||type) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_address_association_xb010-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_address_association_xb010-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_address_association_xb010-101') }})
    )
	
	select * from ctms_stg__address_association
    