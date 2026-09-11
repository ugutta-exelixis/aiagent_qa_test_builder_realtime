with ctms_stg__contact_association as (
select 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(contact_id as varchar) as  contact_id
,cast(associated_dataset as varchar) as  associated_dataset
,cast(associated_record_id as varchar) as  associated_record_id
,cast(address_id as varchar) as  address_id
,cast(account_id as varchar) as  account_id
,cast(role as varchar) as  role
,cast(start_date as varchar) as  start_date
,cast(end_date as varchar) as  end_date
,partition_date as load_date
,md5(study_protocol_number||contact_id||associated_dataset||associated_record_id||start_date||coalesce(role,'role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_contact_association_xb002_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xb002_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xb002_101') }})

union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(contact_id as varchar) as  contact_id
,cast(associated_dataset as varchar) as  associated_dataset
,cast(associated_record_id as varchar) as  associated_record_id
,cast(address_id as varchar) as  address_id
,cast(account_id as varchar) as  account_id
,cast(role as varchar) as  role
,cast(start_date as varchar) as  start_date
,cast(end_date as varchar) as  end_date
,partition_date as load_date
,md5(study_protocol_number||contact_id||associated_dataset||associated_record_id||start_date||coalesce(role,'role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_contact_association_xl092-002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl092-002') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl092-002') }})
	
union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(contact_id as varchar) as  contact_id
,cast(associated_dataset as varchar) as  associated_dataset
,cast(associated_record_id as varchar) as  associated_record_id
,cast(address_id as varchar) as  address_id
,cast(account_id as varchar) as  account_id
,cast(role as varchar) as  role
,cast(start_date as varchar) as  start_date
,cast(end_date as varchar) as  end_date
,partition_date as load_date
,md5(study_protocol_number||contact_id||associated_dataset||associated_record_id||start_date||coalesce(role,'role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_contact_association_xl092-303') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl092-303') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl092-303') }})
	
union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(contact_id as varchar) as  contact_id
,cast(associated_dataset as varchar) as  associated_dataset
,cast(associated_record_id as varchar) as  associated_record_id
,cast(address_id as varchar) as  address_id
,cast(account_id as varchar) as  account_id
,cast(role as varchar) as  role
,cast(start_date as varchar) as  start_date
,cast(end_date as varchar) as  end_date
,partition_date as load_date
,md5(study_protocol_number||contact_id||associated_dataset||associated_record_id||start_date||coalesce(role,'role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_contact_association_xl092_001') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl092_001') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl092_001') }})
	
union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(contact_id as varchar) as  contact_id
,cast(associated_dataset as varchar) as  associated_dataset
,cast(associated_record_id as varchar) as  associated_record_id
,cast(address_id as varchar) as  address_id
,cast(account_id as varchar) as  account_id
,cast(role as varchar) as  role
,cast(start_date as varchar) as  start_date
,cast(end_date as varchar) as  end_date
,partition_date as load_date
,md5(study_protocol_number||contact_id||associated_dataset||associated_record_id||start_date||coalesce(role,'role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_contact_association_xl102_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl102_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl102_101') }})
	
union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(contact_id as varchar) as  contact_id
,cast(associated_dataset as varchar) as  associated_dataset
,cast(associated_record_id as varchar) as  associated_record_id
,cast(address_id as varchar) as  address_id
,cast(account_id as varchar) as  account_id
,cast(role as varchar) as  role
,cast(start_date as varchar) as  start_date
,cast(end_date as varchar) as  end_date
,partition_date as load_date
,md5(study_protocol_number||contact_id||associated_dataset||associated_record_id||start_date||coalesce(role,'role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_contact_association_xl114-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl114-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl114-101') }})
	
union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(contact_id as varchar) as  contact_id
,cast(associated_dataset as varchar) as  associated_dataset
,cast(associated_record_id as varchar) as  associated_record_id
,cast(address_id as varchar) as  address_id
,cast(account_id as varchar) as  account_id
,cast(role as varchar) as  role
,cast(start_date as varchar) as  start_date
,cast(end_date as varchar) as  end_date
,partition_date as load_date
,md5(study_protocol_number||contact_id||associated_dataset||associated_record_id||coalesce(start_date,'Null')||coalesce(role,'role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_contact_association_xl184-312') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl184-312') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl184-312') }})
	
union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(contact_id as varchar) as  contact_id
,cast(associated_dataset as varchar) as  associated_dataset
,cast(associated_record_id as varchar) as  associated_record_id
,cast(address_id as varchar) as  address_id
,cast(account_id as varchar) as  account_id
,cast(role as varchar) as  role
,cast(start_date as varchar) as  start_date
,cast(end_date as varchar) as  end_date
,partition_date as load_date
,md5(study_protocol_number||contact_id||associated_dataset||associated_record_id||start_date||coalesce(role,'role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_contact_association_xl184-315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl184-315') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl184-315') }})
	
union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(contact_id as varchar) as  contact_id
,cast(associated_dataset as varchar) as  associated_dataset
,cast(associated_record_id as varchar) as  associated_record_id
,cast(address_id as varchar) as  address_id
,cast(account_id as varchar) as  account_id
,cast(role as varchar) as  role
,cast(start_date as varchar) as  start_date
,cast(end_date as varchar) as  end_date
,partition_date as load_date
,md5(study_protocol_number||contact_id||associated_dataset||associated_record_id||start_date||coalesce(role,'role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_contact_association_xl184_021') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl184_021') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl184_021') }})
	
union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(contact_id as varchar) as  contact_id
,cast(associated_dataset as varchar) as  associated_dataset
,cast(associated_record_id as varchar) as  associated_record_id
,cast(address_id as varchar) as  address_id
,cast(account_id as varchar) as  account_id
,cast(role as varchar) as  role
,cast(start_date as varchar) as  start_date
,cast(end_date as varchar) as  end_date
,partition_date as load_date
,md5(study_protocol_number||contact_id||associated_dataset||associated_record_id||start_date||coalesce(role,'role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_contact_association_xl184_311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl184_311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl184_311') }})

union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(contact_id as varchar) as  contact_id
,cast(associated_dataset as varchar) as  associated_dataset
,cast(associated_record_id as varchar) as  associated_record_id
,cast(address_id as varchar) as  address_id
,cast(account_id as varchar) as  account_id
,cast(role as varchar) as  role
,cast(start_date as varchar) as  start_date
,cast(end_date as varchar) as  end_date
,partition_date as load_date
,md5(study_protocol_number||contact_id||associated_dataset||associated_record_id||start_date||coalesce(role,'role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_contact_association_xb010-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xb010-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xb010-101') }})
union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(contact_id as varchar) as  contact_id
,cast(associated_dataset as varchar) as  associated_dataset
,cast(associated_record_id as varchar) as  associated_record_id
,cast(address_id as varchar) as  address_id
,cast(account_id as varchar) as  account_id
,cast(role as varchar) as  role
,cast(start_date as varchar) as  start_date
,cast(end_date as varchar) as  end_date
,partition_date as load_date
,md5(study_protocol_number||contact_id||associated_dataset||associated_record_id||start_date||coalesce(role,'role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_contact_association_xl495-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl495-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl495-101') }})

union all

select 
cast(clinical_study_source_id as varchar) as  study_protocol_number
,cast(contact_source_id as varchar) as  contact_id
,null as  associated_dataset
,cast(study_site_source_id as varchar) as  associated_record_id
,cast(address_source_id as varchar) as  address_id
,null as  account_id
,cast(job_role as varchar) as  role
-- ,cast(site_contact_start_date as varchar) as  start_date
,case when site_contact_start_date ~'^\d{2}/\d{2}/\d{4}$'
then TO_CHAR(TO_DATE(site_contact_start_date, 'MM/DD/YYYY'),'DD-MON-YYYY')        
else null
end as start_date
-- ,cast(site_contact_end_date as varchar) as  end_date
,case when site_contact_end_date ~'^\d{2}/\d{2}/\d{4}$'
then TO_CHAR(TO_DATE(site_contact_end_date, 'MM/DD/YYYY'),'DD-MON-YYYY')        
else null
end as end_date
,partition_date as load_date
,md5(clinical_study_source_id||contact_source_id||''||study_site_source_id||site_contact_start_date||coalesce(job_role,'job_role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'q_exe_sitecontact_xl092-304') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_sitecontact_xl092-304') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_sitecontact_xl092-304') }})

union all
select 
cast(clinical_study_source_id as varchar) as  study_protocol_number
,cast(contact_source_id as varchar) as  contact_id
,'STUDY SITE' as  associated_dataset
,cast(study_site_source_id as varchar) as  associated_record_id
,cast(address_source_id as varchar) as  address_id
,null as  account_id
,cast(job_role as varchar) as  role
-- ,cast(site_contact_start_date as varchar) as  start_date
,case when site_contact_start_date ~'^\d{2}/\d{2}/\d{4}$'
then TO_CHAR(TO_DATE(site_contact_start_date, 'MM/DD/YYYY'),'DD-MON-YYYY')        
else null
end as start_date
-- ,cast(site_contact_end_date as varchar) as  end_date
,case when site_contact_end_date ~'^\d{2}/\d{2}/\d{4}$'
then TO_CHAR(TO_DATE(site_contact_end_date, 'MM/DD/YYYY'),'DD-MON-YYYY')        
else null
end as end_date
,partition_date as load_date
,md5(clinical_study_source_id||contact_source_id||''||study_site_source_id||site_contact_start_date||coalesce(job_role,'job_role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'q_exe_sitecontact_xl092-305') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_sitecontact_xl092-305') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_sitecontact_xl092-305') }})

union all
select 
cast(clinical_study_source_id as varchar) as  study_protocol_number
,cast(contact_source_id as varchar) as  contact_id
,'STUDY SITE' as  associated_dataset
,cast(study_site_source_id as varchar) as  associated_record_id
,cast(address_source_id as varchar) as  address_id
,null as  account_id
,cast(job_role as varchar) as  role
-- ,cast(site_contact_start_date as varchar) as  start_date
,case when site_contact_start_date ~'^\d{2}/\d{2}/\d{4}$'
then TO_CHAR(TO_DATE(site_contact_start_date, 'MM/DD/YYYY'),'DD-MON-YYYY')        
else null
end as start_date
-- ,cast(site_contact_end_date as varchar) as  end_date
,case when site_contact_end_date ~'^\d{2}/\d{2}/\d{4}$'
then TO_CHAR(TO_DATE(site_contact_end_date, 'MM/DD/YYYY'),'DD-MON-YYYY')        
else null
end as end_date
,partition_date as load_date
,md5(clinical_study_source_id||contact_source_id||''||study_site_source_id||site_contact_start_date||coalesce(job_role,'job_role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'q_exe_sitecontact_xl092-009') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_sitecontact_xl092-009') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_sitecontact_xl092-009') }})

union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(contact_id as varchar) as  contact_id
,cast(associated_dataset as varchar) as  associated_dataset
,cast(associated_record_id as varchar) as  associated_record_id
,cast(address_id as varchar) as  address_id
,cast(account_id as varchar) as  account_id
,cast(role as varchar) as  role
,cast(start_date as varchar) as  start_date
,cast(end_date as varchar) as  end_date
,partition_date as load_date
,md5(study_protocol_number||contact_id||associated_dataset||associated_record_id||start_date||coalesce(role,'role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_contact_association_xl495-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl495-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl495-101') }})
union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(contact_id as varchar) as  contact_id
,cast(associated_dataset as varchar) as  associated_dataset
,cast(associated_record_id as varchar) as  associated_record_id
,cast(address_id as varchar) as  address_id
,cast(account_id as varchar) as  account_id
,cast(role as varchar) as  role
,cast(start_date as varchar) as  start_date
,cast(end_date as varchar) as  end_date
,partition_date as load_date
,md5(study_protocol_number||contact_id||associated_dataset||associated_record_id||start_date||coalesce(role,'role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_contact_association_xb628-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xb628-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xb628-101') }})

union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(contact_id as varchar) as  contact_id
,cast(associated_dataset as varchar) as  associated_dataset
,cast(associated_record_id as varchar) as  associated_record_id
,cast(address_id as varchar) as  address_id
,cast(account_id as varchar) as  account_id
,cast(role as varchar) as  role
,cast(start_date as varchar) as  start_date
,cast(end_date as varchar) as  end_date
,partition_date as load_date
,md5(study_protocol_number||contact_id||associated_dataset||associated_record_id||start_date||coalesce(role,'role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_contact_association_xl092-311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl092-311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl092-311') }})

union all

select 
cast(study_protocol_number as varchar) as study_protocol_number
,cast(contact_id as varchar) as contact_id
,cast(associated_dataset as varchar) as associated_dataset
,cast(associated_record_id as varchar) as associated_record_id
,cast(address_id as varchar) as address_id
,cast(account_id as varchar) as account_id
,trim(split_part(role,'-',2)) as role
,cast(start_date as varchar) as start_date
,cast(end_date as varchar) as end_date
,partition_date as load_date
,md5(study_protocol_number||contact_id||associated_dataset||associated_record_id||start_date||coalesce(role,'role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_contact_association_xb371-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xb371-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xb371-101') }})


union all

select 
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(contact_id as varchar) as  contact_id
,cast(associated_dataset as varchar) as  associated_dataset
,cast(associated_record_id as varchar) as  associated_record_id
,cast(address_id as varchar) as  address_id
,cast(account_id as varchar) as  account_id
,cast(role as varchar) as  role
,cast(start_date as varchar) as  start_date
,cast(end_date as varchar) as  end_date
,partition_date as load_date
,md5(study_protocol_number||contact_id||associated_dataset||associated_record_id||start_date||coalesce(role,'role')) as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_contact_association_xl092-202') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl092-202') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact_association_xl092-202') }})
)

select * from ctms_stg__contact_association
