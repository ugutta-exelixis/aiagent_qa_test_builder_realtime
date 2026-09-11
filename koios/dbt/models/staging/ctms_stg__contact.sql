with ctms_stg__contact as 
(select
contact_id
,first_name
,last_name
,phone_number
,fax_number
,email
,mid_name
,title
,degree
,inv_flag
,cell_number
,home_number
,pager_number
,pager_pin
,alt_phone_number
,email_2
,email_3
,partition_date as load_date
,md5(contact_id) as hash_key
from
    {{ source ('koios_raw', 'pra_exl_contact') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_contact') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_contact') }})
union all
select
contact_source_id as contact_id
,first_name
,last_name
,work_phone_number as phone_number
,work_fax_number as fax_number
,primary_email_id as email
,mid_name
,job_title as title
,primary_specialty_category as degree
,null as inv_flag
,cell_phone_number as cell_number
,null as home_number
,null as pager_number
,null as pager_pin
,null as alt_phone_number
,null as email_2
,null as email_3
,partition_date as load_date
,md5(contact_source_id) as hash_key
from
    {{ source ('koios_raw', 'q_exe_contact') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_contact') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_contact') }})

)

select * from ctms_stg__contact