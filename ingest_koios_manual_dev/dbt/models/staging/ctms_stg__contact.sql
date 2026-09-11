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
)

select * from ctms_stg__contact