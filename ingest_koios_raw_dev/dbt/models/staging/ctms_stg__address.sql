with ctms_stg__address as 
(select
address_id
,line1
,line2
,line3
,city
,state_province
,postal_code
,country
,partition_date as load_date
,md5(address_id) as hash_key
from
    {{ source ('koios_raw', 'pra_exl_address') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_address') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_address') }})
)

select * from ctms_stg__address