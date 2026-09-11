with ctms_stg__account as 
(
select distinct account_id
,name
,partition_date as load_date
,md5(account_id) as hash_key
from
    {{ source ('koios_raw', 'pra_exl_account') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_account') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_account') }})
)

select * from ctms_stg__account