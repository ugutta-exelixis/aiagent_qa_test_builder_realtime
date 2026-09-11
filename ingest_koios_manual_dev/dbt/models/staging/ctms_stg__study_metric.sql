with ctms_stg__study_metric as (
select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(metric_name as varchar) as  metric_name
,metric_number
,partition_date as load_date
,md5(study_protocol_number||metric_name||parent_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_metric_xb002_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xb002_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xb002_101') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(metric_name as varchar) as  metric_name
,metric_number
,partition_date as load_date
,md5(study_protocol_number||metric_name||parent_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_metric_xl092-002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl092-002') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl092-002') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(metric_name as varchar) as  metric_name
,metric_number
,partition_date as load_date
,md5(study_protocol_number||metric_name||parent_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_metric_xl092-303') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl092-303') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl092-303') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(metric_name as varchar) as  metric_name
,metric_number
,partition_date as load_date
,md5(study_protocol_number||metric_name||parent_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_metric_xl092_001') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl092_001') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl092_001') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(metric_name as varchar) as  metric_name
,metric_number
,partition_date as load_date
,md5(study_protocol_number||metric_name||parent_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_metric_xl102_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl102_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl102_101') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(metric_name as varchar) as  metric_name
,metric_number
,partition_date as load_date
,md5(study_protocol_number||metric_name||parent_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_metric_xl114-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl114-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl114-101') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(metric_name as varchar) as  metric_name
,metric_number
,partition_date as load_date
,md5(study_protocol_number||metric_name||parent_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_metric_xl184_021') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl184_021') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl184_021') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(metric_name as varchar) as  metric_name
,metric_number
,partition_date as load_date
,md5(study_protocol_number||metric_name||parent_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_metric_xl184_311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl184_311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl184_311') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(metric_name as varchar) as  metric_name
,metric_number
,partition_date as load_date
,md5(study_protocol_number||metric_name||parent_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_metric_xl184_312') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl184_312') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl184_312') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(parent_dataset as varchar) as  parent_dataset
,cast(parent_id as varchar) as  parent_id
,cast(metric_name as varchar) as  metric_name
,metric_number
,partition_date as load_date
,md5(study_protocol_number||metric_name||parent_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_metric_xl184_315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl184_315') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_metric_xl184_315') }})
)


select * from ctms_stg__study_metric