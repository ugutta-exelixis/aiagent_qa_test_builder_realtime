with ctms_stg__study_subject as (
select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(subject_id as varchar) as  subject_id
,cast(subject_number as varchar) as  subject_number
,cast(randomization_id as varchar) as  randomization_id
,cast(enrollment_id as varchar) as  enrollment_id
,cast(subject_status as varchar) as  subject_status
,cast(subject_status_reason as varchar) as  subject_status_reason
,partition_date as load_date
,md5(study_protocol_number||study_site_id||subject_id||coalesce(subject_status,'subject_status')) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_subject_xb002_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xb002_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xb002_101') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(subject_id as varchar) as  subject_id
,cast(subject_number as varchar) as  subject_number
,cast(randomization_id as varchar) as  randomization_id
,cast(enrollment_id as varchar) as  enrollment_id
,cast(subject_status as varchar) as  subject_status
,cast(subject_status_reason as varchar) as  subject_status_reason
,partition_date as load_date
,md5(study_protocol_number||study_site_id||subject_id||coalesce(subject_status,'subject_status')) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_subject_xl092-002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xl092-002') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xl092-002') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(subject_id as varchar) as  subject_id
,cast(subject_number as varchar) as  subject_number
,cast(randomization_id as varchar) as  randomization_id
,cast(enrollment_id as varchar) as  enrollment_id
,cast(subject_status as varchar) as  subject_status
,cast(subject_status_reason as varchar) as  subject_status_reason
,partition_date as load_date
,md5(study_protocol_number||study_site_id||subject_id||coalesce(subject_status,'subject_status')) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_subject_xl092-303') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xl092-303') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xl092-303') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(subject_id as varchar) as  subject_id
,cast(subject_number as varchar) as  subject_number
,cast(randomization_id as varchar) as  randomization_id
,cast(enrollment_id as varchar) as  enrollment_id
,cast(subject_status as varchar) as  subject_status
,cast(subject_status_reason as varchar) as  subject_status_reason
,partition_date as load_date
,md5(study_protocol_number||study_site_id||subject_id||coalesce(subject_status,'subject_status')) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_subject_xl092_001') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xl092_001') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xl092_001') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(subject_id as varchar) as  subject_id
,cast(subject_number as varchar) as  subject_number
,cast(randomization_id as varchar) as  randomization_id
,cast(enrollment_id as varchar) as  enrollment_id
,cast(subject_status as varchar) as  subject_status
,cast(subject_status_reason as varchar) as  subject_status_reason
,partition_date as load_date
,md5(study_protocol_number||study_site_id||subject_id||coalesce(subject_status,'subject_status')) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_subject_xl102_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xl102_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xl102_101') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(subject_id as varchar) as  subject_id
,cast(subject_number as varchar) as  subject_number
,cast(randomization_id as varchar) as  randomization_id
,cast(enrollment_id as varchar) as  enrollment_id
,cast(subject_status as varchar) as  subject_status
,cast(subject_status_reason as varchar) as  subject_status_reason
,partition_date as load_date
,md5(study_protocol_number||study_site_id||subject_id||coalesce(subject_status,'subject_status')) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_subject_xl114-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xl114-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xl114-101') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(subject_id as varchar) as  subject_id
,cast(subject_number as varchar) as  subject_number
,cast(randomization_id as varchar) as  randomization_id
,cast(enrollment_id as varchar) as  enrollment_id
,cast(subject_status as varchar) as  subject_status
,cast(subject_status_reason as varchar) as  subject_status_reason
,partition_date as load_date
,md5(study_protocol_number||study_site_id||subject_id||coalesce(subject_status,'subject_status')) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_subject_xl184_311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xl184_311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xl184_311') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(subject_id as varchar) as  subject_id
,cast(subject_number as varchar) as  subject_number
,cast(randomization_id as varchar) as  randomization_id
,cast(enrollment_id as varchar) as  enrollment_id
,cast(subject_status as varchar) as  subject_status
,cast(subject_status_reason as varchar) as  subject_status_reason
,partition_date as load_date
,md5(study_protocol_number||study_site_id||subject_id||coalesce(subject_status,'subject_status')) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_subject_xl184_312') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xl184_312') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xl184_312') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(subject_id as varchar) as  subject_id
,cast(subject_number as varchar) as  subject_number
,cast(randomization_id as varchar) as  randomization_id
,cast(enrollment_id as varchar) as  enrollment_id
,cast(subject_status as varchar) as  subject_status
,cast(subject_status_reason as varchar) as  subject_status_reason
,partition_date as load_date
,md5(study_protocol_number||study_site_id||subject_id||coalesce(subject_status,'subject_status')) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_subject_xl184_315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xl184_315') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_subject_xl184_315') }})

)


select * from ctms_stg__study_subject