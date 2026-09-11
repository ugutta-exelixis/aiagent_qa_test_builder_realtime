with ctms_stg__study_submission as (
select
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_country_id as varchar) as study_country_id
,cast(study_site_id as varchar) as study_site_id
,cast(submission_id as varchar) as submission_id
,cast(submission_type as varchar) as submission_type
,cast(submission_level as varchar) as submission_level
,cast(submission_description as varchar) as submission_description
,cast(submission_status as varchar) as submission_status
,cast(recipient_type as varchar) as recipient_type
,cast(target_submission_date as varchar) as target_submission_date
,cast(actual_submission_date as varchar) as actual_submission_date
,cast(target_approval_date as varchar) as target_approval_date
,cast(actual_approval_date as varchar) as actual_approval_date
,cast(planned_submission_date as varchar) as planned_submission_date
,cast(planned_approval_date as varchar) as planned_approval_date
,cast(renewal_report_due_date as varchar) as renewal_report_due_date
,cast(renewal_report_complete_date as varchar) as renewal_report_complete_date
,cast(comments as varchar) as comments
,partition_date as load_date
,md5(study_protocol_number||study_country_id||coalesce(cast(study_site_id as varchar),'study_site_id')||submission_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_submission_xb002_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xb002_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xb002_101') }})

union all

select
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_country_id as varchar) as study_country_id
,cast(study_site_id as varchar) as study_site_id
,cast(submission_id as varchar) as submission_id
,cast(submission_type as varchar) as submission_type
,cast(submission_level as varchar) as submission_level
,cast(submission_description as varchar) as submission_description
,cast(submission_status as varchar) as submission_status
,cast(recipient_type as varchar) as recipient_type
,cast(target_submission_date as varchar) as target_submission_date
,cast(actual_submission_date as varchar) as actual_submission_date
,cast(target_approval_date as varchar) as target_approval_date
,cast(actual_approval_date as varchar) as actual_approval_date
,cast(planned_submission_date as varchar) as planned_submission_date
,cast(planned_approval_date as varchar) as planned_approval_date
,cast(renewal_report_due_date as varchar) as renewal_report_due_date
,cast(renewal_report_complete_date as varchar) as renewal_report_complete_date
,cast(comments as varchar) as comments
,partition_date as load_date
,md5(study_protocol_number||study_country_id||coalesce(cast(study_site_id as varchar),'study_site_id')||submission_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_submission_xl092-002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl092-002') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl092-002') }})

union all

select
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_country_id as varchar) as study_country_id
,cast(study_site_id as varchar) as study_site_id
,cast(submission_id as varchar) as submission_id
,cast(submission_type as varchar) as submission_type
,cast(submission_level as varchar) as submission_level
,cast(submission_description as varchar) as submission_description
,cast(submission_status as varchar) as submission_status
,cast(recipient_type as varchar) as recipient_type
,cast(target_submission_date as varchar) as target_submission_date
,cast(actual_submission_date as varchar) as actual_submission_date
,cast(target_approval_date as varchar) as target_approval_date
,cast(actual_approval_date as varchar) as actual_approval_date
,cast(planned_submission_date as varchar) as planned_submission_date
,cast(planned_approval_date as varchar) as planned_approval_date
,cast(renewal_report_due_date as varchar) as renewal_report_due_date
,cast(renewal_report_complete_date as varchar) as renewal_report_complete_date
,cast(comments as varchar) as comments
,partition_date as load_date
,md5(study_protocol_number||study_country_id||coalesce(cast(study_site_id as varchar),'study_site_id')||submission_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_submission_xl092-303') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl092-303') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl092-303') }})

union all

select
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_country_id as varchar) as study_country_id
,cast(study_site_id as varchar) as study_site_id
,cast(submission_id as varchar) as submission_id
,cast(submission_type as varchar) as submission_type
,cast(submission_level as varchar) as submission_level
,cast(submission_description as varchar) as submission_description
,cast(submission_status as varchar) as submission_status
,cast(recipient_type as varchar) as recipient_type
,cast(target_submission_date as varchar) as target_submission_date
,cast(actual_submission_date as varchar) as actual_submission_date
,cast(target_approval_date as varchar) as target_approval_date
,cast(actual_approval_date as varchar) as actual_approval_date
,cast(planned_submission_date as varchar) as planned_submission_date
,cast(planned_approval_date as varchar) as planned_approval_date
,cast(renewal_report_due_date as varchar) as renewal_report_due_date
,cast(renewal_report_complete_date as varchar) as renewal_report_complete_date
,cast(comments as varchar) as comments
,partition_date as load_date
,md5(study_protocol_number||study_country_id||coalesce(cast(study_site_id as varchar),'study_site_id')||submission_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_submission_xl092_001') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl092_001') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl092_001') }})

union all

select
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_country_id as varchar) as study_country_id
,cast(study_site_id as varchar) as study_site_id
,cast(submission_id as varchar) as submission_id
,cast(submission_type as varchar) as submission_type
,cast(submission_level as varchar) as submission_level
,cast(submission_description as varchar) as submission_description
,cast(submission_status as varchar) as submission_status
,cast(recipient_type as varchar) as recipient_type
,cast(target_submission_date as varchar) as target_submission_date
,cast(actual_submission_date as varchar) as actual_submission_date
,cast(target_approval_date as varchar) as target_approval_date
,cast(actual_approval_date as varchar) as actual_approval_date
,cast(planned_submission_date as varchar) as planned_submission_date
,cast(planned_approval_date as varchar) as planned_approval_date
,cast(renewal_report_due_date as varchar) as renewal_report_due_date
,cast(renewal_report_complete_date as varchar) as renewal_report_complete_date
,cast(comments as varchar) as comments
,partition_date as load_date
,md5(study_protocol_number||study_country_id||coalesce(cast(study_site_id as varchar),'study_site_id')||submission_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_submission_xl102_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl102_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl102_101') }})

union all

select
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_country_id as varchar) as study_country_id
,cast(study_site_id as varchar) as study_site_id
,cast(submission_id as varchar) as submission_id
,cast(submission_type as varchar) as submission_type
,cast(submission_level as varchar) as submission_level
,cast(submission_description as varchar) as submission_description
,cast(submission_status as varchar) as submission_status
,cast(recipient_type as varchar) as recipient_type
,cast(target_submission_date as varchar) as target_submission_date
,cast(actual_submission_date as varchar) as actual_submission_date
,cast(target_approval_date as varchar) as target_approval_date
,cast(actual_approval_date as varchar) as actual_approval_date
,cast(planned_submission_date as varchar) as planned_submission_date
,cast(planned_approval_date as varchar) as planned_approval_date
,cast(renewal_report_due_date as varchar) as renewal_report_due_date
,cast(renewal_report_complete_date as varchar) as renewal_report_complete_date
,cast(comments as varchar) as comments
,partition_date as load_date
,md5(study_protocol_number||study_country_id||coalesce(cast(study_site_id as varchar),'study_site_id')||submission_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_submission_xl114-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl114-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl114-101') }})

union all

select
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_country_id as varchar) as study_country_id
,cast(study_site_id as varchar) as study_site_id
,cast(submission_id as varchar) as submission_id
,cast(submission_type as varchar) as submission_type
,cast(submission_level as varchar) as submission_level
,cast(submission_description as varchar) as submission_description
,cast(submission_status as varchar) as submission_status
,cast(recipient_type as varchar) as recipient_type
,cast(target_submission_date as varchar) as target_submission_date
,cast(actual_submission_date as varchar) as actual_submission_date
,cast(target_approval_date as varchar) as target_approval_date
,cast(actual_approval_date as varchar) as actual_approval_date
,cast(planned_submission_date as varchar) as planned_submission_date
,cast(planned_approval_date as varchar) as planned_approval_date
,cast(renewal_report_due_date as varchar) as renewal_report_due_date
,cast(renewal_report_complete_date as varchar) as renewal_report_complete_date
,cast(comments as varchar) as comments
,partition_date as load_date
,md5(study_protocol_number||coalesce(cast(study_country_id as varchar),'study_country_id')||coalesce(cast(study_site_id as varchar),'study_site_id')||submission_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_submission_xl184_021') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl184_021') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl184_021') }})

union all

select
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_country_id as varchar) as study_country_id
,cast(study_site_id as varchar) as study_site_id
,cast(submission_id as varchar) as submission_id
,cast(submission_type as varchar) as submission_type
,cast(submission_level as varchar) as submission_level
,cast(submission_description as varchar) as submission_description
,cast(submission_status as varchar) as submission_status
,cast(recipient_type as varchar) as recipient_type
,cast(target_submission_date as varchar) as target_submission_date
,cast(actual_submission_date as varchar) as actual_submission_date
,cast(target_approval_date as varchar) as target_approval_date
,cast(actual_approval_date as varchar) as actual_approval_date
,cast(planned_submission_date as varchar) as planned_submission_date
,cast(planned_approval_date as varchar) as planned_approval_date
,cast(renewal_report_due_date as varchar) as renewal_report_due_date
,cast(renewal_report_complete_date as varchar) as renewal_report_complete_date
,cast(comments as varchar) as comments
,partition_date as load_date
,md5(study_protocol_number||study_country_id||coalesce(cast(study_site_id as varchar),'study_site_id')||submission_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_submission_xl184_311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl184_311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl184_311') }})

union all

select
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_country_id as varchar) as study_country_id
,cast(study_site_id as varchar) as study_site_id
,cast(submission_id as varchar) as submission_id
,cast(submission_type as varchar) as submission_type
,cast(submission_level as varchar) as submission_level
,cast(submission_description as varchar) as submission_description
,cast(submission_status as varchar) as submission_status
,cast(recipient_type as varchar) as recipient_type
,cast(target_submission_date as varchar) as target_submission_date
,cast(actual_submission_date as varchar) as actual_submission_date
,cast(target_approval_date as varchar) as target_approval_date
,cast(actual_approval_date as varchar) as actual_approval_date
,cast(planned_submission_date as varchar) as planned_submission_date
,cast(planned_approval_date as varchar) as planned_approval_date
,cast(renewal_report_due_date as varchar) as renewal_report_due_date
,cast(renewal_report_complete_date as varchar) as renewal_report_complete_date
,cast(comments as varchar) as comments
,partition_date as load_date
,md5(study_protocol_number||coalesce(cast(study_country_id as varchar),'study_country_id')||coalesce(cast(study_site_id as varchar),'study_site_id')||submission_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_submission_xl184_312') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl184_312') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl184_312') }})

union all

select
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_country_id as varchar) as study_country_id
,cast(study_site_id as varchar) as study_site_id
,cast(submission_id as varchar) as submission_id
,cast(submission_type as varchar) as submission_type
,cast(submission_level as varchar) as submission_level
,cast(submission_description as varchar) as submission_description
,cast(submission_status as varchar) as submission_status
,cast(recipient_type as varchar) as recipient_type
,cast(target_submission_date as varchar) as target_submission_date
,cast(actual_submission_date as varchar) as actual_submission_date
,cast(target_approval_date as varchar) as target_approval_date
,cast(actual_approval_date as varchar) as actual_approval_date
,cast(planned_submission_date as varchar) as planned_submission_date
,cast(planned_approval_date as varchar) as planned_approval_date
,cast(renewal_report_due_date as varchar) as renewal_report_due_date
,cast(renewal_report_complete_date as varchar) as renewal_report_complete_date
,cast(comments as varchar) as comments
,partition_date as load_date
,md5(study_protocol_number||study_country_id||coalesce(cast(study_site_id as varchar),'study_site_id')||submission_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_submission_xl184_315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl184_315') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xl184_315') }})

union all

select
cast(study_protocol_number as varchar) as study_protocol_number
,cast(study_country_id as varchar) as study_country_id
,cast(study_site_id as varchar) as study_site_id
,cast(submission_id as varchar) as submission_id
,cast(submission_type as varchar) as submission_type
,cast(submission_level as varchar) as submission_level
,cast(submission_description as varchar) as submission_description
,cast(submission_status as varchar) as submission_status
,cast(recipient_type as varchar) as recipient_type
,cast(target_submission_date as varchar) as target_submission_date
,cast(actual_submission_date as varchar) as actual_submission_date
,cast(target_approval_date as varchar) as target_approval_date
,cast(actual_approval_date as varchar) as actual_approval_date
,cast(planned_submission_date as varchar) as planned_submission_date
,cast(planned_approval_date as varchar) as planned_approval_date
,cast(renewal_report_due_date as varchar) as renewal_report_due_date
,cast(renewal_report_complete_date as varchar) as renewal_report_complete_date
,cast(comments as varchar) as comments
,partition_date as load_date
,md5(study_protocol_number||study_country_id||coalesce(cast(study_site_id as varchar),'study_site_id')||submission_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_submission_xb010-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xb010-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_submission_xb010-101') }})

)

select * from ctms_stg__study_submission