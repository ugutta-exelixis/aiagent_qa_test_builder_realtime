with ctms_stg__study_site_monitor as (
select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(visit_id as varchar) as  visit_id
,cast(cra_contact_id as varchar) as  cra_contact_id
,cast(visit_type as varchar) as  visit_type
,cast(visit_method as varchar) as  visit_method
,cast(visit_status as varchar) as  visit_status
,cast(planned_visit_start_date as varchar) as  planned_visit_start_date
,cast(visit_start_date as varchar) as  visit_start_date
,cast(visit_stop_date as varchar) as  visit_stop_date
,cast(trip_report_status as varchar) as  trip_report_status
,cast(trip_report_planned_apprv_date as varchar) as  trip_report_planned_apprv_date
,cast(trip_report_approved_date as varchar) as  trip_report_approved_date
,cast(trip_report_final_submit_date as varchar) as  trip_report_final_submit_date
,days_since_last_visit
,total_dos
,cast(co_mon_visit_flag as varchar) as  co_mon_visit_flag
,cast(initial_report_submission_date as varchar) as  initial_report_submission_date
,cast(flu_sent_date as varchar) as  flu_sent_date
,cast(confirm_sent_date as varchar) as  confirm_sent_date
,partition_date as load_date
,md5(study_protocol_number||study_site_id||visit_id||cra_contact_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_monitor_xb002_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xb002_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xb002_101') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(visit_id as varchar) as  visit_id
,cast(cra_contact_id as varchar) as  cra_contact_id
,cast(visit_type as varchar) as  visit_type
,cast(visit_method as varchar) as  visit_method
,cast(visit_status as varchar) as  visit_status
,cast(planned_visit_start_date as varchar) as  planned_visit_start_date
,cast(visit_start_date as varchar) as  visit_start_date
,cast(visit_stop_date as varchar) as  visit_stop_date
,cast(trip_report_status as varchar) as  trip_report_status
,cast(trip_report_planned_apprv_date as varchar) as  trip_report_planned_apprv_date
,cast(trip_report_approved_date as varchar) as  trip_report_approved_date
,cast(trip_report_final_submit_date as varchar) as  trip_report_final_submit_date
,days_since_last_visit
,total_dos
,cast(co_mon_visit_flag as varchar) as  co_mon_visit_flag
,cast(initial_report_submission_date as varchar) as  initial_report_submission_date
,cast(flu_sent_date as varchar) as  flu_sent_date
,cast(confirm_sent_date as varchar) as  confirm_sent_date
,partition_date as load_date
,md5(study_protocol_number||study_site_id||visit_id||cra_contact_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl092-002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl092-002') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl092-002') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(visit_id as varchar) as  visit_id
,cast(cra_contact_id as varchar) as  cra_contact_id
,cast(visit_type as varchar) as  visit_type
,cast(visit_method as varchar) as  visit_method
,cast(visit_status as varchar) as  visit_status
,cast(planned_visit_start_date as varchar) as  planned_visit_start_date
,cast(visit_start_date as varchar) as  visit_start_date
,cast(visit_stop_date as varchar) as  visit_stop_date
,cast(trip_report_status as varchar) as  trip_report_status
,cast(trip_report_planned_apprv_date as varchar) as  trip_report_planned_apprv_date
,cast(trip_report_approved_date as varchar) as  trip_report_approved_date
,cast(trip_report_final_submit_date as varchar) as  trip_report_final_submit_date
,days_since_last_visit
,total_dos
,cast(co_mon_visit_flag as varchar) as  co_mon_visit_flag
,cast(initial_report_submission_date as varchar) as  initial_report_submission_date
,cast(flu_sent_date as varchar) as  flu_sent_date
,cast(confirm_sent_date as varchar) as  confirm_sent_date
,partition_date as load_date
,md5(study_protocol_number||study_site_id||visit_id||cra_contact_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl092-303') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl092-303') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl092-303') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(visit_id as varchar) as  visit_id
,cast(cra_contact_id as varchar) as  cra_contact_id
,cast(visit_type as varchar) as  visit_type
,cast(visit_method as varchar) as  visit_method
,cast(visit_status as varchar) as  visit_status
,cast(planned_visit_start_date as varchar) as  planned_visit_start_date
,cast(visit_start_date as varchar) as  visit_start_date
,cast(visit_stop_date as varchar) as  visit_stop_date
,cast(trip_report_status as varchar) as  trip_report_status
,cast(trip_report_planned_apprv_date as varchar) as  trip_report_planned_apprv_date
,cast(trip_report_approved_date as varchar) as  trip_report_approved_date
,cast(trip_report_final_submit_date as varchar) as  trip_report_final_submit_date
,days_since_last_visit
,total_dos
,cast(co_mon_visit_flag as varchar) as  co_mon_visit_flag
,cast(initial_report_submission_date as varchar) as  initial_report_submission_date
,cast(flu_sent_date as varchar) as  flu_sent_date
,cast(confirm_sent_date as varchar) as  confirm_sent_date
,partition_date as load_date
,md5(study_protocol_number||study_site_id||visit_id||cra_contact_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl092_001') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl092_001') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl092_001') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(visit_id as varchar) as  visit_id
,cast(cra_contact_id as varchar) as  cra_contact_id
,cast(visit_type as varchar) as  visit_type
,cast(visit_method as varchar) as  visit_method
,cast(visit_status as varchar) as  visit_status
,cast(planned_visit_start_date as varchar) as  planned_visit_start_date
,cast(visit_start_date as varchar) as  visit_start_date
,cast(visit_stop_date as varchar) as  visit_stop_date
,cast(trip_report_status as varchar) as  trip_report_status
,cast(trip_report_planned_apprv_date as varchar) as  trip_report_planned_apprv_date
,cast(trip_report_approved_date as varchar) as  trip_report_approved_date
,cast(trip_report_final_submit_date as varchar) as  trip_report_final_submit_date
,days_since_last_visit
,total_dos
,cast(co_mon_visit_flag as varchar) as  co_mon_visit_flag
,cast(initial_report_submission_date as varchar) as  initial_report_submission_date
,cast(flu_sent_date as varchar) as  flu_sent_date
,cast(confirm_sent_date as varchar) as  confirm_sent_date
,partition_date as load_date
,md5(study_protocol_number||study_site_id||visit_id||cra_contact_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl102_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl102_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl102_101') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(visit_id as varchar) as  visit_id
,cast(cra_contact_id as varchar) as  cra_contact_id
,cast(visit_type as varchar) as  visit_type
,cast(visit_method as varchar) as  visit_method
,cast(visit_status as varchar) as  visit_status
,cast(planned_visit_start_date as varchar) as  planned_visit_start_date
,cast(visit_start_date as varchar) as  visit_start_date
,cast(visit_stop_date as varchar) as  visit_stop_date
,cast(trip_report_status as varchar) as  trip_report_status
,cast(trip_report_planned_apprv_date as varchar) as  trip_report_planned_apprv_date
,cast(trip_report_approved_date as varchar) as  trip_report_approved_date
,cast(trip_report_final_submit_date as varchar) as  trip_report_final_submit_date
,days_since_last_visit
,total_dos
,cast(co_mon_visit_flag as varchar) as  co_mon_visit_flag
,cast(initial_report_submission_date as varchar) as  initial_report_submission_date
,cast(flu_sent_date as varchar) as  flu_sent_date
,cast(confirm_sent_date as varchar) as  confirm_sent_date
,partition_date as load_date
,md5(study_protocol_number||study_site_id||visit_id||cra_contact_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl114-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl114-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl114-101') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(visit_id as varchar) as  visit_id
,cast(cra_contact_id as varchar) as  cra_contact_id
,cast(visit_type as varchar) as  visit_type
,cast(visit_method as varchar) as  visit_method
,cast(visit_status as varchar) as  visit_status
,cast(planned_visit_start_date as varchar) as  planned_visit_start_date
,cast(visit_start_date as varchar) as  visit_start_date
,cast(visit_stop_date as varchar) as  visit_stop_date
,cast(trip_report_status as varchar) as  trip_report_status
,cast(trip_report_planned_apprv_date as varchar) as  trip_report_planned_apprv_date
,cast(trip_report_approved_date as varchar) as  trip_report_approved_date
,cast(trip_report_final_submit_date as varchar) as  trip_report_final_submit_date
,days_since_last_visit
,total_dos
,cast(co_mon_visit_flag as varchar) as  co_mon_visit_flag
,cast(initial_report_submission_date as varchar) as  initial_report_submission_date
,cast(flu_sent_date as varchar) as  flu_sent_date
,cast(confirm_sent_date as varchar) as  confirm_sent_date
,partition_date as load_date
,md5(study_protocol_number||study_site_id||visit_id||cra_contact_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl184_021') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl184_021') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl184_021') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(visit_id as varchar) as  visit_id
,cast(cra_contact_id as varchar) as  cra_contact_id
,cast(visit_type as varchar) as  visit_type
,cast(visit_method as varchar) as  visit_method
,cast(visit_status as varchar) as  visit_status
,cast(planned_visit_start_date as varchar) as  planned_visit_start_date
,cast(visit_start_date as varchar) as  visit_start_date
,cast(visit_stop_date as varchar) as  visit_stop_date
,cast(trip_report_status as varchar) as  trip_report_status
,cast(trip_report_planned_apprv_date as varchar) as  trip_report_planned_apprv_date
,cast(trip_report_approved_date as varchar) as  trip_report_approved_date
,cast(trip_report_final_submit_date as varchar) as  trip_report_final_submit_date
,days_since_last_visit
,total_dos
,cast(co_mon_visit_flag as varchar) as  co_mon_visit_flag
,cast(initial_report_submission_date as varchar) as  initial_report_submission_date
,cast(flu_sent_date as varchar) as  flu_sent_date
,cast(confirm_sent_date as varchar) as  confirm_sent_date
,partition_date as load_date
,md5(study_protocol_number||study_site_id||visit_id||cra_contact_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl184_311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl184_311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl184_311') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(visit_id as varchar) as  visit_id
,cast(cra_contact_id as varchar) as  cra_contact_id
,cast(visit_type as varchar) as  visit_type
,cast(visit_method as varchar) as  visit_method
,cast(visit_status as varchar) as  visit_status
,cast(planned_visit_start_date as varchar) as  planned_visit_start_date
,cast(visit_start_date as varchar) as  visit_start_date
,cast(visit_stop_date as varchar) as  visit_stop_date
,cast(trip_report_status as varchar) as  trip_report_status
,cast(trip_report_planned_apprv_date as varchar) as  trip_report_planned_apprv_date
,cast(trip_report_approved_date as varchar) as  trip_report_approved_date
,cast(trip_report_final_submit_date as varchar) as  trip_report_final_submit_date
,days_since_last_visit
,total_dos
,cast(co_mon_visit_flag as varchar) as  co_mon_visit_flag
,cast(initial_report_submission_date as varchar) as  initial_report_submission_date
,cast(flu_sent_date as varchar) as  flu_sent_date
,cast(confirm_sent_date as varchar) as  confirm_sent_date
,partition_date as load_date
,md5(study_protocol_number||study_site_id||visit_id||cra_contact_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl184_312') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl184_312') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl184_312') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(visit_id as varchar) as  visit_id
,cast(cra_contact_id as varchar) as  cra_contact_id
,cast(visit_type as varchar) as  visit_type
,cast(visit_method as varchar) as  visit_method
,cast(visit_status as varchar) as  visit_status
,cast(planned_visit_start_date as varchar) as  planned_visit_start_date
,cast(visit_start_date as varchar) as  visit_start_date
,cast(visit_stop_date as varchar) as  visit_stop_date
,cast(trip_report_status as varchar) as  trip_report_status
,cast(trip_report_planned_apprv_date as varchar) as  trip_report_planned_apprv_date
,cast(trip_report_approved_date as varchar) as  trip_report_approved_date
,cast(trip_report_final_submit_date as varchar) as  trip_report_final_submit_date
,days_since_last_visit
,total_dos
,cast(co_mon_visit_flag as varchar) as  co_mon_visit_flag
,cast(initial_report_submission_date as varchar) as  initial_report_submission_date
,cast(flu_sent_date as varchar) as  flu_sent_date
,cast(confirm_sent_date as varchar) as  confirm_sent_date
,partition_date as load_date
,md5(study_protocol_number||study_site_id||visit_id||cra_contact_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl184_315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl184_315') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl184_315') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(visit_id as varchar) as  visit_id
,cast(cra_contact_id as varchar) as  cra_contact_id
,cast(visit_type as varchar) as  visit_type
,cast(visit_method as varchar) as  visit_method
,cast(visit_status as varchar) as  visit_status
,cast(planned_visit_start_date as varchar) as  planned_visit_start_date
,cast(visit_start_date as varchar) as  visit_start_date
,cast(visit_stop_date as varchar) as  visit_stop_date
,cast(trip_report_status as varchar) as  trip_report_status
,cast(trip_report_planned_apprv_date as varchar) as  trip_report_planned_apprv_date
,cast(trip_report_approved_date as varchar) as  trip_report_approved_date
,cast(trip_report_final_submit_date as varchar) as  trip_report_final_submit_date
,days_since_last_visit
,total_dos
,cast(co_mon_visit_flag as varchar) as  co_mon_visit_flag
,cast(initial_report_submission_date as varchar) as  initial_report_submission_date
,cast(flu_sent_date as varchar) as  flu_sent_date
,cast(confirm_sent_date as varchar) as  confirm_sent_date
,partition_date as load_date
,md5(study_protocol_number||study_site_id||visit_id||cra_contact_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_monitor_xb010-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xb010-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xb010-101') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(visit_id as varchar) as  visit_id
,cast(cra_contact_id as varchar) as  cra_contact_id
,cast(visit_type as varchar) as  visit_type
,cast(visit_method as varchar) as  visit_method
,cast(visit_status as varchar) as  visit_status
,cast(planned_visit_start_date as varchar) as  planned_visit_start_date
,cast(visit_start_date as varchar) as  visit_start_date
,cast(visit_stop_date as varchar) as  visit_stop_date
,cast(trip_report_status as varchar) as  trip_report_status
,cast(trip_report_planned_apprv_date as varchar) as  trip_report_planned_apprv_date
,cast(trip_report_approved_date as varchar) as  trip_report_approved_date
,cast(trip_report_final_submit_date as varchar) as  trip_report_final_submit_date
,days_since_last_visit
,total_dos
,cast(co_mon_visit_flag as varchar) as  co_mon_visit_flag
,cast(initial_report_submission_date as varchar) as  initial_report_submission_date
,cast(flu_sent_date as varchar) as  flu_sent_date
,cast(confirm_sent_date as varchar) as  confirm_sent_date
,partition_date as load_date
,md5(study_protocol_number||study_site_id||visit_id||cra_contact_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl495-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl495-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl495-101') }})



union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_site_id as varchar) as  study_site_id
,cast(visit_id as varchar) as  visit_id
,cast(cra_contact_id as varchar) as  cra_contact_id
,cast(visit_type as varchar) as  visit_type
,cast(visit_method as varchar) as  visit_method
,cast(visit_status as varchar) as  visit_status
,cast(planned_visit_start_date as varchar) as  planned_visit_start_date
,cast(visit_start_date as varchar) as  visit_start_date
,cast(visit_stop_date as varchar) as  visit_stop_date
,cast(trip_report_status as varchar) as  trip_report_status
,cast(trip_report_planned_apprv_date as varchar) as  trip_report_planned_apprv_date
,cast(trip_report_approved_date as varchar) as  trip_report_approved_date
,cast(trip_report_final_submit_date as varchar) as  trip_report_final_submit_date
,null as days_since_last_visit
,null as total_dos
,null as  co_mon_visit_flag
,null as initial_report_submission_date
,null as  flu_sent_date
,null as  confirm_sent_date
,partition_date as load_date
,md5(study_protocol_number||study_site_id||visit_id||cra_contact_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl092-202') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl092-202') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_monitor_xl092-202') }})


)

select * from ctms_stg__study_site_monitor