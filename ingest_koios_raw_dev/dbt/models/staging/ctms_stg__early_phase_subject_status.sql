with early_phase_study_status_data as (
select project as studyid 
,subject
,split_part(subject,'-',3) as participant_id
,md5(split_part(subject,'-',3)) as participant_id_sk
,split_part(subject,'-',3) as participant_number
,split_part(subject,'-',2) as site_number
,site as site_name
,subjstat as participant_status
,stage as participant_type 
,cohort
,cast(partition_date as date) as load_date 
from {{ source ('koios_raw', 'xl092_002_earlyphase_subject_status') }}
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092_002_earlyphase_subject_status') }})
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092_002_earlyphase_subject_status') }})

union all

select project as studyid 
,subject
,split_part(subject,'-',3) as participant_id
,md5(split_part(subject,'-',3)) as participant_id_sk
,split_part(subject,'-',3) as participant_number
,split_part(subject,'-',2) as site_number
,site as site_name
,substat as participant_status
,stage3 as participant_type
,cohort
,cast(partition_date as date) as load_date 
from {{ source ('koios_raw', 'xb002_101_earlyphase_subject_status') }}
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xb002_101_earlyphase_subject_status') }})
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xb002_101_earlyphase_subject_status') }})
)

select * from early_phase_study_status_data