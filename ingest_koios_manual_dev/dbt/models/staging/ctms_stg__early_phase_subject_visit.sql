with early_phase_subject_visit as (
select project as study_id
,subject as participant_number
,split_part(subject,'-',3) as participant_id
,split_part(subject,'-',2) as site_number 
,site as site_name
,svdts as date_of_visit_scheduled
,Null as date_of_visit_actual
,instancename as visit_description
,cast(partition_date as date) as load_date 
from {{ source ('koios_raw', 'xl092_002_earlyphase_subject_visit') }}
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092_002_earlyphase_subject_visit') }})
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092_002_earlyphase_subject_visit') }})

union all

select project as study_id
,subject as participant_number
,split_part(subject,'-',3) as participant_id
,split_part(subject,'-',2) as site_number 
,site as site_name
,vddt as date_of_visit_scheduled
,Null as date_of_visit_actual
,instancename as visit_description
,cast(partition_date as date) as load_date 
from {{ source ('koios_raw', 'xb002_101_earlyphase_subject_visit') }}
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xb002_101_earlyphase_subject_visit') }})
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xb002_101_earlyphase_subject_visit') }})
)

select * from early_phase_subject_visit