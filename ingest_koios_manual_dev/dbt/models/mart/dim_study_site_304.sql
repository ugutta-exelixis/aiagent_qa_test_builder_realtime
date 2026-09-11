with study_site_304 as (
    select distinct
study_id
,region
,"standard country name" as country_name
,"study site number" as site_number
,"account name" as site_name
,"pi name" as pi_name
,"current study site status" as site_status
,"actual site ready to enroll" as site_activation_date
,to_date("actual site selected date", 'DD-MON-YY') as site_selected_date
,md5(study_id||"study site number") as study_site_id_sk
from {{ ref('ctms_stg__study_site_304') }}
),
site_contact_details as (
    select distinct
    study_id
,"site #" as site_number
,"account name" as site_name
,"pi name" as pi_name
,"site status" as site_status
,"site country" as country_name
,md5(study_id||"site #") as study_site_id_sk
from {{ ref('ctms_stg__site_contact_details_xl092_304') }}
),
site_details_304 as (
select distinct
ss.study_id
,ss.region
,ss.country_name
,ss.site_number
,ss.site_name
,cd.pi_name
,ss.site_status
,ss.site_activation_date
,ss.site_selected_date
,ss.study_site_id_sk
from study_site_304 ss
join site_contact_details cd
on ss.study_site_id_sk = cd.study_site_id_sk
)
select * from site_details_304