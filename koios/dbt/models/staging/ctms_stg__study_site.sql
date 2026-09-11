with country_region_mapping as 
(
    select distinct
    country as country_name
    ,cast(1000+dense_rank() over (order by country) as varchar) as study_country_id
    ,region as country_region
    from {{ ref('stg_country_region_mapping') }}
), 
ctms_stg__study_site as (
select
cast(s.study_protocol_number as varchar) as  study_protocol_number
,cast(s.study_country_id as varchar) as  study_country_id
,cast(study_site_id as varchar) as  study_site_id
,cast(account_id as varchar) as  account_id
,cast(pi_contact_id as varchar) as  pi_contact_id
,cast(nci as varchar) as  nci
,cast(pi_address_id as varchar) as  pi_address_id
,cast(site_number as varchar) as  site_number
,case 
    when crm.country_region = 'NA' then 'North America'
    when crm.country_region = 'APAC' then 'Asia/Pacific'
    when crm.country_region = 'LATAM' then 'Latin America'
    when crm.country_region = 'EMEA' then 'Europe/Africa'
    else crm.country_region
end as  icn_region
,cast(site_type as varchar) as  site_type
,cast(study_site_status as varchar) as  study_site_status
,cast(parent_site as varchar) as  parent_site_number
,cast(site_status_reason as varchar) as  site_status_reason
,cast(site_priority as varchar) as  site_priority
,cast(referral_source as varchar) as  referral_source
,cast(first_cda_sent_date as varchar) as  first_cda_sent_date
,cast(first_cda_completion_date as varchar) as  first_cda_completion_date
,cast(first_srq_sent_date as varchar) as  first_srq_sent_date
,cast(first_srq_complete_date as varchar) as  first_srq_complete_date
,cast(sub_prot_rdy_date as varchar) as  sub_prot_rdy_date
,cast(protocol_version as varchar) as  protocol_version
,cast(num_actip_docs_req as varchar) as  num_actip_docs_req
,cast(num_actip_docs_pend as varchar) as  num_actip_docs_pend
,cast(ip_pack_submitted_to_iedr as varchar) as  ip_pack_submitted_to_iedr
,cast(ip_pack_rejected_by_iedr as varchar) as  ip_pack_rejected_by_iedr
,cast(ip_pack_approval_by_iedr as varchar) as  ip_pack_approval_by_iedr
,cast(ip_pack_submitted_to_sponsor as varchar) as  ip_pack_submitted_to_sponsor
,cast(ip_pack_rejected_by_sponsor as varchar) as  ip_pack_rejected_by_sponsor
,cast(ip_pack_approval_by_sponsor as varchar) as  ip_pack_approval_by_sponsor
,cast(num_psars_required as varchar) as  num_psars_required
,cast(num_psars_pending as varchar) as  num_psars_pending
,cast(all_psars_complete as varchar) as  all_psars_complete
,cast(site_portal_access as varchar) as  site_portal_access
,cast(last_comm_date as varchar) as  last_comm_date
,cast(last_comm_type as varchar) as  last_comm_type
,cast(days_working as varchar) as  days_working
,s.partition_date as load_date
,md5(s.study_protocol_number||s.study_country_id||study_site_id||account_id||pi_contact_id) as hash_key
from {{ source ('koios_raw', 'pra_exl_study_site_xb002_101') }} s
join {{source('koios_raw', 'pra_exl_study_country_xb002_101')}} sc
  on s.study_country_id = sc.study_country_id
join country_region_mapping crm
  on upper(sc.country_name) = crm.country_name
where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xb002_101') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xb002_101') }})
    and sc.last_modified_date = (select max(last_modified_date) from {{source('koios_raw', 'pra_exl_study_country_xb002_101')}})
    and sc.partition_date = (select max(partition_date) from {{source('koios_raw', 'pra_exl_study_country_xb002_101')}})

union all

select
cast(s.study_protocol_number as varchar) as  study_protocol_number
,cast(s.study_country_id as varchar) as  study_country_id
,cast(study_site_id as varchar) as  study_site_id
,cast(account_id as varchar) as  account_id
,cast(pi_contact_id as varchar) as  pi_contact_id
,cast(nci as varchar) as  nci
,cast(pi_address_id as varchar) as  pi_address_id
,cast(site_number as varchar) as  site_number
,case 
    when crm.country_region = 'NA' then 'North America'
    when crm.country_region = 'APAC' then 'Asia/Pacific'
    when crm.country_region = 'LATAM' then 'Latin America'
    when crm.country_region = 'EMEA' then 'Europe/Africa'
    else crm.country_region
end as  icn_region
,cast(site_type as varchar) as  site_type
,cast(study_site_status as varchar) as  study_site_status
,cast(parent_site as varchar) as  parent_site_number
,cast(site_status_reason as varchar) as  site_status_reason
,cast(site_priority as varchar) as  site_priority
,cast(referral_source as varchar) as  referral_source
,cast(first_cda_sent_date as varchar) as  first_cda_sent_date
,cast(first_cda_completion_date as varchar) as  first_cda_completion_date
,cast(first_srq_sent_date as varchar) as  first_srq_sent_date
,cast(first_srq_complete_date as varchar) as  first_srq_complete_date
,cast(sub_prot_rdy_date as varchar) as  sub_prot_rdy_date
,cast(protocol_version as varchar) as  protocol_version
,cast(num_actip_docs_req as varchar) as  num_actip_docs_req
,cast(num_actip_docs_pend as varchar) as  num_actip_docs_pend
,cast(ip_pack_submitted_to_iedr as varchar) as  ip_pack_submitted_to_iedr
,cast(ip_pack_rejected_by_iedr as varchar) as  ip_pack_rejected_by_iedr
,cast(ip_pack_approval_by_iedr as varchar) as  ip_pack_approval_by_iedr
,cast(ip_pack_submitted_to_sponsor as varchar) as  ip_pack_submitted_to_sponsor
,cast(ip_pack_rejected_by_sponsor as varchar) as  ip_pack_rejected_by_sponsor
,cast(ip_pack_approval_by_sponsor as varchar) as  ip_pack_approval_by_sponsor
,cast(num_psars_required as varchar) as  num_psars_required
,cast(num_psars_pending as varchar) as  num_psars_pending
,cast(all_psars_complete as varchar) as  all_psars_complete
,cast(site_portal_access as varchar) as  site_portal_access
,cast(last_comm_date as varchar) as  last_comm_date
,cast(last_comm_type as varchar) as  last_comm_type
,cast(days_working as varchar) as  days_working
,s.partition_date as load_date
,md5(s.study_protocol_number||s.study_country_id||study_site_id||account_id||pi_contact_id) as hash_key
from {{ source ('koios_raw', 'pra_exl_study_site_xl092-002') }} s
join {{source('koios_raw', 'pra_exl_study_country_xl092-002')}} sc
  on s.study_country_id = sc.study_country_id
join country_region_mapping crm
  on upper(sc.country_name) = crm.country_name
where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092-002') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092-002') }})
    and sc.last_modified_date = (select max(last_modified_date) from {{source('koios_raw', 'pra_exl_study_country_xl092-002')}})
    and sc.partition_date = (select max(partition_date) from {{source('koios_raw', 'pra_exl_study_country_xl092-002')}})

union all

select
cast(s.study_protocol_number as varchar) as  study_protocol_number
,cast(s.study_country_id as varchar) as  study_country_id
,cast(study_site_id as varchar) as  study_site_id
,cast(account_id as varchar) as  account_id
,cast(pi_contact_id as varchar) as  pi_contact_id
,cast(nci as varchar) as  nci
,cast(pi_address_id as varchar) as  pi_address_id
,cast(site_number as varchar) as  site_number
,case 
    when crm.country_region = 'NA' then 'North America'
    when crm.country_region = 'APAC' then 'Asia/Pacific'
    when crm.country_region = 'LATAM' then 'Latin America'
    when crm.country_region = 'EMEA' then 'Europe/Africa'
    else crm.country_region
end as  icn_region
,cast(site_type as varchar) as  site_type
,cast(study_site_status as varchar) as  study_site_status
,cast(parent_site as varchar) as  parent_site_number
,cast(site_status_reason as varchar) as  site_status_reason
,cast(site_priority as varchar) as  site_priority
,cast(referral_source as varchar) as  referral_source
,cast(first_cda_sent_date as varchar) as  first_cda_sent_date
,cast(first_cda_completion_date as varchar) as  first_cda_completion_date
,cast(first_srq_sent_date as varchar) as  first_srq_sent_date
,cast(first_srq_complete_date as varchar) as  first_srq_complete_date
,cast(sub_prot_rdy_date as varchar) as  sub_prot_rdy_date
,cast(protocol_version as varchar) as  protocol_version
,cast(num_actip_docs_req as varchar) as  num_actip_docs_req
,cast(num_actip_docs_pend as varchar) as  num_actip_docs_pend
,cast(ip_pack_submitted_to_iedr as varchar) as  ip_pack_submitted_to_iedr
,cast(ip_pack_rejected_by_iedr as varchar) as  ip_pack_rejected_by_iedr
,cast(ip_pack_approval_by_iedr as varchar) as  ip_pack_approval_by_iedr
,cast(ip_pack_submitted_to_sponsor as varchar) as  ip_pack_submitted_to_sponsor
,cast(ip_pack_rejected_by_sponsor as varchar) as  ip_pack_rejected_by_sponsor
,cast(ip_pack_approval_by_sponsor as varchar) as  ip_pack_approval_by_sponsor
,cast(num_psars_required as varchar) as  num_psars_required
,cast(num_psars_pending as varchar) as  num_psars_pending
,cast(all_psars_complete as varchar) as  all_psars_complete
,cast(site_portal_access as varchar) as  site_portal_access
,cast(last_comm_date as varchar) as  last_comm_date
,cast(last_comm_type as varchar) as  last_comm_type
,cast(days_working as varchar) as  days_working
,s.partition_date as load_date
,md5(s.study_protocol_number||s.study_country_id||study_site_id||account_id||pi_contact_id) as hash_key
from {{ source ('koios_raw', 'pra_exl_study_site_xl092-303') }} s
join {{source('koios_raw', 'pra_exl_study_country_xl092-303')}} sc
  on s.study_country_id = sc.study_country_id
join country_region_mapping crm
  on upper(sc.country_name) = crm.country_name
where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092-303') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092-303') }})
    and sc.last_modified_date = (select max(last_modified_date) from {{source('koios_raw', 'pra_exl_study_country_xl092-303')}})
    and sc.partition_date = (select max(partition_date) from {{source('koios_raw', 'pra_exl_study_country_xl092-303')}})
	

union all

select
cast(s.study_protocol_number as varchar) as  study_protocol_number
,cast(s.study_country_id as varchar) as  study_country_id
,cast(study_site_id as varchar) as  study_site_id
,cast(account_id as varchar) as  account_id
,cast(pi_contact_id as varchar) as  pi_contact_id
,cast(nci as varchar) as  nci
,cast(pi_address_id as varchar) as  pi_address_id
,cast(site_number as varchar) as  site_number
,case 
    when crm.country_region = 'NA' then 'North America'
    when crm.country_region = 'APAC' then 'Asia/Pacific'
    when crm.country_region = 'LATAM' then 'Latin America'
    when crm.country_region = 'EMEA' then 'Europe/Africa'
    else crm.country_region
end as  icn_region
,cast(site_type as varchar) as  site_type
,cast(study_site_status as varchar) as  study_site_status
,cast(parent_site as varchar) as  parent_site_number
,cast(site_status_reason as varchar) as  site_status_reason
,cast(site_priority as varchar) as  site_priority
,cast(referral_source as varchar) as  referral_source
,cast(first_cda_sent_date as varchar) as  first_cda_sent_date
,cast(first_cda_completion_date as varchar) as  first_cda_completion_date
,cast(first_srq_sent_date as varchar) as  first_srq_sent_date
,cast(first_srq_complete_date as varchar) as  first_srq_complete_date
,cast(sub_prot_rdy_date as varchar) as  sub_prot_rdy_date
,cast(protocol_version as varchar) as  protocol_version
,cast(num_actip_docs_req as varchar) as  num_actip_docs_req
,cast(num_actip_docs_pend as varchar) as  num_actip_docs_pend
,cast(ip_pack_submitted_to_iedr as varchar) as  ip_pack_submitted_to_iedr
,cast(ip_pack_rejected_by_iedr as varchar) as  ip_pack_rejected_by_iedr
,cast(ip_pack_approval_by_iedr as varchar) as  ip_pack_approval_by_iedr
,cast(ip_pack_submitted_to_sponsor as varchar) as  ip_pack_submitted_to_sponsor
,cast(ip_pack_rejected_by_sponsor as varchar) as  ip_pack_rejected_by_sponsor
,cast(ip_pack_approval_by_sponsor as varchar) as  ip_pack_approval_by_sponsor
,cast(num_psars_required as varchar) as  num_psars_required
,cast(num_psars_pending as varchar) as  num_psars_pending
,cast(all_psars_complete as varchar) as  all_psars_complete
,cast(site_portal_access as varchar) as  site_portal_access
,cast(last_comm_date as varchar) as  last_comm_date
,cast(last_comm_type as varchar) as  last_comm_type
,cast(days_working as varchar) as  days_working
,s.partition_date as load_date
,md5(s.study_protocol_number||s.study_country_id||study_site_id||account_id||pi_contact_id) as hash_key
from {{ source ('koios_raw', 'pra_exl_study_site_xl092_001') }} s
join {{source('koios_raw', 'pra_exl_study_country_xl092_001')}} sc
  on s.study_country_id = sc.study_country_id
join country_region_mapping crm
  on upper(sc.country_name) = crm.country_name
where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092_001') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092_001') }})
    and sc.last_modified_date = (select max(last_modified_date) from {{source('koios_raw', 'pra_exl_study_country_xl092_001')}})
    and sc.partition_date = (select max(partition_date) from {{source('koios_raw', 'pra_exl_study_country_xl092_001')}})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,cast(study_site_id as varchar) as  study_site_id
,cast(account_id as varchar) as  account_id
,cast(pi_contact_id as varchar) as  pi_contact_id
,cast(nci as varchar) as  nci
,cast(pi_address_id as varchar) as  pi_address_id
,cast(site_number as varchar) as  site_number
,cast(icn_region as varchar) as  icn_region
,cast(site_type as varchar) as  site_type
,cast(study_site_status as varchar) as  study_site_status
,cast(parent_site_number as varchar) as  parent_site_number
,cast(site_status_reason as varchar) as  site_status_reason
,cast(site_priority as varchar) as  site_priority
,cast(referral_source as varchar) as  referral_source
,cast(first_cda_sent_date as varchar) as  first_cda_sent_date
,cast(first_cda_completion_date as varchar) as  first_cda_completion_date
,cast(first_srq_sent_date as varchar) as  first_srq_sent_date
,cast(first_srq_complete_date as varchar) as  first_srq_complete_date
,cast(sub_prot_rdy_date as varchar) as  sub_prot_rdy_date
,cast(protocol_version as varchar) as  protocol_version
,cast(num_actip_docs_req as varchar) as  num_actip_docs_req
,cast(num_actip_docs_pend as varchar) as  num_actip_docs_pend
,cast(ip_pack_submitted_to_iedr as varchar) as  ip_pack_submitted_to_iedr
,cast(ip_pack_rejected_by_iedr as varchar) as  ip_pack_rejected_by_iedr
,cast(ip_pack_approval_by_iedr as varchar) as  ip_pack_approval_by_iedr
,cast(ip_pack_submitted_to_sponsor as varchar) as  ip_pack_submitted_to_sponsor
,cast(ip_pack_rejected_by_sponsor as varchar) as  ip_pack_rejected_by_sponsor
,cast(ip_pack_approval_by_sponsor as varchar) as  ip_pack_approval_by_sponsor
,cast(num_psars_required as varchar) as  num_psars_required
,cast(num_psars_pending as varchar) as  num_psars_pending
,cast(all_psars_complete as varchar) as  all_psars_complete
,cast(site_portal_access as varchar) as  site_portal_access
,cast(last_comm_date as varchar) as  last_comm_date
,cast(last_comm_type as varchar) as  last_comm_type
,cast(days_working as varchar) as  days_working
,partition_date as load_date
,md5(study_protocol_number||study_country_id||study_site_id||account_id||pi_contact_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_xl102_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl102_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl102_101') }})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,cast(study_site_id as varchar) as  study_site_id
,cast(account_id as varchar) as  account_id
,cast(pi_contact_id as varchar) as  pi_contact_id
,cast(nci as varchar) as  nci
,cast(pi_address_id as varchar) as  pi_address_id
,cast(site_number as varchar) as  site_number
,cast(icn_region as varchar) as  icn_region
,cast(site_type as varchar) as  site_type
,cast(study_site_status as varchar) as  study_site_status
,cast(parent_site_number as varchar) as  parent_site_number
,cast(site_status_reason as varchar) as  site_status_reason
,cast(site_priority as varchar) as  site_priority
,cast(referral_source as varchar) as  referral_source
,cast(first_cda_sent_date as varchar) as  first_cda_sent_date
,cast(first_cda_completion_date as varchar) as  first_cda_completion_date
,cast(first_srq_sent_date as varchar) as  first_srq_sent_date
,cast(first_srq_complete_date as varchar) as  first_srq_complete_date
,cast(sub_prot_rdy_date as varchar) as  sub_prot_rdy_date
,cast(protocol_version as varchar) as  protocol_version
,cast(num_actip_docs_req as varchar) as  num_actip_docs_req
,cast(num_actip_docs_pend as varchar) as  num_actip_docs_pend
,cast(ip_pack_submitted_to_iedr as varchar) as  ip_pack_submitted_to_iedr
,cast(ip_pack_rejected_by_iedr as varchar) as  ip_pack_rejected_by_iedr
,cast(ip_pack_approval_by_iedr as varchar) as  ip_pack_approval_by_iedr
,cast(ip_pack_submitted_to_sponsor as varchar) as  ip_pack_submitted_to_sponsor
,cast(ip_pack_rejected_by_sponsor as varchar) as  ip_pack_rejected_by_sponsor
,cast(ip_pack_approval_by_sponsor as varchar) as  ip_pack_approval_by_sponsor
,cast(num_psars_required as varchar) as  num_psars_required
,cast(num_psars_pending as varchar) as  num_psars_pending
,cast(all_psars_complete as varchar) as  all_psars_complete
,cast(site_portal_access as varchar) as  site_portal_access
,cast(last_comm_date as varchar) as  last_comm_date
,cast(last_comm_type as varchar) as  last_comm_type
,cast(days_working as varchar) as  days_working
,partition_date as load_date
,md5(study_protocol_number||study_country_id||study_site_id||account_id||pi_contact_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_xl114-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl114-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl114-101') }})

union all

select
cast(s.study_protocol_number as varchar) as  study_protocol_number
,cast(s.study_country_id as varchar) as  study_country_id
,cast(study_site_id as varchar) as  study_site_id
,cast(account_id as varchar) as  account_id
,cast(pi_contact_id as varchar) as  pi_contact_id
,cast(nci as varchar) as  nci
,cast(pi_address_id as varchar) as  pi_address_id
,cast(site_number as varchar) as  site_number
,case 
    when crm.country_region = 'NA' then 'North America'
    when crm.country_region = 'APAC' then 'Asia/Pacific'
    when crm.country_region = 'LATAM' then 'Latin America'
    when crm.country_region = 'EMEA' then 'Europe/Africa'
    else crm.country_region
end as  icn_region
,cast(site_type as varchar) as  site_type
,cast(study_site_status as varchar) as  study_site_status
,cast(parent_site as varchar) as  parent_site_number
,cast(site_status_reason as varchar) as  site_status_reason
,cast(site_priority as varchar) as  site_priority
,cast(referral_source as varchar) as  referral_source
,cast(first_cda_sent_date as varchar) as  first_cda_sent_date
,cast(first_cda_completion_date as varchar) as  first_cda_completion_date
,cast(first_srq_sent_date as varchar) as  first_srq_sent_date
,cast(first_srq_complete_date as varchar) as  first_srq_complete_date
,cast(sub_prot_rdy_date as varchar) as  sub_prot_rdy_date
,cast(protocol_version as varchar) as  protocol_version
,cast(num_actip_docs_req as varchar) as  num_actip_docs_req
,cast(num_actip_docs_pend as varchar) as  num_actip_docs_pend
,cast(ip_pack_submitted_to_iedr as varchar) as  ip_pack_submitted_to_iedr
,cast(ip_pack_rejected_by_iedr as varchar) as  ip_pack_rejected_by_iedr
,cast(ip_pack_approval_by_iedr as varchar) as  ip_pack_approval_by_iedr
,cast(ip_pack_submitted_to_sponsor as varchar) as  ip_pack_submitted_to_sponsor
,cast(ip_pack_rejected_by_sponsor as varchar) as  ip_pack_rejected_by_sponsor
,cast(ip_pack_approval_by_sponsor as varchar) as  ip_pack_approval_by_sponsor
,cast(num_psars_required as varchar) as  num_psars_required
,cast(num_psars_pending as varchar) as  num_psars_pending
,cast(all_psars_complete as varchar) as  all_psars_complete
,cast(site_portal_access as varchar) as  site_portal_access
,cast(last_comm_date as varchar) as  last_comm_date
,cast(last_comm_type as varchar) as  last_comm_type
,cast(days_working as varchar) as  days_working
,s.partition_date as load_date
,md5(s.study_protocol_number||s.study_country_id||study_site_id||account_id||pi_contact_id) as hash_key
from {{ source ('koios_raw', 'pra_exl_study_site_xl184_021') }} s
join {{source('koios_raw', 'pra_exl_study_country_xl184_021')}} sc
  on s.study_country_id = sc.study_country_id
join country_region_mapping crm
  on upper(sc.country_name) = crm.country_name
where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl184_021') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl184_021') }})
    and sc.last_modified_date = (select max(last_modified_date) from {{source('koios_raw', 'pra_exl_study_country_xl184_021')}})
    and sc.partition_date = (select max(partition_date) from {{source('koios_raw', 'pra_exl_study_country_xl184_021')}})

union all

select
cast(s.study_protocol_number as varchar) as  study_protocol_number
,cast(s.study_country_id as varchar) as  study_country_id
,cast(study_site_id as varchar) as  study_site_id
,cast(account_id as varchar) as  account_id
,cast(pi_contact_id as varchar) as  pi_contact_id
,cast(nci as varchar) as  nci
,cast(pi_address_id as varchar) as  pi_address_id
,cast(site_number as varchar) as  site_number
,case 
    when crm.country_region = 'NA' then 'North America'
    when crm.country_region = 'APAC' then 'Asia/Pacific'
    when crm.country_region = 'LATAM' then 'Latin America'
    when crm.country_region = 'EMEA' then 'Europe/Africa'
    else crm.country_region
end as  icn_region
,cast(site_type as varchar) as  site_type
,cast(study_site_status as varchar) as  study_site_status
,cast(parent_site as varchar) as  parent_site_number
,cast(site_status_reason as varchar) as  site_status_reason
,cast(site_priority as varchar) as  site_priority
,cast(referral_source as varchar) as  referral_source
,cast(first_cda_sent_date as varchar) as  first_cda_sent_date
,cast(first_cda_completion_date as varchar) as  first_cda_completion_date
,cast(first_srq_sent_date as varchar) as  first_srq_sent_date
,cast(first_srq_complete_date as varchar) as  first_srq_complete_date
,cast(sub_prot_rdy_date as varchar) as  sub_prot_rdy_date
,cast(protocol_version as varchar) as  protocol_version
,cast(num_actip_docs_req as varchar) as  num_actip_docs_req
,cast(num_actip_docs_pend as varchar) as  num_actip_docs_pend
,cast(ip_pack_submitted_to_iedr as varchar) as  ip_pack_submitted_to_iedr
,cast(ip_pack_rejected_by_iedr as varchar) as  ip_pack_rejected_by_iedr
,cast(ip_pack_approval_by_iedr as varchar) as  ip_pack_approval_by_iedr
,cast(ip_pack_submitted_to_sponsor as varchar) as  ip_pack_submitted_to_sponsor
,cast(ip_pack_rejected_by_sponsor as varchar) as  ip_pack_rejected_by_sponsor
,cast(ip_pack_approval_by_sponsor as varchar) as  ip_pack_approval_by_sponsor
,cast(num_psars_required as varchar) as  num_psars_required
,cast(num_psars_pending as varchar) as  num_psars_pending
,cast(all_psars_complete as varchar) as  all_psars_complete
,cast(site_portal_access as varchar) as  site_portal_access
,cast(last_comm_date as varchar) as  last_comm_date
,cast(last_comm_type as varchar) as  last_comm_type
,cast(days_working as varchar) as  days_working
,s.partition_date as load_date
,md5(s.study_protocol_number||s.study_country_id||study_site_id||account_id||pi_contact_id) as hash_key
from {{ source ('koios_raw', 'pra_exl_study_site_xl184_311') }} s
join {{source('koios_raw', 'pra_exl_study_country_xl184_311')}} sc
  on s.study_country_id = sc.study_country_id
join country_region_mapping crm
  on upper(sc.country_name) = crm.country_name
where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl184_311') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl184_311') }})
    and sc.last_modified_date = (select max(last_modified_date) from {{source('koios_raw', 'pra_exl_study_country_xl184_311')}})
    and sc.partition_date = (select max(partition_date) from {{source('koios_raw', 'pra_exl_study_country_xl184_311')}})

union all

select
cast(s.study_protocol_number as varchar) as  study_protocol_number
,cast(s.study_country_id as varchar) as  study_country_id
,cast(study_site_id as varchar) as  study_site_id
,cast(account_id as varchar) as  account_id
,cast(pi_contact_id as varchar) as  pi_contact_id
,cast(nci as varchar) as  nci
,cast(pi_address_id as varchar) as  pi_address_id
,cast(site_number as varchar) as  site_number
,case 
    when crm.country_region = 'NA' then 'North America'
    when crm.country_region = 'APAC' then 'Asia/Pacific'
    when crm.country_region = 'LATAM' then 'Latin America'
    when crm.country_region = 'EMEA' then 'Europe/Africa'
    else crm.country_region
end as  icn_region
,cast(site_type as varchar) as  site_type
,cast(study_site_status as varchar) as  study_site_status
,cast(parent_site as varchar) as  parent_site_number
,cast(site_status_reason as varchar) as  site_status_reason
,cast(site_priority as varchar) as  site_priority
,cast(referral_source as varchar) as  referral_source
,cast(first_cda_sent_date as varchar) as  first_cda_sent_date
,cast(first_cda_completion_date as varchar) as  first_cda_completion_date
,cast(first_srq_sent_date as varchar) as  first_srq_sent_date
,cast(first_srq_complete_date as varchar) as  first_srq_complete_date
,cast(sub_prot_rdy_date as varchar) as  sub_prot_rdy_date
,cast(protocol_version as varchar) as  protocol_version
,cast(num_actip_docs_req as varchar) as  num_actip_docs_req
,cast(num_actip_docs_pend as varchar) as  num_actip_docs_pend
,cast(ip_pack_submitted_to_iedr as varchar) as  ip_pack_submitted_to_iedr
,cast(ip_pack_rejected_by_iedr as varchar) as  ip_pack_rejected_by_iedr
,cast(ip_pack_approval_by_iedr as varchar) as  ip_pack_approval_by_iedr
,cast(ip_pack_submitted_to_sponsor as varchar) as  ip_pack_submitted_to_sponsor
,cast(ip_pack_rejected_by_sponsor as varchar) as  ip_pack_rejected_by_sponsor
,cast(ip_pack_approval_by_sponsor as varchar) as  ip_pack_approval_by_sponsor
,cast(num_psars_required as varchar) as  num_psars_required
,cast(num_psars_pending as varchar) as  num_psars_pending
,cast(all_psars_complete as varchar) as  all_psars_complete
,cast(site_portal_access as varchar) as  site_portal_access
,cast(last_comm_date as varchar) as  last_comm_date
,cast(last_comm_type as varchar) as  last_comm_type
,cast(days_working as varchar) as  days_working
,s.partition_date as load_date
,md5(s.study_protocol_number||s.study_country_id||study_site_id||account_id||pi_contact_id) as hash_key
from {{ source ('koios_raw', 'pra_exl_study_site_xl184_312') }} s
join {{source('koios_raw', 'pra_exl_study_country_xl184_312')}} sc
  on s.study_country_id = sc.study_country_id
join country_region_mapping crm
  on upper(sc.country_name) = crm.country_name
where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl184_312') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl184_312') }})
    and sc.last_modified_date = (select max(last_modified_date) from {{source('koios_raw', 'pra_exl_study_country_xl184_312')}})
    and sc.partition_date = (select max(partition_date) from {{source('koios_raw', 'pra_exl_study_country_xl184_312')}})

 union all

select
cast(s.study_protocol_number as varchar) as  study_protocol_number
,cast(s.study_country_id as varchar) as  study_country_id
,cast(study_site_id as varchar) as  study_site_id
,cast(account_id as varchar) as  account_id
,cast(pi_contact_id as varchar) as  pi_contact_id
,cast(nci as varchar) as  nci
,cast(pi_address_id as varchar) as  pi_address_id
,cast(site_number as varchar) as  site_number
,case 
    when crm.country_region = 'NA' then 'North America'
    when crm.country_region = 'APAC' then 'Asia/Pacific'
    when crm.country_region = 'LATAM' then 'Latin America'
    when crm.country_region = 'EMEA' then 'Europe/Africa'
    else crm.country_region
end as  icn_region
,cast(site_type as varchar) as  site_type
,cast(study_site_status as varchar) as  study_site_status
,cast(parent_site as varchar) as  parent_site_number
,cast(site_status_reason as varchar) as  site_status_reason
,cast(site_priority as varchar) as  site_priority
,cast(referral_source as varchar) as  referral_source
,cast(first_cda_sent_date as varchar) as  first_cda_sent_date
,cast(first_cda_completion_date as varchar) as  first_cda_completion_date
,cast(first_srq_sent_date as varchar) as  first_srq_sent_date
,cast(first_srq_complete_date as varchar) as  first_srq_complete_date
,cast(sub_prot_rdy_date as varchar) as  sub_prot_rdy_date
,cast(protocol_version as varchar) as  protocol_version
,cast(num_actip_docs_req as varchar) as  num_actip_docs_req
,cast(num_actip_docs_pend as varchar) as  num_actip_docs_pend
,cast(ip_pack_submitted_to_iedr as varchar) as  ip_pack_submitted_to_iedr
,cast(ip_pack_rejected_by_iedr as varchar) as  ip_pack_rejected_by_iedr
,cast(ip_pack_approval_by_iedr as varchar) as  ip_pack_approval_by_iedr
,cast(ip_pack_submitted_to_sponsor as varchar) as  ip_pack_submitted_to_sponsor
,cast(ip_pack_rejected_by_sponsor as varchar) as  ip_pack_rejected_by_sponsor
,cast(ip_pack_approval_by_sponsor as varchar) as  ip_pack_approval_by_sponsor
,cast(num_psars_required as varchar) as  num_psars_required
,cast(num_psars_pending as varchar) as  num_psars_pending
,cast(all_psars_complete as varchar) as  all_psars_complete
,cast(site_portal_access as varchar) as  site_portal_access
,cast(last_comm_date as varchar) as  last_comm_date
,cast(last_comm_type as varchar) as  last_comm_type
,cast(days_working as varchar) as  days_working
,s.partition_date as load_date
,md5(s.study_protocol_number||s.study_country_id||study_site_id||account_id||pi_contact_id) as hash_key
from {{ source ('koios_raw', 'pra_exl_study_site_xl184_315') }} s
join {{source('koios_raw', 'pra_exl_study_country_xl184_315')}} sc
  on s.study_country_id = sc.study_country_id
join country_region_mapping crm
  on upper(sc.country_name) = crm.country_name
where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl184_315') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl184_315') }})
    and sc.last_modified_date = (select max(last_modified_date) from {{source('koios_raw', 'pra_exl_study_country_xl184_315')}})
    and sc.partition_date = (select max(partition_date) from {{source('koios_raw', 'pra_exl_study_country_xl184_315')}})

union all

select
cast(s.study_protocol_number as varchar) as  study_protocol_number
,cast(s.study_country_id as varchar) as  study_country_id
,cast(study_site_id as varchar) as  study_site_id
,cast(account_id as varchar) as  account_id
,cast(pi_contact_id as varchar) as  pi_contact_id
,cast(nci as varchar) as  nci
,cast(pi_address_id as varchar) as  pi_address_id
,cast(site_number as varchar) as  site_number
,case 
    when crm.country_region = 'NA' then 'North America'
    when crm.country_region = 'APAC' then 'Asia/Pacific'
    when crm.country_region = 'LATAM' then 'Latin America'
    when crm.country_region = 'EMEA' then 'Europe/Africa'
    else crm.country_region
end as  icn_region
,cast(site_type as varchar) as  site_type
,cast(study_site_status as varchar) as  study_site_status
,cast(parent_site as varchar) as  parent_site_number
,cast(site_status_reason as varchar) as  site_status_reason
,cast(site_priority as varchar) as  site_priority
,cast(referral_source as varchar) as  referral_source
,cast(first_cda_sent_date as varchar) as  first_cda_sent_date
,cast(first_cda_completion_date as varchar) as  first_cda_completion_date
,cast(first_srq_sent_date as varchar) as  first_srq_sent_date
,cast(first_srq_complete_date as varchar) as  first_srq_complete_date
,cast(sub_prot_rdy_date as varchar) as  sub_prot_rdy_date
,cast(protocol_version as varchar) as  protocol_version
,cast(num_actip_docs_req as varchar) as  num_actip_docs_req
,cast(num_actip_docs_pend as varchar) as  num_actip_docs_pend
,cast(ip_pack_submitted_to_iedr as varchar) as  ip_pack_submitted_to_iedr
,cast(ip_pack_rejected_by_iedr as varchar) as  ip_pack_rejected_by_iedr
,cast(ip_pack_approval_by_iedr as varchar) as  ip_pack_approval_by_iedr
,cast(ip_pack_submitted_to_sponsor as varchar) as  ip_pack_submitted_to_sponsor
,cast(ip_pack_rejected_by_sponsor as varchar) as  ip_pack_rejected_by_sponsor
,cast(ip_pack_approval_by_sponsor as varchar) as  ip_pack_approval_by_sponsor
,cast(num_psars_required as varchar) as  num_psars_required
,cast(num_psars_pending as varchar) as  num_psars_pending
,cast(all_psars_complete as varchar) as  all_psars_complete
,cast(site_portal_access as varchar) as  site_portal_access
,cast(last_comm_date as varchar) as  last_comm_date
,cast(last_comm_type as varchar) as  last_comm_type
,cast(days_working as varchar) as  days_working
,s.partition_date as load_date
,md5(s.study_protocol_number||s.study_country_id||study_site_id||account_id||pi_contact_id) as hash_key
from {{ source ('koios_raw', 'pra_exl_study_site_xb010-101') }} s
join {{source('koios_raw', 'pra_exl_study_country_xb010-101')}} sc
  on s.study_country_id = sc.study_country_id
join country_region_mapping crm
  on upper(sc.country_name) = crm.country_name
where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xb010-101') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xb010-101') }})
    and sc.last_modified_date = (select max(last_modified_date) from {{source('koios_raw', 'pra_exl_study_country_xb010-101')}})
    and sc.partition_date = (select max(partition_date) from {{source('koios_raw', 'pra_exl_study_country_xb010-101')}})

union all

select
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(study_country_id as varchar) as  study_country_id
,cast(study_site_id as varchar) as  study_site_id
,cast(account_id as varchar) as  account_id
,cast(pi_contact_id as varchar) as  pi_contact_id
,cast(nci as varchar) as  nci
,cast(pi_address_id as varchar) as  pi_address_id
,cast(site_number as varchar) as  site_number
,cast(icn_region as varchar) as  icn_region
,cast(site_type as varchar) as  site_type
,cast(study_site_status as varchar) as  study_site_status
,cast(parent_site_number as varchar) as  parent_site_number
,cast(site_status_reason as varchar) as  site_status_reason
,cast(site_priority as varchar) as  site_priority
,cast(referral_source as varchar) as  referral_source
,cast(first_cda_sent_date as varchar) as  first_cda_sent_date
,cast(first_cda_completion_date as varchar) as  first_cda_completion_date
,cast(first_srq_sent_date as varchar) as  first_srq_sent_date
,cast(first_srq_complete_date as varchar) as  first_srq_complete_date
,cast(sub_prot_rdy_date as varchar) as  sub_prot_rdy_date
,cast(protocol_version as varchar) as  protocol_version
,cast(num_actip_docs_req as varchar) as  num_actip_docs_req
,cast(num_actip_docs_pend as varchar) as  num_actip_docs_pend
,cast(ip_pack_submitted_to_iedr as varchar) as  ip_pack_submitted_to_iedr
,cast(ip_pack_rejected_by_iedr as varchar) as  ip_pack_rejected_by_iedr
,cast(ip_pack_approval_by_iedr as varchar) as  ip_pack_approval_by_iedr
,cast(ip_pack_submitted_to_sponsor as varchar) as  ip_pack_submitted_to_sponsor
,cast(ip_pack_rejected_by_sponsor as varchar) as  ip_pack_rejected_by_sponsor
,cast(ip_pack_approval_by_sponsor as varchar) as  ip_pack_approval_by_sponsor
,cast(num_psars_required as varchar) as  num_psars_required
,cast(num_psars_pending as varchar) as  num_psars_pending
,cast(all_psars_complete as varchar) as  all_psars_complete
,cast(site_portal_access as varchar) as  site_portal_access
,cast(last_comm_date as varchar) as  last_comm_date
,cast(last_comm_type as varchar) as  last_comm_type
,cast(days_working as varchar) as  days_working
,partition_date as load_date
,md5(study_protocol_number||study_country_id||study_site_id||account_id||pi_contact_id) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_study_site_xl495-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl495-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl495-101') }})

union all

select
cast(s.clinical_study_source_id as varchar) as  study_protocol_number
,cast(crm.study_country_id as varchar) as  study_country_id
,cast(s.study_site_source_id as varchar) as  study_site_id
,cast(sa.account_source_id as varchar) as  account_id
,cast(s.primary_investigator_source_id as varchar) as  pi_contact_id
,null as  nci
,null as  pi_address_id
,cast(s.site_number as varchar) as  site_number
,case 
    when crm.country_region = 'NA' then 'North America'
    when crm.country_region = 'APAC' then 'Asia/Pacific'
    when crm.country_region = 'LATAM' then 'Latin America'
    when crm.country_region = 'EMEA' then 'Europe/Africa'
    else crm.country_region
end as  icn_region
,'Main' as  site_type
,cast(s.site_status as varchar) as  study_site_status
,cast(s.site_number as varchar) as  parent_site_number
,null as  site_status_reason
,null as  site_priority
,null as  referral_source
,null as  first_cda_sent_date
,null as  first_cda_completion_date
,null as  first_srq_sent_date
,null as  first_srq_complete_date
,null as  sub_prot_rdy_date
,null as  protocol_version
,null as  num_actip_docs_req
,null as  num_actip_docs_pend
,null as  ip_pack_submitted_to_iedr
,null as  ip_pack_rejected_by_iedr
,null as  ip_pack_approval_by_iedr
,null as  ip_pack_submitted_to_sponsor
,null as  ip_pack_rejected_by_sponsor
,null as  ip_pack_approval_by_sponsor
,null as  num_psars_required
,null as  num_psars_pending
,null as  all_psars_complete
,null as  site_portal_access
,null as  last_comm_date
,null as  last_comm_type
,null as  days_working
,s.partition_date as load_date
,md5(s.clinical_study_source_id||crm.study_country_id||s.study_site_source_id||''||s.primary_investigator_source_id) as hash_key
FROM
     {{ source ('koios_raw', 'q_exe_site_xl092-304') }} s
    left join {{ source ('koios_raw', 'q_exe_siteaccount_xl092-304') }} sa 
    on s.study_site_source_id = sa.study_site_source_id 
    left join {{ source ('koios_raw', 'q_exe_studycountry_xl092-304') }} sc
    on s.study_country_source_id = sc.study_country_source_id
    left join country_region_mapping crm
    on case
    when upper(sc.study_region) = 'UNITED STATES OF AMERICA' then 'UNITED STATES'
	when upper(sc.study_region) = 'KOREA, REPUBLIC OF' then 'SOUTH KOREA'
	when upper(sc.study_region) = 'TAIWAN, REPUBLIC OF CHINA' then 'TAIWAN' 
    else upper(sc.study_region) end = crm.country_name
    where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_site_xl092-304') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_site_xl092-304') }})
    and sc.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-304') }})
    and sc.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-304') }})
    and sa.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_siteaccount_xl092-304') }})
    and sa.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_siteaccount_xl092-304') }})

union all

select
cast(s.clinical_study_source_id as varchar) as  study_protocol_number
,cast(crm.study_country_id as varchar) as  study_country_id
,cast(s.study_site_source_id as varchar) as  study_site_id
,cast(sa.account_source_id as varchar) as  account_id
,cast(s.primary_investigator_source_id as varchar) as  pi_contact_id
,null as  nci
,null as  pi_address_id
,cast(s.site_number as varchar) as  site_number
,case 
    when crm.country_region = 'NA' then 'North America'
    when crm.country_region = 'APAC' then 'Asia/Pacific'
    when crm.country_region = 'LATAM' then 'Latin America'
    when crm.country_region = 'EMEA' then 'Europe/Africa'
    else crm.country_region
end as  icn_region
,'Main' as  site_type
,case
    when s.site_status in ('Regulatory Green Light', 'Essential Documents in place') then 'Selected'
	when s.site_number = '4818' and  s.site_status = 'Closed' then 'Dropped'
    else s.site_status
end as study_site_status 
,cast(s.site_number as varchar) as  parent_site_number
,null as  site_status_reason
,null as  site_priority
,null as  referral_source
,null as  first_cda_sent_date
,null as  first_cda_completion_date
,null as  first_srq_sent_date
,null as  first_srq_complete_date
,null as  sub_prot_rdy_date
,null as  protocol_version
,null as  num_actip_docs_req
,null as  num_actip_docs_pend
,null as  ip_pack_submitted_to_iedr
,null as  ip_pack_rejected_by_iedr
,null as  ip_pack_approval_by_iedr
,null as  ip_pack_submitted_to_sponsor
,null as  ip_pack_rejected_by_sponsor
,null as  ip_pack_approval_by_sponsor
,null as  num_psars_required
,null as  num_psars_pending
,null as  all_psars_complete
,null as  site_portal_access
,null as  last_comm_date
,null as  last_comm_type
,null as  days_working
,s.partition_date as load_date
,md5(s.clinical_study_source_id||crm.study_country_id||s.study_site_source_id||''||s.primary_investigator_source_id) as hash_key
FROM
    {{ source ('koios_raw', 'q_exe_site_xl092-305') }} s
    left join {{ source ('koios_raw', 'q_exe_siteaccount_xl092-305') }} sa 
    on s.study_site_source_id = sa.study_site_source_id 
    left join {{ source ('koios_raw', 'q_exe_studycountry_xl092-305') }} sc
    on s.study_country_source_id = sc.study_country_source_id
    left join country_region_mapping crm
    on case
    when upper(sc.study_region) = 'UNITED STATES OF AMERICA' then 'UNITED STATES'
	when upper(sc.study_region) = 'KOREA, REPUBLIC OF' then 'SOUTH KOREA'
	when upper(sc.study_region) = 'TAIWAN, REPUBLIC OF CHINA' then 'TAIWAN' 
    else upper(sc.study_region) end = crm.country_name
    where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_site_xl092-305') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_site_xl092-305') }})
    and sc.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-305') }})
    and sc.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-305') }})
    and sa.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_siteaccount_xl092-305') }})
    and sa.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_siteaccount_xl092-305') }})

union all

select
cast(s.clinical_study_source_id as varchar) as  study_protocol_number
,cast(crm.study_country_id as varchar) as  study_country_id
,cast(s.study_site_source_id as varchar) as  study_site_id
,cast(sa.account_source_id as varchar) as  account_id
,cast(s.primary_investigator_source_id as varchar) as  pi_contact_id
,null as  nci
,null as  pi_address_id
,cast(s.site_number as varchar) as  site_number
,case 
    when crm.country_region = 'NA' then 'North America'
    when crm.country_region = 'APAC' then 'Asia/Pacific'
    when crm.country_region = 'LATAM' then 'Latin America'
    when crm.country_region = 'EMEA' then 'Europe/Africa'
    else crm.country_region
end as  icn_region
,'Main' as  site_type
,cast(s.site_status as varchar) as  study_site_status
,cast(s.site_number as varchar) as  parent_site_number
,null as  site_status_reason
,null as  site_priority
,null as  referral_source
,null as  first_cda_sent_date
,null as  first_cda_completion_date
,null as  first_srq_sent_date
,null as  first_srq_complete_date
,null as  sub_prot_rdy_date
,null as  protocol_version
,null as  num_actip_docs_req
,null as  num_actip_docs_pend
,null as  ip_pack_submitted_to_iedr
,null as  ip_pack_rejected_by_iedr
,null as  ip_pack_approval_by_iedr
,null as  ip_pack_submitted_to_sponsor
,null as  ip_pack_rejected_by_sponsor
,null as  ip_pack_approval_by_sponsor
,null as  num_psars_required
,null as  num_psars_pending
,null as  all_psars_complete
,null as  site_portal_access
,null as  last_comm_date
,null as  last_comm_type
,null as  days_working
,s.partition_date as load_date
,md5(s.clinical_study_source_id||crm.study_country_id||s.study_site_source_id||''||s.primary_investigator_source_id) as hash_key
FROM
    {{ source ('koios_raw', 'q_exe_site_xl092-009') }} s
    left join {{ source ('koios_raw', 'q_exe_siteaccount_xl092-009') }} sa 
    on s.study_site_source_id = sa.study_site_source_id 
    left join {{ source ('koios_raw', 'q_exe_studycountry_xl092-009') }} sc
    on s.study_country_source_id = sc.study_country_source_id
    left join country_region_mapping crm
    on case
    when upper(sc.study_region) = 'UNITED STATES OF AMERICA' then 'UNITED STATES'
	when upper(sc.study_region) = 'KOREA, REPUBLIC OF' then 'SOUTH KOREA'
	when upper(sc.study_region) = 'TAIWAN, REPUBLIC OF CHINA' then 'TAIWAN' 
    else upper(sc.study_region) end = crm.country_name
    where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_site_xl092-009') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_site_xl092-009') }})
    and sc.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-009') }})
    and sc.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_studycountry_xl092-009') }})
    and sa.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'q_exe_siteaccount_xl092-009') }})
    and sa.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'q_exe_siteaccount_xl092-009') }})
    and cast(s.site_number as varchar) in(
        select cast("site number" as varchar) as site_number from koios_raw."site_contact_details_actual_xl092-009" scdax 
        where partition_date = (select max(partition_date) from koios_raw."site_contact_details_actual_xl092-009")
        and last_modified_date  = (select max(last_modified_date) from koios_raw."site_contact_details_actual_xl092-009")
    )

union all
select
cast(s.study_protocol_number as varchar) as  study_protocol_number
,cast(s.study_country_id as varchar) as  study_country_id
,cast(study_site_id as varchar) as  study_site_id
,cast(account_id as varchar) as  account_id
,cast(pi_contact_id as varchar) as  pi_contact_id
,cast(nci as varchar) as  nci
,cast(pi_address_id as varchar) as  pi_address_id
,cast(site_number as varchar) as  site_number
,case 
    when crm.country_region = 'NA' then 'North America'
    when crm.country_region = 'APAC' then 'Asia/Pacific'
    when crm.country_region = 'LATAM' then 'Latin America'
    when crm.country_region = 'EMEA' then 'Europe/Africa'
    else crm.country_region
end as  icn_region
,cast(site_type as varchar) as  site_type
-- ,cast(study_site_status as varchar) as  study_site_status
,case when study_site_status in ('Active Enrolling','Activated') then 'Active Enrolling'
else study_site_status
end as  study_site_status
,cast(parent_site as varchar) as  parent_site_number
,cast(site_status_reason as varchar) as  site_status_reason
,cast(site_priority as varchar) as  site_priority
,cast(referral_source as varchar) as  referral_source
,cast(first_cda_sent_date as varchar) as  first_cda_sent_date
,cast(first_cda_completion_date as varchar) as  first_cda_completion_date
,cast(first_srq_sent_date as varchar) as  first_srq_sent_date
,cast(first_srq_complete_date as varchar) as  first_srq_complete_date
,cast(sub_prot_rdy_date as varchar) as  sub_prot_rdy_date
,cast(protocol_version as varchar) as  protocol_version
,cast(num_actip_docs_req as varchar) as  num_actip_docs_req
,cast(num_actip_docs_pend as varchar) as  num_actip_docs_pend
,cast(ip_pack_submitted_to_iedr as varchar) as  ip_pack_submitted_to_iedr
,cast(ip_pack_rejected_by_iedr as varchar) as  ip_pack_rejected_by_iedr
,cast(ip_pack_approval_by_iedr as varchar) as  ip_pack_approval_by_iedr
,cast(ip_pack_submitted_to_sponsor as varchar) as  ip_pack_submitted_to_sponsor
,cast(ip_pack_rejected_by_sponsor as varchar) as  ip_pack_rejected_by_sponsor
,cast(ip_pack_approval_by_sponsor as varchar) as  ip_pack_approval_by_sponsor
,cast(num_psars_required as varchar) as  num_psars_required
,cast(num_psars_pending as varchar) as  num_psars_pending
,cast(all_psars_complete as varchar) as  all_psars_complete
,cast(site_portal_access as varchar) as  site_portal_access
,cast(last_comm_date as varchar) as  last_comm_date
,cast(last_comm_type as varchar) as  last_comm_type
,cast(days_working as varchar) as  days_working
,s.partition_date as load_date
,md5(s.study_protocol_number||s.study_country_id||study_site_id||account_id||pi_contact_id) as hash_key
from {{ source ('koios_raw', 'pra_exl_study_site_xb628-101') }} s
join {{source('koios_raw', 'pra_exl_study_country_xb628-101')}} sc
  on s.study_country_id = sc.study_country_id ::text
join country_region_mapping crm
  on upper(sc.country_name) = crm.country_name
where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xb628-101') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xb628-101') }})
    and sc.last_modified_date = (select max(last_modified_date) from {{source('koios_raw', 'pra_exl_study_country_xb628-101')}})
    and sc.partition_date = (select max(partition_date) from {{source('koios_raw', 'pra_exl_study_country_xb628-101')}})

union all
select
cast(s.study_protocol_number as varchar) as  study_protocol_number
,cast(s.study_country_id as varchar) as  study_country_id
,cast(s.study_site_id as varchar) as  study_site_id
,cast(account_id as varchar) as  account_id
,cast(pi_contact_id as varchar) as  pi_contact_id
,cast(nci as varchar) as  nci
,cast(pi_address_id as varchar) as  pi_address_id
,cast(site_number as varchar) as  site_number
,case 
    when crm.country_region = 'NA' then 'North America'
    when crm.country_region = 'APAC' then 'Asia/Pacific'
    when crm.country_region = 'LATAM' then 'Latin America'
    when crm.country_region = 'EMEA' then 'Europe/Africa'
    else crm.country_region
end as  icn_region
,cast(site_type as varchar) as  site_type
,cast(study_site_status as varchar) as  study_site_status
,cast(parent_site as varchar) as  parent_site_number
,cast(site_status_reason as varchar) as  site_status_reason
,cast(site_priority as varchar) as  site_priority
,cast(referral_source as varchar) as  referral_source
,cast(first_cda_sent_date as varchar) as  first_cda_sent_date
,cast(first_cda_completion_date as varchar) as  first_cda_completion_date
,cast(first_srq_sent_date as varchar) as  first_srq_sent_date
,cast(first_srq_complete_date as varchar) as  first_srq_complete_date
,cast(sub_prot_rdy_date as varchar) as  sub_prot_rdy_date
,cast(protocol_version as varchar) as  protocol_version
,cast(num_actip_docs_req as varchar) as  num_actip_docs_req
,cast(num_actip_docs_pend as varchar) as  num_actip_docs_pend
,cast(ip_pack_submitted_to_iedr as varchar) as  ip_pack_submitted_to_iedr
,cast(ip_pack_rejected_by_iedr as varchar) as  ip_pack_rejected_by_iedr
,cast(ip_pack_approval_by_iedr as varchar) as  ip_pack_approval_by_iedr
,cast(ip_pack_submitted_to_sponsor as varchar) as  ip_pack_submitted_to_sponsor
,cast(ip_pack_rejected_by_sponsor as varchar) as  ip_pack_rejected_by_sponsor
,cast(ip_pack_approval_by_sponsor as varchar) as  ip_pack_approval_by_sponsor
,cast(num_psars_required as varchar) as  num_psars_required
,cast(num_psars_pending as varchar) as  num_psars_pending
,cast(all_psars_complete as varchar) as  all_psars_complete
,cast(site_portal_access as varchar) as  site_portal_access
,cast(last_comm_date as varchar) as  last_comm_date
,cast(last_comm_type as varchar) as  last_comm_type
,cast(days_working as varchar) as  days_working
,s.partition_date as load_date
,md5(s.study_protocol_number||s.study_country_id||study_site_id||account_id||pi_contact_id) as hash_key
from {{ source ('koios_raw', 'pra_exl_study_site_xl092-311') }} s
join {{source('koios_raw', 'pra_exl_study_country_xl092-311')}} sc
  on s.study_country_id = sc.study_country_id ::text
join country_region_mapping crm
  on upper(sc.country_name) = crm.country_name
where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092-311') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092-311') }})
    and sc.last_modified_date = (select max(last_modified_date) from {{source('koios_raw', 'pra_exl_study_country_xl092-311')}})
    and sc.partition_date = (select max(partition_date) from {{source('koios_raw', 'pra_exl_study_country_xl092-311')}})

union all

select
cast(s.study_protocol_number as varchar) as  study_protocol_number
,cast(s.study_country_id as varchar) as  study_country_id
,cast(s.study_site_id as varchar) as  study_site_id
,cast(s.account_id as varchar) as  account_id
,cast(s.pi_contact_id as varchar) as  pi_contact_id
,null as  nci
,cast(s.pi_address_id as varchar) as  pi_address_id
,cast(s.site_number as varchar) as  site_number
,case 
    when crm.country_region = 'NA' then 'North America'
    when crm.country_region = 'APAC' then 'Asia/Pacific'
    when crm.country_region = 'LATAM' then 'Latin America'
    when crm.country_region = 'EMEA' then 'Europe/Africa'
    else crm.country_region
end as  icn_region
,cast('Main' as varchar) as  site_type
-- ,cast(study_site_status as varchar) as  study_site_status
,case when s.study_site_status in ('Active Enrolling','Activated') then 'Active Enrolling'
else study_site_status
end as  study_site_status
,cast(s.site_number as varchar) as  parent_site_number
,cast(s.site_status_reason as varchar) as  site_status_reason
,null as site_priority
,null as  referral_source
,null as  first_cda_sent_date
,null as  first_cda_completion_date
,null as  first_srq_sent_date
,null as  first_srq_complete_date
,null as  sub_prot_rdy_date
,cast(s.protocol_version as varchar) as  protocol_version
,null as  num_actip_docs_req
,null as  num_actip_docs_pend
,null as  ip_pack_submitted_to_iedr
,null as  ip_pack_rejected_by_iedr
,null as  ip_pack_approval_by_iedr
,null as  ip_pack_submitted_to_sponsor
,null as  ip_pack_rejected_by_sponsor
,null as  ip_pack_approval_by_sponsor
,null as  num_psars_required
,null as  num_psars_pending
,null as  all_psars_complete
,null as  site_portal_access
,null as  last_comm_date
,null as  last_comm_type
,null as  days_working
,s.partition_date as load_date
,md5(s.study_protocol_number||s.study_country_id||s.study_site_id||account_id||pi_contact_id) as hash_key
from {{ source ('koios_raw', 'pra_exl_study_site_xb371-101') }} s
left join {{source('koios_raw', 'pra_exl_study_country_xb371-101')}} sc
  on s.study_country_id = sc.study_country_id
left join country_region_mapping crm
on case
    when upper(sc.country_name) = 'UNITED STATES OF AMERICA' then 'UNITED STATES'
 when upper(sc.country_name) = 'KOREA  REPUBLIC OF' then 'SOUTH KOREA'
 when upper(sc.country_name) = 'TAIWAN  PROVINCE OF CHINA' then 'TAIWAN' 
    else upper(sc.country_name) end = crm.country_name
  --on upper(sc.country_name) = crm.country_name
where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xb371-101') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xb371-101') }})
    and sc.last_modified_date = (select max(last_modified_date) from {{source('koios_raw', 'pra_exl_study_country_xb371-101')}})
    and sc.partition_date = (select max(partition_date) from {{source('koios_raw', 'pra_exl_study_country_xb371-101')}})


union all

select
cast(s.study_protocol_number as varchar) as  study_protocol_number
,cast(s.study_country_id as varchar) as  study_country_id
,cast(s.study_site_id as varchar) as  study_site_id
,cast(s.account_id as varchar) as  account_id
,cast(s.pi_contact_id as varchar) as  pi_contact_id
,null as  nci
,cast(s.pi_address_id as varchar) as  pi_address_id
,cast(s.site_number as varchar) as  site_number
,case 
    when crm.country_region = 'NA' then 'North America'
    when crm.country_region = 'APAC' then 'Asia/Pacific'
    when crm.country_region = 'LATAM' then 'Latin America'
    when crm.country_region = 'EMEA' then 'Europe/Africa'
    else crm.country_region
end as  icn_region
,case when site_type is not null then cast(site_type as varchar) else 'Main' end as  site_type
-- ,cast(study_site_status as varchar) as  study_site_status
,case when s.study_site_status in ('Active Enrolling','Activated') then 'Active Enrolling'
else study_site_status
end as  study_site_status
,cast(s.site_number as varchar) as  parent_site_number
,cast(s.site_status_reason as varchar) as  site_status_reason
,null as site_priority
,null as  referral_source
,null as  first_cda_sent_date
,null as  first_cda_completion_date
,null as  first_srq_sent_date
,null as  first_srq_complete_date
,null as  sub_prot_rdy_date
,cast(s.protocol_version as varchar) as  protocol_version
,null as  num_actip_docs_req
,null as  num_actip_docs_pend
,null as  ip_pack_submitted_to_iedr
,null as  ip_pack_rejected_by_iedr
,null as  ip_pack_approval_by_iedr
,null as  ip_pack_submitted_to_sponsor
,null as  ip_pack_rejected_by_sponsor
,null as  ip_pack_approval_by_sponsor
,null as  num_psars_required
,null as  num_psars_pending
,null as  all_psars_complete
,null as  site_portal_access
,null as  last_comm_date
,null as  last_comm_type
,null as  days_working
,s.partition_date as load_date
,md5(s.study_protocol_number||s.study_country_id||study_site_id||account_id||pi_contact_id) as hash_key
from {{ source ('koios_raw', 'pra_exl_study_site_xl092-202') }} s
join {{source('koios_raw', 'pra_exl_study_country_xl092-202')}} sc
  on s.study_country_id = sc.study_country_id
join country_region_mapping crm
  on upper(sc.country_name) = crm.country_name
where s.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092-202') }})
    and s.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092-202') }})
    and sc.last_modified_date = (select max(last_modified_date) from {{source('koios_raw', 'pra_exl_study_country_xl092-202')}})
    and sc.partition_date = (select max(partition_date) from {{source('koios_raw', 'pra_exl_study_country_xl092-202')}})


)

select 
study_protocol_number
,study_country_id
,study_site_id
,account_id
,pi_contact_id
,nci
,pi_address_id
,ss.site_number
,icn_region
,case when 
spm.parent_site_number is not null
then 'Satellite'
else ss.site_type 
end as site_type
,study_site_status
,case when 
spm.parent_site_number is not null 
then spm.parent_site_number
else ss.parent_site_number 
end as parent_site_number
,site_status_reason
,site_priority
,referral_source
,first_cda_sent_date
,first_cda_completion_date
,first_srq_sent_date
,first_srq_complete_date
,sub_prot_rdy_date,protocol_version
,num_actip_docs_req
,num_actip_docs_pend
,ip_pack_submitted_to_iedr
,ip_pack_rejected_by_iedr
,ip_pack_approval_by_iedr
,ip_pack_submitted_to_sponsor
,ip_pack_rejected_by_sponsor
,ip_pack_approval_by_sponsor
,num_psars_required
,num_psars_pending
,all_psars_complete
,site_portal_access
,last_comm_date
,last_comm_type
,days_working
,load_date
,hash_key
from ctms_stg__study_site ss
left join (select * from {{ source ('koios_raw', 'q_exe_site_parent_mapping') }}
where last_modified_date = (select max(last_modified_date)from {{ source ('koios_raw', 'q_exe_site_parent_mapping') }}) 
and partition_date = (select max(partition_date)from {{ source ('koios_raw', 'q_exe_site_parent_mapping') }}))as spm
on ss.study_protocol_number = spm.study_id and ss.site_number = spm.site_number
