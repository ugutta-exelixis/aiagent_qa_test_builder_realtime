with ctms_stg__study_site as (
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
    {{ source ('koios_raw', 'pra_exl_study_site_xb002_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xb002_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xb002_101') }})

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
    {{ source ('koios_raw', 'pra_exl_study_site_xl092-002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092-002') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092-002') }})

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
    {{ source ('koios_raw', 'pra_exl_study_site_xl092-303') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092-303') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092-303') }})

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
    {{ source ('koios_raw', 'pra_exl_study_site_xl092_001') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092_001') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl092_001') }})

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
    {{ source ('koios_raw', 'pra_exl_study_site_xl184_021') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl184_021') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl184_021') }})

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
    {{ source ('koios_raw', 'pra_exl_study_site_xl184_311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl184_311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl184_311') }})

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
    {{ source ('koios_raw', 'pra_exl_study_site_xl184_312') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl184_312') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl184_312') }})

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
    {{ source ('koios_raw', 'pra_exl_study_site_xl184_315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl184_315') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_study_site_xl184_315') }})

)


select * from ctms_stg__study_site
