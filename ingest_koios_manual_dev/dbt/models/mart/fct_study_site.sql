with study_site as(
    select distinct
    study_protocol_number,
	md5(study_protocol_number) as study_id_sk,
	case when length(site_number) = 9 then split_part(site_number,'-',1) 
    when length(site_number) > 9 then split_part(site_number,'-',2) else null end as site_id,
	md5(study_country_id||study_protocol_number) as study_country_id_sk,
	md5(pi_address_id) as address_id_sk,
	md5(pi_contact_id) as contact_id_sk,
    md5(account_id) as account_id_sk,
	md5(study_site_id) as study_site_id_doc_id_sk,
	protocol_version as protocol_version_number_sk,
	to_date(sub_prot_rdy_date,'DD-MON-YY') as sub_prot_rdy_date,
	site_portal_access,
	to_date(last_comm_date,'DD-MON-YY') as last_comm_date,
	last_comm_type,
	days_working,
	parent_site_number,
    substring(parent_site_number,1,4) as alt_study_site_id,
	nci,
	load_date
from {{ref ('ctms_stg__study_site')}}
union 
select 
study_id,
md5(study_id) as study_id_sk,
site_number as site_id,
md5(study_country_id||study_id) as study_country_id_sk,
null as address_id_sk,
null as contact_id_sk,
null as account_id_sk,
md5(site_number) as study_site_id_doc_id_sk,
null as protocol_version_number_sk,
null as sub_prot_rdy_date,
null as site_portal_access,
null as last_comm_date,
null as last_comm_type,
null as days_working,
site_number as parent_site_number,
substring(site_number,1,4) as alt_study_site_id,
null as nci,
	load_date
from {{ref ('ctms_stg__study_site_IQVIA')}}
),
srm_enroll_proj as(
    select distinct
    study_protocol_number,
    SUM(cast(planned_subjects as int)) as projected_participants,
    to_date(month_end_date,'DD-MON-YY') as month_end_projected
    from {{ref ('ctms_stg__srm_enroll_proj')}}
    GROUP BY month_end_projected,study_protocol_number
),
enroll_forecast as(
    select distinct
    SUM(cast(planned_subjects as int)) as forecasted_participants,
    to_date(month_end_date,'DD-MON-YY') as month_end_forecasted
    from {{ref ('ctms_stg__enroll_forecast')}}
    GROUP BY month_end_forecasted
),
fct_study_site as(
    select distinct
    ss.study_id_sk,
    md5(concat(study_protocol_number||site_id)) as study_site_id_sk,
    ss.study_country_id_sk,
    ss.address_id_sk,
    ss.contact_id_sk,
    ss.account_id_sk,
    ss.study_site_id_doc_id_sk,
    ss.protocol_version_number_sk,
    ss.sub_prot_rdy_date,
    ss.site_portal_access,
    ss.last_comm_date,
    ss.last_comm_type,
    cast(ss.days_working as bigint) as days_working,
    ss.parent_site_number,
    case when ss.alt_study_site_id  !~ E'^[+-]?[0-9]+(\\\\.[0-9]*)?([Ee][+-]?[0-9]+)?\$' then Null else ss.alt_study_site_id end as alt_study_site_id,
    ss.nci,
    to_date(ss.load_date,'YYYY-MM-DD') as load_date
    -- ep.projected_participants,
    -- ep.month_end_projected,
    -- ef.forecasted_participants,
    -- ef.month_end_forecasted
    from study_site as ss
    -- left join srm_enroll_proj as ep
    -- on ss.study_protocol_number=ep.study_protocol_number
    -- left join enroll_forecast as ef
    -- on ep.projected_participants=ef.forecasted_participants
    )
    select * from fct_study_site