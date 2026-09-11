with study_site as(
	select distinct
	study_protocol_number as study_id,
	md5(study_protocol_number) as study_id_sk,
	study_site_id,
	left(site_number,4) as site_id,
	md5(concat(study_protocol_number||left(site_number,4))) as study_site_id_sk,
	parent_site_number,
	substring(parent_site_number,1,4) as alt_study_site_id,
	substring(site_number, 1, 4) as site_number,
	study_country_id,
	site_type,
	account_id,
	pi_contact_id,
	site_priority,
	referral_source,
	icn_region,
	study_site_status ,
	site_status_reason,
	to_date(load_date, 'YYY-MM-DD') as load_date
	from {{ref ('ctms_stg__study_site')}}
    ),
account as(
	select distinct
	account_id,
	name as site_name
	from {{ref ('ctms_stg__account')}}
    ),
contact as(
    select distinct
    contact_id,
    first_name,
    last_name,
    concat(first_name, ' ',last_name) as role_name,
    phone_number as role_phone_number,
	email as role_email
    from {{ref ('ctms_stg__contact')}}
),
Pi_Contact as (
	select 
	study_protocol_number as study_id,
	associated_record_id,
	study_protocol_number,
	max(pi_contact_id) as pi_contact_id
	from (
		select 
		study_protocol_number, associated_record_id,ca.contact_id as pi_contact_id,START_DATE,
		RANK()OVER(partition by ca.study_protocol_number||ca.associated_record_id
		order by ca.study_protocol_number, cast(START_DATE as date) DESC) as CA_RANK,
		contact_id 
		from {{ref ('ctms_stg__contact_association')}} ca
		where ca."role" in ('Principal Investigator')
		group by associated_record_id,START_DATE,study_protocol_number,contact_id
	) as a
	where CA_RANK = 1
	group by associated_record_id,study_protocol_number
	),
Final_Pi_contact as (
select distinct
	pc.study_id,
    c.contact_id,
    c.role_name as pi_name,
    c.role_phone_number as pi_phone_number,
	c.role_email as pi_email,
	pc.associated_record_id,
	pc.study_protocol_number,
	pc.pi_contact_id
	from Pi_Contact as pc
	left join contact c 
	on pc.pi_contact_id = c.contact_id
),
sc_contact as (
	select 
	study_protocol_number as study_id,
	associated_record_id,
	study_protocol_number,
	max(sc_contact_id) as sc_contact_id
	from (
		select 
		study_protocol_number, associated_record_id,ca.contact_id as sc_contact_id,START_DATE,
		RANK()OVER(partition by ca.study_protocol_number||ca.associated_record_id
		order by ca.study_protocol_number, cast(START_DATE as date) DESC) as CA_RANK,
		contact_id 
		from {{ref ('ctms_stg__contact_association')}} ca
		where ca."role" in ('Study Coordinator')
		group by associated_record_id,START_DATE,study_protocol_number,contact_id
	) as a
	where CA_RANK = 1
	group by associated_record_id,study_protocol_number
	),
final_sc_contact as (
select distinct
	sc.study_id,
    c.contact_id,
    c.role_name as sc_name,
    c.role_phone_number as sc_phone_number,
	c.role_email as sc_email,
	sc.associated_record_id,
	sc.study_protocol_number,
	sc.sc_contact_id
	from sc_contact sc
	left join contact c 
	on sc.sc_contact_id = c.contact_id
),
dim_study_site as(
        select
        ss.study_id_sk,
        ss.study_site_id_sk,
        ss.study_id,
        ss.study_site_id,
        ss.study_country_id,
        case when ss.alt_study_site_id  !~ E'^[+-]?[0-9]+(\\\\.[0-9]*)?([Ee][+-]?[0-9]+)?\$' then Null else ss.alt_study_site_id end as alt_study_site_id,
        ss.parent_site_number,
        ss.site_number,
        ss.site_type,
        ss.site_priority,
        ss.referral_source,
        ss.icn_region,
        ss.study_site_status ,
        ss.site_status_reason,
        ss.load_date,
        a.site_name,
        pc.pi_name,
        pc.pi_phone_number,
        pc.pi_email,
        sc.sc_name,
        sc.sc_phone_number,
        sc.sc_email
        from study_site ss
        left join account a
        on  ss.account_id=a.account_id
        left join Final_Pi_contact pc 
        on pc.study_id = ss.study_id
        and ss.study_site_id = pc.associated_record_id
        LEFT join final_sc_contact sc 
        on sc.study_id = ss.study_id
        and ss.study_site_id = sc.associated_record_id
    ),
	dim_study_site_IQVIA as (
		select distinct
		md5(ss.study_id) as study_id_sk,
		md5(ss.study_id||ss.site_number) as study_site_id_sk,
		ss.study_id,
		ss.site_number as study_site_id,
		study_country_id,
		null as alt_study_site_id,
		ss.site_number as parent_site_number,
		ss.site_number,
		'Main' as site_type,
		null as site_priority,
		null as referral_source,
		ss.region as icn_region,
		ss.study_site_status,
		null as site_status_reason,
		to_date(ss.load_date,'yyyy-mm-dd') as load_date,
		ss.site_name,
		sc."pi name" as pi_name,
		sc."pi phone #" as pi_phone_number,
		sc."pi email" as pi_email,
		sc."study coordinator name" as sc_name,
		sc."study coordinator phone #" as sc_phone_number,
		sc."study coordinator email" as sc_email 
		from 
		{{ref ('ctms_stg__study_site_IQVIA')}} ss
		left join 
		{{ref ('ctms_stg__site_contact_details_IQVIA')}} sc
		on md5(ss.study_id||ss.site_number) = sc.study_site_id_sk
	)
    select 
		study_id_sk,
        study_site_id_sk,
        study_id,
        study_site_id,
        study_country_id,
        alt_study_site_id,
        parent_site_number,
        site_number,
        site_type,
        site_priority,
        referral_source,
        icn_region,
        study_site_status ,
        site_status_reason,
        load_date,
        site_name,
        pi_name,
        pi_phone_number,
        pi_email,
        sc_name,
        sc_phone_number,
        sc_email
		 from dim_study_site
    union all
    select 
	study_id_sk,
	study_site_id_sk,
	study_id,
	study_site_id,
	study_country_id,
	alt_study_site_id,
	parent_site_number,
	site_number,
	site_type,
	site_priority,
	referral_source,
	icn_region,
	study_site_status,
	site_status_reason,
	load_date,
	site_name,
	pi_name,
	pi_phone_number,
	pi_email,
	sc_name,
	sc_phone_number,
	sc_email 
	from 
    dim_study_site_IQVIA