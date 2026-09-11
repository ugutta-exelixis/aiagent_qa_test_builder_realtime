
with address_association as (
    select distinct
    study_protocol_number as study_id
    ,associated_record_id as study_site_id
    ,address_id
    from {{ ref ('ctms_stg__address_association') }}
),

address as (
    select distinct
    address_id
    ,line1
    ,line2
    ,line3
    ,city
    ,state_province
    ,postal_code
    ,country
    ,load_date
    from {{ ref('ctms_stg__address') }}
),

dim_study_address as (
    select distinct
    md5(study_id) as study_id_sk
    ,md5(a.address_id) as address_id_sk
    ,cast(a.address_id as varchar(255)) as address_id
    ,aa.study_site_id
    ,cast(a.line1 as varchar(255)) as line1
    ,cast(a.line2 as varchar(255)) as line2
    ,cast(a.line3 as varchar(255)) as line3
    ,cast(a.city as varchar(255)) as city
    ,cast(a.state_province as varchar(255)) as state_province
    ,cast(a.postal_code as varchar(255)) as postal_code
    ,cast(a.country as varchar(255)) as country
    ,to_date(a.load_date,'YYYY-MM-DD') as load_date
	,current_date as aud_created_date
	,current_date as aud_updated_date
    from address_association aa
    inner join address a
    on aa.address_id = a.address_id
)

select * from dim_study_address