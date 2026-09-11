with contact_association as (
    select
        study_protocol_number as study_id,
        associated_record_id as study_site_id,
        contact_id,
        load_date
    from {{ ref('ctms_stg__contact_association') }}
),

contact as (
    select
        first_name,
        last_name,
        concat(first_name||','||last_name) as full_name,
        phone_number,
        fax_number,
        email,
        mid_name,
        title,
        degree,
        inv_flag,
        cell_number,
        home_number,
        pager_number,
        pager_pin,
        alt_phone_number,
        email_2,
        email_3,
        contact_id
    from {{ ref('ctms_stg__contact') }}
),

dim_study_contact as (
    select distinct
        md5(study_id) as study_id_sk,
        md5(cad.contact_id) as contact_id_sk,
        cast(cd.contact_id as varchar(255)) as contact_id,
        cad.study_site_id,
        cast(cd.first_name as varchar(255)) as first_name,
        cast(cd.last_name as varchar(255)) as last_name,
        cast(cd.full_name as varchar(255)) as full_name,
        cast(cd.phone_number as varchar(255)) as phone_number,
        cast(cd.fax_number as varchar(255)) as fax_number,
        cast(cd.email as varchar(255)) as email,
        cast(cd.mid_name as varchar(255)) as mid_name,
        cast(cd.title as varchar(500)) as title,
        cast(cd.degree as varchar(255)) as degree,
        cast(cd.inv_flag as boolean) as inv_flag,
        cast(cd.cell_number as varchar(255)) as cell_number,
        cast(cd.home_number as varchar(255)) as home_number,
        cast(cd.pager_number as varchar(255)) as pager_number,
        cast(cd.pager_pin as varchar(255)) as pager_pin,
        cast(cd.alt_phone_number as varchar(255)) as alt_phone_number,
        cast(cd.email_2 as varchar(255)) as email_2,
        cast(cd.email_3 as varchar(255)) as email_3,
        to_date(cad.load_date,'YYYY-MM-DD') as load_date,
        current_date as aud_created_date,
        current_date as aud_updated_date
    from contact_association as cad
    join contact as cd
    on cad.contact_id = cd.contact_id
)
select * from dim_study_contact