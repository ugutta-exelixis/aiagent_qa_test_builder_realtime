with study_country as(
    select distinct
    study_protocol_number as study_id,
    country_name,
    study_country_id,
    country_status,
    load_date
    from {{ ref ('ctms_stg__study_country') }}
),

country_region_mapping as (
    select 
    country as country_name
    ,cast(1000+dense_rank() over (order by country) as varchar) as study_country_id
    ,region as country_region
    from {{ ref('stg_country_region_mapping') }}
),

dim_study_country as (
    select
    md5(sc.study_id) as study_id_sk,
    md5(sc.study_country_id||sc.study_id) as study_country_id_sk,
    sc.study_id,
    sc.study_country_id,
    sc.country_name,
    sc.country_status,
    cast(rm.country_region as varchar(255)) as country_region,
    to_date(sc.load_date,'YYYY-MM-DD') as load_date,
    current_date as aud_created_date,
    current_date as aud_updated_date
    from study_country sc
    inner join country_region_mapping rm
    on sc.country_name = rm.country_name
)

select * from dim_study_country
