with ct_portfolio_source as(
    select 
        distinct
        cps."CRO" as cro,
        cps."Study_Id" as study_id,
        md5(cps."Study_Id") as study_id_sk,
        cps."Study Type" as study_type,
        cps."brief_title" as study_title,
        cps."Compound #" as study_drug,
        cps.phase,
        cps."Indication" as indication,
        cps."Study Stage" as study_stage,
        cps."Study State" as study_state,
        cps."study_status" as study_status,
        cps."Active_Flag" as active_flag,
        cps."Compound/Program Name" as "compound/program_name"
    from {{ref ("ct_portfolio_source")}} as cps
),
study_data as(
    select 
    distinct
    md5(ps.study_protocol_number) as study_id_sk,
    ps.study_title,
    ps.study_drug,
    ps.phase,
    ps.therapeutic_area as indication,
    ps.study_protocol_number as study_id,
    ps.study_protocol_number,
    load_date
    from {{ref ('ctms_stg__study')}} as ps

),
srm_enroll_proj as(
    select 
    distinct
    SUM(ep.planned_subjects) AS enrollment_target,
    ep.study_protocol_number
    from {{ ref ('ctms_stg__srm_enroll_proj')}} as ep
    WHERE EXTRACT(YEAR FROM to_date(month_end_date ,'DD-MON-YYYY')) = EXTRACT(YEAR FROM CURRENT_DATE)
    AND EXTRACT(MONTH FROM to_date(month_end_date ,'DD-MON-YYYY')) = EXTRACT(MONTH FROM CURRENT_DATE)
     --to_date(month_end_date ,'DD-MON-YYYY')= CURRENT_MONTH
    group by study_protocol_number
    
),

dim_study as(
    select
        cps.study_id_sk,
        cps.study_id,
        case 
            when cps.study_id in ('XL092-304','XL092-009','XL092-305') then cps.study_title
            else sd.study_title
        end as study_title,
        case 
            when cps.study_id in ('XL092-304','XL092-009','XL092-305') then cps.study_drug
            else sd.study_drug
        end as study_drug,
        case 
            when cps.study_id in ('XL092-304','XL092-009','XL092-305') then cps.phase
            else sd.phase
        end as phase,
        case 
            when cps.study_id in ('XL092-304','XL092-009','XL092-305') then initcap(cps.indication)
            else initcap(sd.indication)
        end as indication,
        cast(cps.cro as varchar(255)) as cro,
        cast(cps.study_type as varchar(255)) as study_type,
        cast(cps.study_stage as varchar(255)) as study_stage,
        cast(cps.study_state as varchar(255)) as study_state,
        cast(cps.study_status as varchar(255)) as study_status,
        cast(cps.active_flag as boolean) as active_flag,
        cast(cps."compound/program_name" as varchar(255)) AS "compound/program_name",
        Null as franchise,
        to_date(sd.load_date,'YYYY-MM-DD') AS load_date,
        current_date as aud_created_date,
        current_date as aud_updated_date
    from ct_portfolio_source as cps
    left join study_data as sd
    on cps.study_id=sd.study_id
    left join srm_enroll_proj as sep
    on sep.study_protocol_number=sd.study_protocol_number
)
select * from dim_study 