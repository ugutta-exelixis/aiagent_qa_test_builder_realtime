with enroll_forecast as (
    select distinct
    SUM(cast(planned_subjects as int)) as forecasted_participants,
    country_name ,
    md5(country_name || study_protocol_number) as enroll_forecast_sk
    from {{ref ('ctms_stg__enroll_forecast')}}
    --where month_end_date = (select max(month_end_date) from {{ref ('ctms_stg__enroll_forecast')}})
    --and study_protocol_number = 'XL092-303'
    GROUP BY study_protocol_number,country_name
),
pra_exl_srm_enroll_proj as(
    select distinct
    SUM(planned_subjects) as projected_participants,
    country_name,
    study_protocol_number as study_id,
    md5(country_name || study_protocol_number) as srm_enroll_proj_sk,
    cast(month_end_date as date) month_end_date
     from {{ref ('ctms_stg__srm_enroll_proj')}}
     --where cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ref ('ctms_stg__srm_enroll_proj')}} where study_protocol_number = 'XL092-303')
     --and study_protocol_number = 'XL092-303'
     GROUP BY study_protocol_number,country_name,cast(month_end_date as date)
),

pra_exl_srm_proj_sites as(
    select distinct
    SUM(planned_sites) as "#_of_planned_sites",
    country_name ,
    md5(country_name || study_protocol_number) as srm_proj_sites_sk
     from {{ref ('ctms_stg__srm_proj_sites')}}
     --where month_end_date = (select max(month_end_date) from {{ref ('ctms_stg__srm_proj_sites')}})
     GROUP BY study_protocol_number,country_name
),
pra_exl_study_country as(
    select distinct
    study_country_id,
    md5(study_country_id||study_protocol_number) as study_country_id_sk,
    country_status,
    md5(study_protocol_number) as study_id_sk,
    country_name,
    md5(country_name||study_protocol_number) as exl_study_country_sk,
    md5(study_country_id || study_protocol_number) as exl_study_country_id_sk
    from {{ref ('ctms_stg__study_country')}}
   ),
pra_exl_study_site as(
select distinct
    count(distinct study_site_id) as "#_of_actual_sites",study_protocol_number,
    md5(study_country_id) as exl_country_id_sk,
    md5(study_country_id || study_protocol_number) as exl_study_country_id_sk
   from {{ref ('ctms_stg__study_site')}}
    where study_site_status in ('Activated', 'Active Enrolling') and site_type = 'Main'
    GROUP BY study_protocol_number,exl_study_country_id_sk,study_country_id
    union     
select distinct
    COUNT(distinct sc.site_number) as "#_of_actual_sites", sc.study_id, 
	md5(study_country_id) as exl_country_id_sk, 
    md5(study_country_id || sc.study_id) as exl_study_country_id_sk
    from {{ref ('ctms_stg__study_site_IQVIA')}} sc
    where study_site_status in ('Enrollment Open') 
    group by study_id,exl_study_country_id_sk,study_country_id
),
overall_edc_study_metrics as (
    select distinct
    studyid,
    country_name,
    on_treatment,
    screen_failed_subjects,
    total_randomized_subjects,
    in_survival_fu,
    off_study,
    md5(country_name|| studyid) as edc_study_metrics_sk
 from {{ref ('ctms_stg__overall_edc_study_metrics')}}
),
fct_study_country as (
    select distinct
    sc.study_country_id_sk,
    sc.study_id_sk,
    sc.country_status,
    ep.projected_participants,
    ef.forecasted_participants,
    ps."#_of_planned_sites",
    ss."#_of_actual_sites",
    sm.screen_failed_subjects,
    sm.total_randomized_subjects,
    sm.on_treatment,
    sm.in_survival_fu,
    sm.off_study
    from pra_exl_study_country as sc
    left join pra_exl_srm_enroll_proj as ep
    on sc.exl_study_country_sk=ep.srm_enroll_proj_sk
    left join enroll_forecast as ef
    on sc.exl_study_country_sk=ef.enroll_forecast_sk
    left join pra_exl_srm_proj_sites as ps
    on sc.exl_study_country_sk=ps.srm_proj_sites_sk
    left  join overall_edc_study_metrics as sm
    on sc.exl_study_country_sk=sm.edc_study_metrics_sk
    left join pra_exl_study_site as ss
    on sc.exl_study_country_id_sk=ss.exl_study_country_id_sk
),
fct_study_country_final as (
select distinct
    study_country_id_sk,
    study_id_sk,
    country_status,
    max(projected_participants) as projected_participants,
    max(forecasted_participants) as forecasted_participants,
    max("#_of_planned_sites") as "#_of_planned_sites",
    max("#_of_actual_sites") as "#_of_actual_sites",
    max(screen_failed_subjects) as screen_failed_subjects,
    max(total_randomized_subjects) as total_randomized_subjects,
    max(on_treatment) as on_treatment,
    max(in_survival_fu) as in_survival_fu,
    max(off_study) as off_study
    from fct_study_country
    group by study_country_id_sk,
    study_id_sk,
    country_status
    )
select * from fct_study_country