with dim_qc_report as (
    select 
    pm.project,
    pm.project_type,
    pm.is_public_version,
    pm.line_identifier,
    pm.name,
    pm.activity_type,
    pm.activity_type_actual,
    pm.protocol_number,
    pm.study_phase_norm,
    pm.cohort,
    pm.planned_finish,
    pm.actual_finish,
    pm.approved_baseline,
    pm.original_baseline,
    pm.load_date,
    pm.last_modified_date,
    pm.status,
    pm.is_required,
    pm.protocol_activity_type_actual_sk,
    pm.protocol_number_sk,
    pm.protocol_no_study_phase_sk
    from {{ref ('ctms_stg__planisware_qc_model')}} as pm
    where pm.status in ('missing','duplicate')
    order by pm.protocol_number)
    select * from dim_qc_report