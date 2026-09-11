{{ config(
    materialized='incremental'
) }}
    SELECT 
    project,
    project_type,
    is_public_version,
    line_identifier,
    name,
    activity_type,
    activity_type_actual,
    protocol_number,
    study_phase_norm,
    cohort,
    -- to_char(to_date(planned_finish, 'MM/DD/YY'), 'MM/DD/YYYY') AS planned_finish,
    planned_finish as planned_finish,
    -- to_char(to_date(actual_finish, 'MM/DD/YY'), 'MM/DD/YYYY') AS actual_finish,
    actual_finish as actual_finish,
    approved_baseline,
    original_baseline,
    load_date,
    last_modified_date,
    status,
    is_required,
    protocol_activity_type_actual_sk,
    protocol_number_sk,
    protocol_no_study_phase_sk
    from {{ref ('ctms_stg__planisware_model')}}

    {% if is_incremental() %}
       where load_date > (select max(load_date) from {{ this }})
    -- {% else %}
    --     SELECT NULL AS project, NULL AS description, NULL AS  milestone, NULL AS protocol_number, NULL AS study_phase, NULL AS planned_finish, NULL AS actual_finish, NULL AS approved_baseline, NULL AS original_baseline, NULL AS hashkey, NULL AS load_date
    --     LIMIT 0
    {% endif %}

