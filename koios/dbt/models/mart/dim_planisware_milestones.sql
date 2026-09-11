WITH today_duplicates AS (
  SELECT 
    protocol_number,
    MAX(load_date) AS today_load
  FROM {{ref ('ctms_stg__planisware_model')}}
  WHERE 
    status in ('duplicate','missing')
    --and protocol_number ~ '^X[A-Z][0-9]{3}-[0-9]{3}$'
  GROUP BY 
    protocol_number
),
valid_prior_loads AS (
  SELECT 
    pdh.protocol_number,
    pdh.load_date
  FROM 
      {{ref ('planisware_daily_hist')}} pdh
  JOIN 
    today_duplicates td
    ON pdh.protocol_number = td.protocol_number
  WHERE 
    pdh.load_date < td.today_load
  GROUP BY 
    pdh.protocol_number, pdh.load_date
  HAVING 
    SUM(CASE WHEN pdh.status in ('duplicate','missing') THEN 1 ELSE 0 END) = 0  -- exclude any load_date with 'duplicate' or 'missing'
),
latest_valid_prior_loads AS (
  SELECT 
    protocol_number,
    MAX(load_date) AS recent_valid_load_date
  FROM 
    valid_prior_loads
  GROUP BY 
    protocol_number
),
clean_data as (
  SELECT
    pm.project,pm.project_type,pm.is_public_version,pm.line_identifier,pm.name,pm.activity_type,pm.activity_type_actual,
    pm.protocol_number,pm.study_phase_norm,pm.cohort
    -- ,to_char(to_date(planned_finish, 'MM/DD/YY'), 'MM/DD/YYYY') AS planned_finish
    ,planned_finish  as planned_finish
    ,actual_finish as actual_finish
    -- ,to_char(to_date(actual_finish, 'MM/DD/YY'), 'MM/DD/YYYY') AS actual_finish
    ,pm.approved_baseline,pm.original_baseline,
    pm.load_date,pm.last_modified_date,pm.status,pm.is_required,pm.protocol_activity_type_actual_sk,pm.protocol_number_sk,
    pm.protocol_no_study_phase_sk,
    case when pm.activity_type_actual='FPA' then 'Final Protocol Approved'
    when pm.activity_type_actual='FSA' then 'First Site Activation'
    when pm.activity_type_actual='FPI' and pm.study_phase_norm ='Escalation' then 'First Participant In (FPI) Escalation'
    when pm.activity_type_actual='FPI' and pm.study_phase_norm ='Expansion' then 'First Participant In (FPI) Expansion'
    when pm.activity_type_actual='FPI' then 'First Participant In'
    when pm.activity_type_actual='LPI' and pm.study_phase_norm ='Escalation' then 'Last Participant In (LPI) Escalation'
    when pm.activity_type_actual='LPI' and pm.study_phase_norm ='Expansion' then 'Last Participant In (LPI) Expansion'
    when pm.activity_type_actual='LPI' then 'Last Participant In'
    when pm.activity_type_actual='DBL' then 'Data Base Lock'
    when pm.activity_type_actual='CSR' then 'Clinical Study Report'
    when pm.activity_type_actual='TFL' then 'Tables, Figures & Listings'
    when pm.activity_type_actual='CCO' and pm.study_phase_norm ='Expansion' then 'Clinical Cutoff (CCO) Expansion'
    when pm.activity_type_actual='CCO' and pm.study_phase_norm ='Escalation' then 'Clinical Cutoff (CCO) Escalation'
    when pm.activity_type_actual='CCO' then 'Clinical Cutoff'
    when pm.activity_type_actual='PSA' then 'Protocol Synopsis Approved'
    when pm.activity_type_actual='TLR' then 'Top Line Results'
    --when pm.activity_type_actual='LPLV' then 'Last Patient Last Visit'
    when pm.activity_type_actual='FPD' then 'First Participant Dosed'
    else pm.activity_type_actual
    end as vendor_milestone_name
  FROM 
    {{ref ('ctms_stg__planisware_model')}} AS pm
    left join today_duplicates cm on pm.protocol_number = cm.protocol_number 
    where cm.protocol_number is null
    union 
  select
    pdh.project,pdh.project_type,pdh.is_public_version,pdh.line_identifier,pdh.name,pdh.activity_type,pdh.activity_type_actual,
    pdh.protocol_number,pdh.study_phase_norm,pdh.cohort
    -- ,to_char(to_date(pdh.planned_finish, 'MM/DD/YY'), 'MM/DD/YYYY') AS planned_finish
    ,pdh.planned_finish as planned_finish
    -- ,to_char(to_date(pdh.actual_finish, 'MM/DD/YY'), 'MM/DD/YYYY') AS actual_finish
    ,pdh.actual_finish AS actual_finish
    ,pdh.approved_baseline,pdh.original_baseline,
    pdh.load_date,pdh.last_modified_date,pdh.status,pdh.is_required,pdh.protocol_activity_type_actual_sk,pdh.protocol_number_sk,
    pdh.protocol_no_study_phase_sk,
    case when pdh.activity_type_actual='FPA' then 'Final Protocol Approved'
    when pdh.activity_type_actual='FSA' then 'First Site Activation'
    when pdh.activity_type_actual='FPI' and pdh.study_phase_norm ='Escalation' then 'First Participant In (FPI) Escalation'
    when pdh.activity_type_actual='FPI' and pdh.study_phase_norm ='Expansion' then 'First Participant In (FPI) Expansion'
    when pdh.activity_type_actual='FPI' then 'First Participant In'
    when pdh.activity_type_actual='LPI' and pdh.study_phase_norm ='Escalation' then 'Last Participant In (LPI) Escalation'
    when pdh.activity_type_actual='LPI' and pdh.study_phase_norm ='Expansion' then 'Last Participant In (LPI) Expansion'
    when pdh.activity_type_actual='LPI' then 'Last Participant In'
    when pdh.activity_type_actual='DBL' then 'Data Base Lock'
    when pdh.activity_type_actual='CSR' then 'Clinical Study Report'
    when pdh.activity_type_actual='TFL' then 'Tables, Figures & Listings'
    when pdh.activity_type_actual='CCO' and pdh.study_phase_norm ='Expansion' then 'Clinical Cutoff (CCO) Expansion'
    when pdh.activity_type_actual='CCO' and pdh.study_phase_norm ='Escalation' then 'Clinical Cutoff (CCO) Escalation'
    when pdh.activity_type_actual='CCO' then 'Clinical Cutoff'
    when pdh.activity_type_actual='PSA' then 'Protocol Synopsis Approved'
    when pdh.activity_type_actual='TLR' then 'Top Line Results'
    --when pdh.activity_type_actual='LPLV' then 'Last Patient Last Visit'
    when pdh.activity_type_actual='FPD' then 'First Participant Dosed'
    else pdh.activity_type_actual
    end as vendor_milestone_name 
    from
    {{ref ('planisware_daily_hist')}} pdh
JOIN 
  latest_valid_prior_loads lpl
  ON pdh.protocol_number = lpl.protocol_number
  AND pdh.load_date = lpl.recent_valid_load_date),

	dual_data as (select
    project,project_type,is_public_version,line_identifier,name,activity_type,activity_type_actual,
    protocol_number,study_phase_norm,cohort,planned_finish,actual_finish,approved_baseline,original_baseline,load_date,
    last_modified_date,status,is_required,protocol_activity_type_actual_sk,protocol_number_sk,protocol_no_study_phase_sk,
    vendor_milestone_name,row_number() over (partition by protocol_number, activity_type_actual order by study_phase_norm desc) as ranks
	from clean_data where 
    -- protocol_number ~ '^X[A-Z][0-9]{3}-[0-9]{3}$' and 
    activity_type_actual not in ('FPI','LPI', 'CCO')),
  
  final_data as (
	select project,project_type,is_public_version,line_identifier,name,activity_type,activity_type_actual,
    protocol_number,study_phase_norm,cohort,planned_finish,actual_finish,approved_baseline,original_baseline,
    load_date,last_modified_date,status,is_required,protocol_activity_type_actual_sk,protocol_number_sk,
    protocol_no_study_phase_sk,vendor_milestone_name from clean_data where 
    -- protocol_number ~ '^X[A-Z][0-9]{3}-[0-9]{3}$' and 
    activity_type_actual in ('FPI','LPI', 'CCO')
	union all
    select project,project_type,is_public_version,line_identifier, name, 
	activity_type,activity_type_actual,protocol_number,study_phase_norm,cohort, 
	planned_finish, actual_finish, approved_baseline, original_baseline, 
	load_date,last_modified_date, status,
	is_required, protocol_activity_type_actual_sk,protocol_number_sk, protocol_no_study_phase_sk,vendor_milestone_name from 
  dual_data where ranks=1)

  select fd.project,fd.project_type,fd.is_public_version,fd.line_identifier,fd.name,fd.activity_type,fd.activity_type_actual,
    fd.protocol_number,fd.study_phase_norm,fd.cohort,fd.planned_finish,fd.actual_finish,fd.approved_baseline,fd.original_baseline,
    fd.load_date,fd.last_modified_date,fd.status,fd.is_required,fd.protocol_activity_type_actual_sk,fd.protocol_number_sk,
    fd.protocol_no_study_phase_sk,fd.vendor_milestone_name,rd.trial_identifier
    from final_data fd left join koios_staging.planisware_exs_ref rd on fd.protocol_number=rd.protocol_number
  and fd.study_phase_norm=rd.study_phase and fd.activity_type_actual=rd.milestones
