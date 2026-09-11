with planisware_data as (
select
	cast("project" as VARCHAR) as project,
	cast("project type" as VARCHAR) as project_type,
	cast("is public version" as VARCHAR) as is_public_version,
	cast("line identifier" as VARCHAR) as line_identifier,
	cast("name" as VARCHAR) as name,
	cast("activity type" as VARCHAR) as activity_type,
	case when SPLIT_PART("activity type", '-', 1) ='LPLV' then 'CCO' else SPLIT_PART("activity type", '-', 1) end as activity_type_actual,
	md5(concat_ws('|',(SPLIT_PART("activity type", '-', 1)),"protocol number")) as protocol_activity_type_actual_sk,
	case
		when "protocol number" ~ '(exp|esc)$'
        then SUBSTRING("protocol number" from '^(.*?)(exp|esc)')
		else "protocol number"
	end as protocol_number,
	case
		when "protocol number" like '%exp' then 'Expansion'
		when "protocol number" like '%esc' then 'Escalation'
		else 'empty'
	end as study_phase_norm,
	cast("cohort" as VARCHAR) as cohort,
	case 
    when "planned finish" ~ '^\d{2}/\d{2}/\d{4}$' then "planned finish"
    else to_char(to_date("planned finish", 'MM/DD/YY'), 'MM/DD/YYYY')
    end as planned_finish,  
	cast("approved baseline" as VARCHAR) as approved_baseline,
  case 
    when "actual finish" ~ '^\d{2}/\d{2}/\d{4}$' then "actual finish"
    else to_char(to_date("actual finish", 'MM/DD/YY'), 'MM/DD/YYYY')
  end as actual_finish,
    cast("original baseline" as VARCHAR) AS original_baseline,
	cast("send to onepager 5? (koios)" AS VARCHAR) as is_required, 
	last_modified_date,
	partition_date as load_date
	--from koios_raw.planisware_milestones
	FROM {{ source ('koios_raw', 'planisware_milestones') }}
where 
--"protocol number" not in ('XL495-101esc','XL495-101exp','XL184-021','XL184-401','XL184-311','XL184-312','XL184-313','XL184-315') and
	last_modified_date = (select MAX(last_modified_date) from {{ source ('koios_raw', 'planisware_milestones') }})
    --last_modified_date ='2025-06-18 22:35:07.000'
	and partition_date = (select MAX(partition_date) from {{ source ('koios_raw', 'planisware_milestones') }})
    --and partition_date = '2025-06-19'
	--and "send to onepager 5? (koios/portfolio)" in ('YES')
	--and "protocol number" in ('XB010-101esc','XB010-101exp','XL092-304')
      ),
      --select * from planisware_data;
    actual_counts AS (
  SELECT 
    protocol_number,
    study_phase_norm,
    activity_type_actual,
  --  case when activity_type_actual ='LPLV' then 'CCO'
  --  else activity_type_actual
  --  end as activity_type_actual,
    --activity_type,
    COUNT(*) AS actual_count
  FROM planisware_data
  where protocol_number ~ '^X[A-Z][0-9]{3}-[0-9]{3}$'
  and protocol_number not in (select distinct protocol_number from koios_staging.planisware_exs_ref where state='blocked')
  --where protocol_number in ('XL092-305')
  GROUP BY protocol_number,study_phase_norm,activity_type_actual
  --protocol_activity_type_actual_sk
),
--select * from actual_counts;
 
-- Find new activity types in actual data not present in reference
new_activities AS (
  SELECT 
    ac.protocol_number,
    ac.study_phase_norm,
    ac.activity_type_actual,
    ac.actual_count,
    'new' AS status
  FROM actual_counts ac
  left join koios_staging.planisware_exs_ref pe
    ON ac.protocol_number = pe.protocol_number 
    and ac.study_phase_norm= pe.study_phase
    AND ac.activity_type_actual = pe.milestones
    --and pe.expected_count = ac.actual_count
  WHERE pe.protocol_number IS NULL
  --and pe.protocol_number in ('XB010-101','XL092-304')
  -- pe.protocol_number IS NULL
  --AND pe.protocol_number in ('XB010-101')
),

 --select * from new_activities;
-- Find duplicate activity types where actual count > expected count
duplicate_activities AS (
  SELECT 
    ac.protocol_number,
    ac.study_phase_norm,
    ac.activity_type_actual,
    ac.actual_count,
    'duplicate' AS status
  FROM actual_counts ac
  join koios_staging.planisware_exs_ref pe
    ON ac.protocol_number = pe.protocol_number 
    AND ac.activity_type_actual = pe.milestones
    and ac.study_phase_norm= pe.study_phase
    --and pe.expected_count = ac.actual_count
  WHERE ac.actual_count > pe.expected_count
  --and pe.protocol_number in ('XB010-101','XL092-304')
),
 
-- Find missing activity types (expected in reference but missing in actual)
missing_activities AS (
  SELECT 
    pe.protocol_number,
    pe.study_phase,
    pe.milestones,
    pe.expected_count,
    'missing' AS status
  FROM koios_staging.planisware_exs_ref pe
  left join actual_counts ac
    ON ac.protocol_number = pe.protocol_number 
    AND ac.activity_type_actual = pe.milestones
    and ac.study_phase_norm= pe.study_phase
    --and pe.expected_count = ac.actual_count
    --WHERE ac.actual_count < pe.expected_count
    where ac.protocol_number is null
    and pe.protocol_number not in (select distinct protocol_number from koios_staging.planisware_exs_ref where state='blocked')
    --and pe.protocol_number in ('XB010-101','XL092-304')
),
 
-- Ok activities where actual matches expected
ok_activities AS (
  SELECT 
    ac.protocol_number,
    ac.study_phase_norm,
    ac.activity_type_actual,
    ac.actual_count,
    'ok' AS status
  FROM actual_counts ac
  JOIN koios_staging.planisware_exs_ref pe
    ON ac.protocol_number = pe.protocol_number 
    and ac.study_phase_norm= pe.study_phase
    AND ac.activity_type_actual = pe.milestones
    and ac.actual_count = pe.expected_count
  WHERE ac.actual_count = pe.expected_count
  --and pe.protocol_number in ('XB010-101','XL092-304')
),
-- Combine results
req_protocols as (
SELECT * FROM new_activities
UNION ALL
SELECT * FROM duplicate_activities
UNION ALL
SELECT * FROM missing_activities
UNION ALL
SELECT * FROM ok_activities
order by protocol_number)

--select * from req_protocols

(select pd.project,pd.project_type,pd.is_public_version,pd.line_identifier,pd.name, pd.activity_type ,pd.activity_type_actual,
-- case when pd.activity_type_actual='LPLV' then 'CCO' else pd.activity_type_actual end as activity_type_actual,
pd.protocol_number,pd.study_phase_norm, pd.cohort,pd.planned_finish,pd.approved_baseline,pd.actual_finish,pd.original_baseline,
pd.last_modified_date,pd.load_date,pd.is_required,md5(pd.protocol_number) as protocol_number_sk,
md5(concat_ws('|',pd.protocol_number,pd.study_phase_norm)) as protocol_no_study_phase_sk,
pd.protocol_activity_type_actual_sk,rp.status 
from planisware_data pd join req_protocols rp on pd.protocol_number=rp.protocol_number and 
pd.study_phase_norm= rp.study_phase_norm and pd.activity_type_actual = rp.activity_type_actual
order by protocol_number)
union ALL
(select null as project, null as project_type, null as is_public_version, null as line_identifier, null as name, 
null as activity_type,milestones,protocol_number,study_phase,null as cohort, 
null as planned_finish, null as approved_baseline, null as actual_finish, null as original_baseline, 
(select max(last_modified_date) from planisware_data) as last_modified_date,
(select max(load_date) from planisware_data) as load_date,
null as is_required,null as protocol_number_sk, null as protocol_no_study_phase_sk, null as protocol_activity_type_actual_sk,
status from missing_activities)
union all
  (select null as project, null as project_type, null as is_public_version, null as line_identifier, null as name, 
   null as activity_type,milestones,protocol_number,study_phase,null as cohort, 
   planned_date, null as approved_baseline, actual_date, null as original_baseline, 
  (select max(last_modified_date) from planisware_data) as last_modified_date,
  (select max(load_date) from planisware_data) as load_date,
   null as is_required,
   md5(protocol_number) as protocol_number_sk,null as protocol_no_study_phase_sk, null as protocol_activity_type_actual_sk,
   'ok' as status
   from koios_staging.planisware_exs_ref where 
   state='blocked')
