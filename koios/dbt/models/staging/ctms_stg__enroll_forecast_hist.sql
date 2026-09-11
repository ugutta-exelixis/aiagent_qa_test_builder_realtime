
WITH ctms_stg__enroll_forecast AS (
SELECT distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,cast(planned_subjects as varchar) as  planned_subjects
,partition_date as load_date
,md5("study_protocol_number"||COALESCE(CAST("phase" AS VARCHAR),'PHASE')||coalesce(CAST("cohort" AS VARCHAR),'COHORT')||"country_name"||"month_end_date") as HASH_KEY
FROM
    {{ source ('koios_raw', 'enroll_forecast_xb002-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'enroll_forecast_xb002-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'enroll_forecast_xb002-101') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'enroll_forecast_xb002-101') }})

union

SELECT distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,cast(planned_subjects as varchar) as  planned_subjects
,partition_date as load_date
,md5("study_protocol_number"||COALESCE(CAST("phase" AS VARCHAR),'PHASE')||coalesce(CAST("cohort" AS VARCHAR),'COHORT')||"country_name"||"month_end_date") as HASH_KEY
FROM
    {{ source ('koios_raw', 'enroll_forecast_xl092-002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'enroll_forecast_xl092-002') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'enroll_forecast_xl092-002') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'enroll_forecast_xl092-002') }})

union

SELECT distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,cast(planned_subjects as varchar) as  planned_subjects
,partition_date as load_date
,md5("study_protocol_number"||COALESCE(CAST("phase" AS VARCHAR),'PHASE')||coalesce(CAST("cohort" AS VARCHAR),'COHORT')||"country_name"||"month_end_date") as HASH_KEY
FROM
    {{ source ('koios_raw', 'enroll_forecast_xl092-303') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'enroll_forecast_xl092-303') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'enroll_forecast_xl092-303') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'enroll_forecast_xl092-303') }})
    

union

SELECT distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,cast(planned_subjects as varchar) as  planned_subjects
,partition_date as load_date
,md5("study_protocol_number"||COALESCE(CAST("phase" AS VARCHAR),'PHASE')||coalesce(CAST("cohort" AS VARCHAR),'COHORT')||"country_name"||"month_end_date") as HASH_KEY
FROM
    {{ source ('koios_raw', 'enroll_forecast_xl102-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'enroll_forecast_xl102-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'enroll_forecast_xl102-101') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'enroll_forecast_xl102-101') }})

union

SELECT distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,cast(planned_subjects as varchar) as  planned_subjects
,partition_date as load_date
,md5("study_protocol_number"||COALESCE(CAST("phase" AS VARCHAR),'PHASE')||coalesce(CAST("cohort" AS VARCHAR),'COHORT')||"country_name"||"month_end_date") as HASH_KEY
FROM
    {{ source ('koios_raw', 'enroll_forecast_xl114-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'enroll_forecast_xl114-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'enroll_forecast_xl114-101') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'enroll_forecast_xl114-101') }})

union 

SELECT distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,cast(planned_subjects as varchar) as  planned_subjects
,partition_date as load_date
,md5("study_protocol_number"||COALESCE(CAST("phase" AS VARCHAR),'PHASE')||coalesce(CAST("cohort" AS VARCHAR),'COHORT')||"country_name"||"month_end_date") as HASH_KEY
FROM
    {{ source ('koios_raw', 'enroll_forecast_xl184-021') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'enroll_forecast_xl184-021') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'enroll_forecast_xl184-021') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'enroll_forecast_xl184-021') }})

union

SELECT distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,cast(planned_subjects as varchar) as  planned_subjects
,partition_date as load_date
,md5("study_protocol_number"||COALESCE(CAST("phase" AS VARCHAR),'PHASE')||coalesce(CAST("cohort" AS VARCHAR),'COHORT')||"country_name"||"month_end_date") as HASH_KEY
FROM
    {{ source ('koios_raw', 'enroll_forecast_xl184-311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'enroll_forecast_xl184-311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'enroll_forecast_xl184-311') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'enroll_forecast_xl184-311') }})

union

SELECT distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,cast(planned_subjects as varchar) as  planned_subjects
,partition_date as load_date
,md5("study_protocol_number"||COALESCE(CAST("phase" AS VARCHAR),'PHASE')||coalesce(CAST("cohort" AS VARCHAR),'COHORT')||"country_name"||"month_end_date") as HASH_KEY
FROM
    {{ source ('koios_raw', 'enroll_forecast_xl184-312') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'enroll_forecast_xl184-312') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'enroll_forecast_xl184-312') }})
    --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'enroll_forecast_xl184-312') }})

union

SELECT distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,cast(planned_subjects as varchar) as  planned_subjects
,partition_date as load_date
,md5("study_protocol_number"||COALESCE(CAST("phase" AS VARCHAR),'PHASE')||coalesce(CAST("cohort" AS VARCHAR),'COHORT')||"country_name"||"month_end_date") as HASH_KEY
FROM
    {{ source ('koios_raw', 'enroll_forecast_xl184-315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'enroll_forecast_xl184-315') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'enroll_forecast_xl184-315') }})
   --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'enroll_forecast_xl184-315') }})
union

SELECT distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,cast(planned_subjects as varchar) as  planned_subjects
,partition_date as load_date
,md5("study_protocol_number"||COALESCE(CAST("phase" AS VARCHAR),'PHASE')||coalesce(CAST("cohort" AS VARCHAR),'COHORT')||"country_name"||"month_end_date") as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_srm_enroll_forecast_xb628-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_forecast_xb628-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_forecast_xb628-101') }})
   --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'enroll_forecast_xl184-315') }})

union

SELECT distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,cast(planned_subjects as varchar) as  planned_subjects
,partition_date as load_date
,md5("study_protocol_number"||COALESCE(CAST("phase" AS VARCHAR),'PHASE')||coalesce(CAST("cohort" AS VARCHAR),'COHORT')||"country_name"||"month_end_date") as HASH_KEY
FROM
    {{ source ('koios_raw', 'pra_exl_srm_enroll_forecast_xl092-311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_forecast_xl092-311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_forecast_xl092-311') }})
   --and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'enroll_forecast_xl184-315') }})


)

SELECT * FROM ctms_stg__enroll_forecast
