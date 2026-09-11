with ctms_stg__srm_enroll_proj as (
select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_subjects
,partition_date as load_date
,md5(study_protocol_number||COALESCE(phase,'phase')||COALESCE(cohort,'cohort')||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xb002-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xb002-101') }})
    and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xb002-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xb002-101') }}))
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xb002-101') }})

union all

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_subjects
,partition_date as load_date
,md5(study_protocol_number||COALESCE(phase,'phase')||COALESCE(cohort,'cohort')||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl092-001') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl092-001') }})
    and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl092-001') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl092-001') }}))
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl092-001') }})
union all

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_subjects
,partition_date as load_date
,md5(study_protocol_number||COALESCE(phase,'phase')||COALESCE(cohort,'cohort')||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl092-002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl092-002') }})
    and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl092-002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl092-002') }}))
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl092-002') }})

union all

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_subjects
,partition_date as load_date
,md5(study_protocol_number||COALESCE(phase,0)||COALESCE(cohort,'cohort')||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl092-303') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl092-303') }})
    and cast(month_end_date as date) = (select max(cast(month_end_date as date))from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl092-303') }} 
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl092-303') }}))
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl092-303') }})

union all

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_subjects
,partition_date as load_date
,md5(study_protocol_number||COALESCE(phase,'phase')||COALESCE(cohort,'cohort')||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl114-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl114-101') }})
    and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl114-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl114-101') }}))
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl114-101') }})

union all

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_subjects
,partition_date as load_date
,md5(study_protocol_number||COALESCE(phase,0)||COALESCE(cohort,0)||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-021') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-021') }})
    and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-021') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-021') }}))
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-021') }})

union all

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_subjects
,partition_date as load_date
,md5(study_protocol_number||COALESCE(phase,0)||COALESCE(cohort,0)||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-311') }})
    and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-311') }}))
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-311') }})

union all

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_subjects
,partition_date as load_date
,md5(study_protocol_number||COALESCE(phase,0)||COALESCE(cohort,0)||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-312') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-312') }})
    and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-312') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-312') }}))
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-312') }})

union all

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_subjects
,partition_date as load_date
,md5(study_protocol_number||COALESCE(phase,0)||COALESCE(cohort,0)||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-315') }})
    and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-315') }}))
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xl184-315') }})

union all

select distinct
cast(study_protocol_number as varchar) as  study_protocol_number
,cast(phase as varchar) as  phase
,cast(cohort as varchar) as  cohort
,REPLACE(upper(country_name), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country_name
,cast(month_end_date as varchar) as  month_end_date
,planned_subjects
,partition_date as load_date
,md5(study_protocol_number||COALESCE(phase,'phase')||COALESCE(cohort,'cohort')||country_name||month_end_date) as hash_key
FROM
    {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xb010-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xb010-101') }})
    and cast(month_end_date as date) = (select max(cast(month_end_date as date)) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xb010-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xb010-101') }}))
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'pra_exl_srm_enroll_proj_xb010-101') }})
)

select * from ctms_stg__srm_enroll_proj