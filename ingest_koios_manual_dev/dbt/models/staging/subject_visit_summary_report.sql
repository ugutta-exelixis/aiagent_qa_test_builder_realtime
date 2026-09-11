
with subject_visit_summary_report as (
select
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country,
cast(study_id as varchar) as  study_id
,subject
,"visit description"
,"actual date [local]" as "actual date"
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xb002_101') }}
    where partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xb002_101') }})

UNION

select
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country,
cast(study_id as varchar) as  study_id
,subject
,"visit description"
,"actual date"
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xl092_001') }}
    where partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl092_001') }})

UNION

select
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country,
cast(study_id as varchar) as  study_id
,subject
,"visit description"
,"actual date [local]" as "actual date"
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xl092_002') }}
    where partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl092_002') }})

UNION

select
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country,
cast(study_id as varchar) as  study_id
,subject
,"visit description"
,"actual date [local]" as "actual date"
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xl092_303') }}
    where partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl092_303') }})

UNION

select
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country,
cast(study_id as varchar) as  study_id
,subject
,"visit description"
,"actual date" 
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xl184_315') }}
    where partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl184_315') }})

)

select * from subject_visit_summary_report

