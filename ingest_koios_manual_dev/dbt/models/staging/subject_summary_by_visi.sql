
with subject_summary_by_visit as 
(
select distinct
studyid,
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country,
"subject number",
"subject status"
,partition_date as load_date
from
    {{ source ('koios_raw', 'xb002-101_subject_summary_by_visit') }}
    where partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xb002-101_subject_summary_by_visit') }})

union

select distinct
studyid,
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country,
"subject number",
"subject status"
,partition_date as load_date
from
    {{ source ('koios_raw', 'xl092-002_subject_summary_by_visit') }}
    where partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-002_subject_summary_by_visit') }})
union

select distinct
studyid,
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country,
"subject number",
"subject status"
,partition_date as load_date
from
    {{ source ('koios_raw', 'xl092-303_subject_summary_by_visit') }}
    where partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-303_subject_summary_by_visit') }})

union

select distinct
studyid,
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country,
"subject number",
"subject status"
,partition_date as load_date
from
    {{ source ('koios_raw', 'xl102-101_subject_summary_by_visit') }}
    where partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl102-101_subject_summary_by_visit') }})

)

select * from subject_summary_by_visit
