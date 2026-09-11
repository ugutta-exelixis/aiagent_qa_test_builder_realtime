with ctms_stg__subject_visit_summary_report as (
select
cast(study_id as varchar) as  study_id
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast(studysite as varchar) as  studysite
,cast(investigator as varchar) as  investigator
,cast(subject as varchar) as  subject
,cast("discontinued rollback request approved?" as varchar) as  "discontinued rollback request approved?"
,cast("screen failure rollback request approved?" as varchar) as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,case when "visit description" = 'Enrollment V1' then 'Randomization' else "visit description" end as "visit description"
,cast("scheduled date [local]" as varchar) as  "scheduled date"
,cast("actual date [local]" as varchar) as  "actual date"
,md5(study_id||country||studysite||subject||"visit id"||"actual date [local]") as hash_key
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xb002_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xb002_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xb002_101') }})

UNION all

select
cast(study_id as varchar) as  study_id
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast(studysite as varchar) as  studysite
,cast(investigator as varchar) as  investigator
,cast(subject as varchar) as  subject
,cast("discontinued rollback request approved?" as varchar) as  "discontinued rollback request approved?"
,cast("screen failure rollback request approved?" as varchar) as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,cast("visit description" as varchar) as  "visit description"
,cast("scheduled date" as varchar) as  "scheduled date"
,cast("actual date" as varchar) as  "actual date"
,md5(study_id||country||studysite||subject||"visit id"||"actual date") as hash_key
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xl092_001') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl092_001') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl092_001') }})

UNION all

select
cast(study_id as varchar) as  study_id
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast(studysite as varchar) as  studysite
,cast(investigator as varchar) as  investigator
,cast(subject as varchar) as  subject
,cast("discontinued rollback request approved?" as varchar) as  "discontinued rollback request approved?"
,cast("screen failure rollback request approved?" as varchar) as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,case when "visit description" = 'Enrollment V1' then 'Randomization' else "visit description" end as "visit description"
,cast("scheduled date [local]" as varchar) as  "scheduled date"
,cast("actual date [local]" as varchar) as  "actual date"
,md5(study_id||country||studysite||subject||"visit id"||"actual date [local]") as hash_key
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xl092_002') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl092_002') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl092_002') }})

UNION all

select
cast(study_id as varchar) as  study_id
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast(studysite as varchar) as  studysite
,cast(investigator as varchar) as  investigator
,cast(subject as varchar) as  subject
,cast("discontinued rollback request approved?" as varchar) as  "discontinued rollback request approved?"
,cast("screen failure rollback request approved?" as varchar) as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,cast("visit description" as varchar) as  "visit description"
,cast("scheduled date [local]" as varchar) as  "scheduled date"
,cast("actual date [local]" as varchar) as  "actual date"
,md5(study_id||country||studysite||subject||"visit id"||"actual date [local]") as hash_key
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xl092_303') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl092_303') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl092_303') }})

UNION all

select
cast(study_id as varchar) as  study_id
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast(studysite as varchar) as  studysite
,cast(investigator as varchar) as  investigator
,cast(subject as varchar) as  subject
,cast("discontinued rollback request approved?" as varchar) as  "discontinued rollback request approved?"
,cast("screen failure rollback request approved?" as varchar) as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,cast("visit description" as varchar) as  "visit description"
,cast("scheduled date" as varchar) as  "scheduled date"
,cast("actual date" as varchar) as  "actual date"
,md5(study_id||country||studysite||subject||"visit id"||"actual date") as hash_key
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xl184_315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl184_315') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl184_315') }})

UNION all

select
cast(study_id as varchar) as  study_id
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast(studysite as varchar) as  studysite
,cast(investigator as varchar) as  investigator
,cast(subject as varchar) as  subject
,cast("discontinued rollback request approved?" as varchar) as  "discontinued rollback request approved?"
,cast("screen failure rollback request approved?" as varchar) as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,cast("visit description" as varchar) as  "visit description"
,cast("scheduled date [local]" as varchar) as  "scheduled date"
,cast("actual date [local]" as varchar) as  "actual date"
,md5(study_id||country||studysite||subject||"visit id"||"actual date [local]") as hash_key
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xl092-304') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl092-304') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl092-304') }})

UNION all

select
study_id
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,case when "site number" ='2124' then '1841'
else cast("site number" as varchar) 
end as  studysite
,cast("investigator name" as varchar) as  investigator
,cast("subject number" as varchar) as  subject
,null as  "discontinued rollback request approved?"
,null as  "screen failure rollback request approved?"
,cast(visit  as varchar) as  "visit id"
,"subject status" as "current participant status"
,case 
	when visit = 'Cycle 1 Day 1' then 'Randomization' 
	when visit <> 'Cycle 1 Day 1' and visit like '%Cycle%' then 'Drug Request'
	when visit = 'Subject Status Change' then "subject status"
	else visit 
end as  "visit description"
,cast("expected visit date" as varchar) as  "scheduled date"
,cast(to_char(TO_DATE("visit date",'DD-MON-YYYY'),'DD MON YY')as varchar) as  "actual date"
,md5(study_id||country||"site number"||"subject number"||''||"visit date") as hash_key
from 
{{ source ('koios_raw', 'cenduit_blinded_subject_visit_xl092-009') }}
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'cenduit_blinded_subject_visit_xl092-009') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'cenduit_blinded_subject_visit_xl092-009') }})
   and "visit date" is not null
UNION all

select
study_id
,case when country = 'KOREA (THE REPUBLIC OF)' then 'SOUTH KOREA'
    when country = 'CZECHIA' then 'CZECH REPUBLIC'
    when country = 'UNITED STATES OF AMERICA' then 'UNITED STATES'
    else cast(country as varchar)
    end as  country
,cast("site number" as varchar) as  studysite
,cast("investigator name" as varchar) as  investigator
,cast("subject number" as varchar) as  subject
,null as  "discontinued rollback request approved?"
,null as  "screen failure rollback request approved?"
,cast(visit  as varchar) as  "visit id"
,"subject status" as "current participant status"
,case 
	when visit = 'Cycle 1 Day 1 (Randomization)' then 'Randomization' 
	when visit <> 'Cycle 1 Day 1 (Randomization)' and visit like '%Cycle%' then 'Drug Request'
	when visit = 'Subject Status Change' then "subject status"
	else visit 
end as  "visit description"
,cast("expected visit date" as varchar) as  "scheduled date"
,cast(to_char(TO_DATE("visit date",'DD-MON-YYYY'),'DD MON YY')as varchar) as  "actual date"
,md5(study_id||country||"site number"||"subject number"||''||"visit date") as hash_key
from 
{{ source ('koios_raw', 'cenduit_blinded_subject_visit_xl092-305') }}
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'cenduit_blinded_subject_visit_xl092-305') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'cenduit_blinded_subject_visit_xl092-305') }})
   and "visit date" is not null
)

select * from ctms_stg__subject_visit_summary_report