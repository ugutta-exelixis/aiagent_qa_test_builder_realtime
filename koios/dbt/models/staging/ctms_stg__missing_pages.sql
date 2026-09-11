with ctms_stg__missing_pages as
(
select 
cast("priority crf" as varchar) as priority_crf
,null as "critical page y/n"
,REPLACE(upper(cast(country as varchar)), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,cast("study stage" as varchar) as study_stage
,cast("enroll or screen fail date" as varchar) as enroll_or_screen_fail_date
,null as cohort
,cast("treatment arm" as varchar) as treatment_arm
,null as sdvtier
,cast("visit name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("log line" as varchar) as log_line
,cast("expected date" as varchar) as expected_date
,cast("team comments" as varchar) as comments
,cast("days overdue" as varchar) as days_overdue
,cast("overdue timeframe" as varchar) as overdue_timeframe
,null as unique_id
,cast("studyid" as varchar) as studyid
,last_modified_date as load_date
from 
{{ source ('koios_raw', 'xb002-101_missing_pages') }}
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xb002-101_missing_pages') }})
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xb002-101_missing_pages') }})
union all
select 
null as priority_crf
,cast("critical page y/n" as varchar) as "critical page y/n"
,REPLACE(upper(cast(country as varchar)), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,null as study_stage
,cast("enrolled randomized or screen fail date date" as varchar) as enroll_or_screen_fail_date
,cast("cohort" as varchar) as cohort
,cast("treatment arm" as varchar) as treatment_arm
,null as sdvtier
,cast("visit name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("log line" as varchar) as log_line
,cast("expected date" as varchar) as expected_date
,null as comments
,cast("days overdue" as varchar) as days_overdue
,cast("overdue timeframe" as varchar) as overdue_timeframe
,null as unique_id
,cast("studyid" as varchar) as studyid
,last_modified_date as load_date
from 
{{ source ('koios_raw', 'xl092-002_missing_pages') }}
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-002_missing_pages') }})
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-002_missing_pages') }})
union all
select 
null as priority_crf
,cast( "critical page y/n" as varchar) as "critical page y/n"
,REPLACE(upper(cast(country as varchar)), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,null as study_stage
,cast("rand or screen fail date" as varchar) as enroll_or_screen_fail_date
,null as cohort
,cast("treatment arm" as varchar) as treatment_arm
,cast("sdvtier" as varchar) as sdvtier
,cast("visit name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("log line" as varchar) as log_line
,cast("expected date" as varchar) as expected_date
,cast("dm comments" as varchar) as comments
,cast("days overdue" as varchar) as days_overdue
,cast("overdue timeframe" as varchar) as overdue_timeframe
,null as unique_id
,cast("studyid" as varchar) as studyid
,last_modified_date as load_date
from 
{{ source ('koios_raw', 'xl092-303_missing_pages') }}
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-303_missing_pages') }})
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-303_missing_pages') }})
union all 
select 
null as priority_crf
,cast( "critical page y/n" as varchar) as "critical page y/n"
,REPLACE(upper(cast(country as varchar)), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,null as study_stage
,cast("rand or screen fail date" as varchar) as enroll_or_screen_fail_date
,null as cohort
,cast("treatment arm" as varchar) as treatment_arm
,null as sdvtier
,cast("visit name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("log line" as varchar) as log_line
,cast("expected date" as varchar) as expected_date
,null as comments
,cast("days overdue" as varchar) as days_overdue
,cast("overdue timeframe" as varchar) as overdue_timeframe
,cast("unique id" as varchar) as unique_id
,cast("studyid" as varchar) as studyid
,last_modified_date as load_date
from
{{ source ('koios_raw', 'xl092-304_missing_pages') }}
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-304_missing_pages') }})
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-304_missing_pages') }})
union all
select 
null as priority_crf
,null as "critical page y/n"
,REPLACE(upper(cast( country as varchar)), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,null as study_stage
,cast("rand or screen fail date" as varchar) as enroll_or_screen_fail_date
,cohort as cohort
,null as treatment_arm
,null as sdvtier
,cast("visit name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("log line" as varchar) as log_line
,cast("expected date" as varchar) as expected_date
,null as comments
,cast("days overdue" as varchar) as days_overdue
,cast("overdue timeframe" as varchar) as overdue_timeframe
,null as unique_id
,cast("studyid" as varchar) as studyid
,last_modified_date as load_date
from
{{ source ('koios_raw', 'xl092-009_missing_pages') }}
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-009_missing_pages') }})
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-009_missing_pages') }})

union all

select 
null as priority_crf
,null as "critical page y/n"
,REPLACE(upper(cast( country as varchar)), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,null as study_stage
,cast("randomized/ screen fail date" as varchar) as enroll_or_screen_fail_date
,null as cohort
,null as treatment_arm
,null as sdvtier
,cast("visit name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("log line" as varchar) as log_line
,cast("expected date" as varchar) as expected_date
,null as comments
,cast("days overdue" as varchar) as days_overdue
,cast("overdue timeframe" as varchar) as overdue_timeframe
,null as unique_id
,cast("studyid" as varchar) as studyid
,last_modified_date as load_date
from
{{ source ('koios_raw', 'xl092-305_missing_pages') }}
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-305_missing_pages') }})
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-305_missing_pages') }})

union all

select 
null as priority_crf
,null as "critical page y/n"
,REPLACE(upper(cast( country as varchar)), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,null as study_stage
,cast("enrolled/randomized/ screen fail date" as varchar) as enroll_or_screen_fail_date
,null as cohort
,cast("treatment arm" as varchar) as treatment_arm
,null as sdvtier
,cast("visit name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("log line" as varchar) as log_line
,cast("expected date" as varchar) as expected_date
,null as comments
,cast("days overdue" as varchar) as days_overdue
,cast("overdue timeframe" as varchar) as overdue_timeframe
,null as unique_id
,cast("studyid" as varchar) as studyid
,last_modified_date as load_date
from
{{ source ('koios_raw', 'xb010-101_missing_pages') }}
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xb010-101_missing_pages') }})
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xb010-101_missing_pages') }})

union all

select 
null as priority_crf
,null as "critical page y/n"
,REPLACE(upper(cast( country as varchar)), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,null as study_stage
,cast("enroll/rand|or screen fail|date" as varchar) as enroll_or_screen_fail_date
,null as cohort
,cast("treatment description arm" as varchar) as treatment_arm
,null as sdvtier
,cast("visit name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("log line" as varchar) as log_line
,cast("expected date" as varchar) as expected_date
,null as comments
,cast("days overdue" as varchar) as days_overdue
,cast("overdue timeframe" as varchar) as overdue_timeframe
,null as unique_id
,cast("studyid" as varchar) as studyid
,last_modified_date as load_date
from
{{ source ('koios_raw', 'xl495-101_missing_pages') }}
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl495-101_missing_pages') }})
and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl495-101_missing_pages') }})
)
select * from ctms_stg__missing_pages