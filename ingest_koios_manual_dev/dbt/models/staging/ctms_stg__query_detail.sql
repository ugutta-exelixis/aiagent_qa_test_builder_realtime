
with ctms_stg__query_detail as 
(
select 
Null as "critical_query_(y/n)"
,Null as critical_variable_list
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,cast("enroll or screen fail date" as varchar) as enroll_or_screen_fail_date
,cast("treatment arm" as varchar) as treatment_arm
,cast("visit name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("record id" as varchar) as record_id
,cast("field oid" as varchar) as field_oid
,cast("line id" as varchar) as line_id
,cast("query recipient" as varchar) as query_recipient
,cast("issued by name" as varchar) as issued_by_name
,cast("query status" as varchar) as query_status
,cast("days open" as varchar) as days_open
,cast("days answered" as varchar) as days_answered
,cast("last opened query text" as varchar) as last_opened_query_text
,cast("query response" as varchar) as query_response
,cast("opened date" as varchar) as opened_date
,cast("answered date" as varchar) as answered_date
,cast("answered by" as varchar) as answered_by
,cast("cancelled date" as varchar) as cancelled_date
,cast("cancelled by" as varchar) as cancelled_by
,cast("closed date" as varchar) as closed_date
,cast("closed by" as varchar) as closed_by
,cast("days open to answered" as varchar) as days_open_to_answered
,cast("days answered to closed" as varchar) as days_answered_to_closed
,cast("days open to closed" as varchar) as days_open_to_closed
,cast("query id" as varchar) as query_id
,cast("page link" as varchar) as page_link
,cast("studyid" as varchar) as studyid
,partition_date as load_date
from
    {{ source ('koios_raw', 'xb002-101_query_detail') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xb002-101_query_detail') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xb002-101_query_detail') }})

union all

select
Null as "critical_query_(y/n)"
,Null as critical_variable_list
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,cast("enrollment or screen fail date" as varchar) as enroll_or_screen_fail_date
,Null as treatment_arm
,cast("visit short name" as varchar) as visit_name
,cast("page short name" as varchar) as page_name
,cast("record id" as varchar) as record_id
,Null as field_oid
,cast("line id" as varchar) as line_id
,Null as query_recipient
,cast("issued by" as varchar) as issued_by_name
,cast("query status" as varchar) as query_status
,cast("days open" as varchar) as days_open
,cast("days answered" as varchar) as days_answered
,cast("query text" as varchar) as last_opened_query_text
,cast("query response" as varchar) as query_response
,cast("opened date" as varchar) as opened_date
,Null as answered_date
,Null as answered_by
,Null as cancelled_date
,Null as cancelled_by
,Null as closed_date
,Null as closed_by
,Null as days_open_to_answered
,Null as days_answered_to_closed
,Null as days_open_to_closed
,Null as query_id
,Null as page_link
,cast("studyid" as varchar) as studyid
,partition_date as load_date
from
    {{ source ('koios_raw', 'xl092-001_query_detail') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-001_query_detail') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-001_query_detail') }})


union all

select
cast("critical query (y/n)" as varchar) as "critical_query_(y/n)"
,"critical variable list" as critical_variable_list
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,cast("enrolled randomized or screen fail date date" as varchar) as enroll_or_screen_fail_date
,cast("treatment arm" as varchar) as treatment_arm
,cast("visit name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("record id" as varchar) as record_id
,cast("field oid" as varchar) as field_oid
,cast("line id" as varchar) as line_id
,cast("query recipient" as varchar) as query_recipient
,cast("issued by name" as varchar) as issued_by_name
,cast("query status" as varchar) as query_status
,cast("days open" as varchar) as days_open
,cast("days answered" as varchar) as days_answered
,cast("last opened query text" as varchar) as last_opened_query_text
,cast("query response" as varchar) as query_response
,cast("opened date" as varchar) as opened_date
,cast("answered date" as varchar) as answered_date
,cast("answered by" as varchar) as answered_by
,cast("cancelled date" as varchar) as cancelled_date
,cast("cancelled by" as varchar) as cancelled_by
,cast("closed date" as varchar) as closed_date
,cast("closed by" as varchar) as closed_by
,cast("days open to answered" as varchar) as days_open_to_answered
,cast("days answered to closed" as varchar) as days_answered_to_closed
,cast("days open to closed" as varchar) as days_open_to_closed
,cast("query id" as varchar) as query_id
,Null as page_link
,cast("studyid" as varchar) as studyid
,partition_date as load_date
from
    {{ source ('koios_raw', 'xl092-002_query_detail') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-002_query_detail') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-002_query_detail') }})


union all

select
cast("critical query (y/n)" as varchar) as "critical_query_(y/n)"
,"critical variable list" as critical_variable_list
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,cast("rand or screen fail date" as varchar) as enroll_or_screen_fail_date
,cast("treatment arm" as varchar) as treatment_arm
,cast("visit name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("record id" as varchar) as record_id
,cast("field oid" as varchar) as field_oid
,cast("line id" as varchar) as line_id
,cast("query recipient" as varchar) as query_recipient
,cast("issued by name" as varchar) as issued_by_name
,cast("query status" as varchar) as query_status
,cast("days open" as varchar) as days_open
,cast("days answered" as varchar) as days_answered
,cast("last opened query text" as varchar) as last_opened_query_text
,cast("query response" as varchar) as query_response
,cast("opened date" as varchar) as opened_date
,cast("answered date" as varchar) as answered_date
,cast("answered by" as varchar) as answered_by
,cast("cancelled date" as varchar) as cancelled_date
,cast("cancelled by" as varchar) as cancelled_by
,cast("closed date" as varchar) as closed_date
,cast("closed by" as varchar) as closed_by
,cast("days open to answered" as varchar) as days_open_to_answered
,cast("days answered to closed" as varchar) as days_answered_to_closed
,cast("days open to closed" as varchar) as days_open_to_closed
,cast("query id" as varchar) as query_id
,cast("page link" as varchar) as page_link
,cast("studyid" as varchar) as studyid
,partition_date as load_date
from
    {{ source ('koios_raw', 'xl092-303_query_detail') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-303_query_detail') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-303_query_detail') }})


union all

select
Null as "critical_query_(y/n)"
,Null as critical_variable_list
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,cast("enroll or screen fail date" as varchar) as enroll_or_screen_fail_date
,cast("treatment arm" as varchar) as treatment_arm
,cast("visit name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("record id" as varchar) as record_id
,cast("field oid" as varchar) as field_oid
,cast("line id" as varchar) as line_id
,cast("query recipient" as varchar) as query_recipient
,cast("issued by name" as varchar) as issued_by_name
,cast("query status" as varchar) as query_status
,cast("days open" as varchar) as days_open
,cast("days answered" as varchar) as days_answered
,cast("last opened query text" as varchar) as last_opened_query_text
,cast("query response" as varchar) as query_response
,cast("opened date" as varchar) as opened_date
,cast("answered date" as varchar) as answered_date
,cast("answered by" as varchar) as answered_by
,cast("cancelled date" as varchar) as cancelled_date
,cast("cancelled by" as varchar) as cancelled_by
,cast("closed date" as varchar) as closed_date
,cast("closed by" as varchar) as closed_by
,cast("days open to answered" as varchar) as days_open_to_answered
,cast("days answered to closed" as varchar) as days_answered_to_closed
,cast("days open to closed" as varchar) as days_open_to_closed
,Null as query_id
,cast("page link" as varchar) as page_link
,cast("studyid" as varchar) as studyid
,partition_date as load_date
from
    {{ source ('koios_raw', 'xl102-101_query_detail') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl102-101_query_detail') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl102-101_query_detail') }})

union all

select
cast("critical crf|(y/n)" as varchar) as "critical_query_(y/n)"
,Null as critical_variable_list
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,cast("rand or screen fail date" as varchar) as enroll_or_screen_fail_date
,cast("treatment arm" as varchar) as treatment_arm
,cast("visit|name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("record id" as varchar) as record_id
,cast("field oid" as varchar) as field_oid
,cast("line id" as varchar) as line_id
,cast("query recipient" as varchar) as query_recipient
,cast("issued by name" as varchar) as issued_by_name
,cast("query status" as varchar) as query_status
,cast("days open" as varchar) as days_open
,cast("days answered" as varchar) as days_answered
,cast("last opened query text" as varchar) as last_opened_query_text
,cast("query response" as varchar) as query_response
,cast("opened date" as varchar) as opened_date
,cast("answered date" as varchar) as answered_date
,cast("answered by" as varchar) as answered_by
,cast("cancelled date" as varchar) as cancelled_date
,cast("cancelled by" as varchar) as cancelled_by
,cast("closed date" as varchar) as closed_date
,cast("closed by" as varchar) as closed_by
,cast("days open to answered" as varchar) as days_open_to_answered
,cast("days answered to closed" as varchar) as days_answered_to_closed
,cast("days open to closed" as varchar) as days_open_to_closed
,cast("query id" as varchar) as query_id
,cast("page|link" as varchar) as page_link
,cast("studyid" as varchar) as studyid
,partition_date as load_date
from
    {{ source ('koios_raw', 'xl184-315_query_detail_apac') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl184-315_query_detail_apac') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl184-315_query_detail_apac') }})

union all

select
cast("critical crf|(y/n)" as varchar) as "critical_query_(y/n)"
,Null as critical_variable_list
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,cast("rand or screen fail date" as varchar) as enroll_or_screen_fail_date
,cast("treatment arm" as varchar) as treatment_arm
,cast("visit|name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("record id" as varchar) as record_id
,cast("field oid" as varchar) as field_oid
,cast("line id" as varchar) as line_id
,cast("query recipient" as varchar) as query_recipient
,cast("issued by name" as varchar) as issued_by_name
,cast("query status" as varchar) as query_status
,cast("days open" as varchar) as days_open
,cast("days answered" as varchar) as days_answered
,cast("last opened query text" as varchar) as last_opened_query_text
,cast("query response" as varchar) as query_response
,cast("opened date" as varchar) as opened_date
,cast("answered date" as varchar) as answered_date
,cast("answered by" as varchar) as answered_by
,cast("cancelled date" as varchar) as cancelled_date
,cast("cancelled by" as varchar) as cancelled_by
,cast("closed date" as varchar) as closed_date
,cast("closed by" as varchar) as closed_by
,cast("days open to answered" as varchar) as days_open_to_answered
,cast("days answered to closed" as varchar) as days_answered_to_closed
,cast("days open to closed" as varchar) as days_open_to_closed
,cast("query id" as varchar) as query_id
,cast("page|link" as varchar) as page_link
,cast("studyid" as varchar) as studyid
,partition_date as load_date
from
    {{ source ('koios_raw', 'xl184-315_query_detail_na_row') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl184-315_query_detail_na_row') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl184-315_query_detail_na_row') }})
    
union all

select 
cast("critical crf|(y/n)" as varchar) as "critical_query_(y/n)"
,Null as critical_variable_list
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,cast("rand or screen fail date" as varchar) as enroll_or_screen_fail_date
,cast("treatment arm" as varchar) as treatment_arm
,cast("visit|name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("record id" as varchar) as record_id
,cast("field oid" as varchar) as field_oid
,cast("line id" as varchar) as line_id
,cast("query recipient" as varchar) as query_recipient
,cast("issued by name" as varchar) as issued_by_name
,cast("query status" as varchar) as query_status
,cast("days open" as varchar) as days_open
,cast("days answered" as varchar) as days_answered
,cast("last opened query text" as varchar) as last_opened_query_text
,cast("query response" as varchar) as query_response
,cast("opened date" as varchar) as opened_date
,cast("answered date" as varchar) as answered_date
,cast("answered by" as varchar) as answered_by
,cast("cancelled date" as varchar) as cancelled_date
,cast("cancelled by" as varchar) as cancelled_by
,cast("closed date" as varchar) as closed_date
,cast("closed by" as varchar) as closed_by
,cast("days open to answered" as varchar) as days_open_to_answered
,cast("days answered to closed" as varchar) as days_answered_to_closed
,cast("days open to closed" as varchar) as days_open_to_closed
,cast("query id" as varchar) as query_id
,cast("page|link" as varchar) as page_link
,cast("studyid" as varchar) as studyid
,partition_date as load_date
from
    {{ source ('koios_raw', 'xl184-315_query_details_uncl') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl184-315_query_details_uncl') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl184-315_query_details_uncl') }})
    
union all

select 
cast("critical query (y/n)" as varchar) as "critical_query_(y/n)"
,"critical variable list" as critical_variable_list
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,cast("rand or screen fail date" as varchar) as enroll_or_screen_fail_date
,cast("treatment arm" as varchar) as treatment_arm
,cast("visit name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("record id" as varchar) as record_id
,cast("field oid" as varchar) as field_oid
,cast("line id" as varchar) as line_id
,cast("query recipient" as varchar) as query_recipient
,cast("issued by name" as varchar) as issued_by_name
,cast("query status" as varchar) as query_status
,cast("days open" as varchar) as days_open
,cast("days answered" as varchar) as days_answered
,cast("last opened query text" as varchar) as last_opened_query_text
,cast("query response" as varchar) as query_response
,cast("opened date" as varchar) as opened_date
,cast("answered date" as varchar) as answered_date
,cast("answered by" as varchar) as answered_by
,cast("cancelled date" as varchar) as cancelled_date
,cast("cancelled by" as varchar) as cancelled_by
,cast("closed date" as varchar) as closed_date
,cast("closed by" as varchar) as closed_by
,cast("days open to answered" as varchar) as days_open_to_answered
,cast("days answered to closed" as varchar) as days_answered_to_closed
,cast("days open to closed" as varchar) as days_open_to_closed
,cast("query id" as varchar) as query_id
,cast("page link" as varchar) as page_link
,cast("studyid" as varchar) as studyid
,partition_date as load_date
from
    {{ source ('koios_raw', 'xl092-304_query_detail') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-304_query_detail') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-304_query_detail') }})

union all

select 
cast("critical query (y/n)" as varchar) as "critical_query_(y/n)"
,"critical variable list" as critical_variable_list
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,cast("rand or screen fail date" as varchar) as enroll_or_screen_fail_date
--,cast("cohort" as varchar)as cohort
,null as treatment_arm
,cast("visit name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("record id" as varchar) as record_id
,cast("field oid" as varchar) as field_oid
--,cast("Item Label" as varchar) as Item_Label
,cast("line id" as varchar) as line_id
,cast("query recipient" as varchar) as query_recipient
,cast("issued by name" as varchar) as issued_by_name
,cast("query status" as varchar) as query_status
,cast("days open" as varchar) as days_open
,cast("days answered" as varchar) as days_answered
,cast("last opened query text" as varchar) as last_opened_query_text
,cast("query response" as varchar) as query_response
,cast("opened date" as varchar) as opened_date
,cast("answered date" as varchar) as answered_date
,cast("answered by" as varchar) as answered_by
,cast("cancelled date" as varchar) as cancelled_date
,cast("cancelled by" as varchar) as cancelled_by
,cast("closed date" as varchar) as closed_date
,cast("closed by" as varchar) as closed_by
,cast("days open to answered" as varchar) as days_open_to_answered
,cast("days answered to closed" as varchar) as days_answered_to_closed
,cast("days open to closed" as varchar) as days_open_to_closed
,cast("query id" as varchar) as query_id
,cast("page link" as varchar) as page_link
,cast("studyid" as varchar) as studyid
,partition_date as load_date
from
    {{ source ('koios_raw', 'xl092-009_query_detail') }}
    where last_modified_date = (select max(last_modified_date) from {{source ('koios_raw', 'xl092-009_query_detail') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-009_query_detail') }})

union all

select 
null as "critical_query_(y/n)"
,null as critical_variable_list
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as site
,cast("subject number" as varchar) as subject_number
,cast("subject status" as varchar) as subject_status
,null as enroll_or_screen_fail_date
--,cast("cohort" as varchar)as cohort
,null as treatment_arm
,cast("visit name" as varchar) as visit_name
,cast("page name" as varchar) as page_name
,cast("record id" as varchar) as record_id
,cast("field oid" as varchar) as field_oid
--,cast("Item Label" as varchar) as Item_Label
,cast("line id" as varchar) as line_id
,cast("query recipient" as varchar) as query_recipient
,cast("issued by name" as varchar) as issued_by_name
,cast("query status" as varchar) as query_status
,cast("days open" as varchar) as days_open
,cast("days answered" as varchar) as days_answered
,cast("last opened query text" as varchar) as last_opened_query_text
,cast("query response" as varchar) as query_response
,cast("opened date" as varchar) as opened_date
,cast("answered date" as varchar) as answered_date
,cast("answered by" as varchar) as answered_by
,cast("cancelled date" as varchar) as cancelled_date
,cast("cancelled by" as varchar) as cancelled_by
,cast("closed date" as varchar) as closed_date
,cast("closed by" as varchar) as closed_by
,cast("days open to answered" as varchar) as days_open_to_answered
,cast("days answered to closed" as varchar) as days_answered_to_closed
,cast("days open to closed" as varchar) as days_open_to_closed
,cast("query id" as varchar) as query_id
,cast("page link" as varchar) as page_link
,cast("studyid" as varchar) as studyid
,partition_date as load_date
from
    {{ source ('koios_raw', 'xl092-305_query_detail') }}
    where last_modified_date = (select max(last_modified_date) from {{source ('koios_raw', 'xl092-305_query_detail') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-305_query_detail') }})

)

select * from ctms_stg__query_detail
