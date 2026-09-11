with subject_visit_summary_report_305 as (
   select "subject number", visit,"visit date" from
   (select "subject number", visit,"visit date",
   RANK()OVER(partition by study_id||visit||"subject number"
        order by "subject number", cast("visit date" as date) ) as visit_RANK
   from
{{ source ('koios_raw', 'cenduit_blinded_subject_visit_xl092-305') }}
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'cenduit_blinded_subject_visit_xl092-305') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'cenduit_blinded_subject_visit_xl092-305') }})
    and "visit date" is not null
  ) as a where visit_RANK= 1
),

ctms_stg__subject_visit_summary_report as (
select
cast(study_id as varchar) as  study_id
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("study site" as varchar) as  studysite
,cast(investigator as varchar) as  investigator
,cast(subject as varchar) as  subject
,cast("disc. rollback approved?" as varchar) as  "discontinued rollback request approved?"
,cast("sf rollback approved?" as varchar) as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,case when "visit description" = 'Enrollment V1' then 'Randomization' else "visit description" end as "visit description"
,cast("scheduled date [local]" as varchar) as  "scheduled date"
,cast("actual date [local]" as varchar) as  "actual date"
,md5(study_id||country||"study site"||subject||"visit id"||"actual date [local]") as hash_key
,last_modified_date
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
,cast("disc. rollback approved?" as varchar) as  "discontinued rollback request approved?"
,cast("sf rollback approved?" as varchar) as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,cast("visit description" as varchar) as  "visit description"
,cast("scheduled date" as varchar) as  "scheduled date"
,cast("actual date" as varchar) as  "actual date"
,md5(study_id||country||studysite||subject||"visit id"||"actual date") as hash_key
,last_modified_date
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
,cast("disc. rollback approved?" as varchar) as  "discontinued rollback request approved?"
,cast("sf rollback approved?" as varchar) as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,case when "visit description" = 'Enrollment V1' then 'Randomization' else "visit description" end as "visit description"
,cast("scheduled date [local]" as varchar) as  "scheduled date"
,cast("actual date [local]" as varchar) as  "actual date"
,md5(study_id||country||studysite||subject||"visit id"||"actual date [local]") as hash_key
,last_modified_date
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
,cast("disc. rollback approved?" as varchar) as  "discontinued rollback request approved?"
,cast("sf rollback approved?" as varchar) as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,cast("visit description" as varchar) as  "visit description"
,cast("scheduled date [local]" as varchar) as  "scheduled date"
,cast("actual date [local]" as varchar) as  "actual date"
,md5(study_id||country||studysite||subject||"visit id"||"actual date [local]") as hash_key
,last_modified_date
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
,last_modified_date
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xl184_315') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl184_315') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl184_315') }})

UNION all

select
cast(study_id as varchar) as  study_id
,case 
    when upper(country)='UNITED STATES OF AMERICA' then  'UNITED STATES'
    when upper(country)='CZECHIA' then  'CZECH REPUBLIC'
    else upper(country)
end AS country
,cast("study site" as varchar) as  studysite
,cast(investigator as varchar) as  investigator
,cast(subject as varchar) as  subject
,cast("disc. rollback approved?" as varchar) as  "discontinued rollback request approved?"
,cast("sf rollback approved?" as varchar) as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,cast("visit description" as varchar) as  "visit description"
,cast("scheduled date [local]" as varchar) as  "scheduled date"
,cast("actual date [local]" as varchar) as  "actual date"
,md5(study_id||country||"study site"||subject||"visit id"||"actual date [local]") as hash_key
,last_modified_date
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
	when visit = 'Subject Status Change' then 'Rescreening'
	else visit 
end as  "visit description"
,cast("expected visit date" as varchar) as  "scheduled date"
,cast(to_char(TO_DATE("visit date",'DD-MON-YYYY'),'DD MON YY')as varchar) as  "actual date"
,md5(study_id||country||"site number"||"subject number"||''||"visit date") as hash_key
,last_modified_date
from 
{{ source ('koios_raw', 'cenduit_blinded_subject_visit_xl092-009') }}
where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'cenduit_blinded_subject_visit_xl092-009') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'cenduit_blinded_subject_visit_xl092-009') }})
   and "visit date" is not null
UNION all

select distinct
b.study_id
,case when b.country = 'KOREA (THE REPUBLIC OF)' then 'SOUTH KOREA'
    when b.country = 'CZECHIA' then 'CZECH REPUBLIC'
    when b.country = 'UNITED STATES OF AMERICA' then 'UNITED STATES'
    else cast(b.country as varchar)
    end as  country
,cast(b."site number" as varchar) as  studysite
,cast(b."investigator name" as varchar) as  investigator
,cast(b."subject number" as varchar) as  subject
,null as  "discontinued rollback request approved?"
,null as  "screen failure rollback request approved?"
,cast(b.visit  as varchar) as  "visit id"
,b."subject status" as "current participant status"
,case 
	when b.visit = 'Cycle 1 Day 1 (Randomization)' then 'Randomization' 
	when b.visit <> 'Cycle 1 Day 1 (Randomization)' and b.visit like '%Cycle%' then 'Drug Request'
	when b.visit = 'Subject Status Change' then 'Rescreening'
	else b.visit 
end as  "visit description"
,cast(b."expected visit date" as varchar) as  "scheduled date"
,cast(to_char(TO_DATE(a."visit date",'DDMONYYYY'),'DD MON YY')as varchar) as  "actual date"
,md5(b.study_id||b.country||b."site number"||b."subject number"||''||a."visit date") as hash_key
,last_modified_date
from subject_visit_summary_report_305 a
right join {{ source ('koios_raw', 'cenduit_blinded_subject_visit_xl092-305') }} b
on a."subject number"=b."subject number" and a.visit= b.visit
where b.last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'cenduit_blinded_subject_visit_xl092-305') }})
    and b.partition_date = (select max(partition_date) from {{ source ('koios_raw', 'cenduit_blinded_subject_visit_xl092-305') }})
   and b."visit date" is not null

union all

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
,last_modified_date
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xl114_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl114_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl114_101') }})


union all

select
    study_id
    ,country
    ,studysite
    ,investigator
    ,subject
    ,"discontinued rollback request approved?"
    ,"screen failure rollback request approved?"
    ,"visit id"
    ,"current participant status"
    ,"visit description"
    ,"scheduled date"
    ,"actual date"
    ,hash_key
    ,last_modified_date
    from (
        select
        cast(study_id as varchar) as  study_id
        ,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
        ,cast("study site" as varchar) as  studysite
        ,cast(investigator as varchar) as  investigator
        ,cast(subject as varchar) as  subject
        ,cast("disc. rollback approved?" as varchar) as  "discontinued rollback request approved?"
        ,cast("sf rollback approved?" as varchar) as  "screen failure rollback request approved?"
        ,cast("visit id" as varchar) as  "visit id"
        ,null as "current participant status"
        ,case when "visit description" in ('Enrollment','Expansion Randomization') then 'Randomization' else "visit description" end as "visit description"
        ,null as "scheduled date"
        ,cast("actual date" as varchar) as  "actual date"
        ,md5(study_id||country||"study site"||subject||"visit id"||"actual date") as hash_key
        ,last_modified_date
        ,RANK() OVER (
            partition by study_id, "study site", subject,
                case when "visit description" in ('Enrollment','Expansion Randomization') then 'Randomization' else "visit description" end
            order by cast("actual date" as date) desc
        ) as visit_rank
        FROM
            koios_raw.subject_visit_summary_report_xb010_101
            where last_modified_date = (select max(last_modified_date) from koios_raw.subject_visit_summary_report_xb010_101)
            and partition_date = (select max(partition_date) from koios_raw.subject_visit_summary_report_xb010_101)
    ) as ranked
    where visit_rank = 1
    
union all
select
cast(study_id as varchar) as  study_id
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("study site" as varchar) as  studysite
,cast(investigator as varchar) as  investigator
,cast(subject as varchar) as  subject
,null as  "discontinued rollback request approved?"
,null as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,case when "visit description" = 'Enrollment' then 'Randomization' else "visit description" end as "visit description"
,cast("scheduled date" as varchar) as  "scheduled date"
,cast("actual date" as varchar) as  "actual date"
,md5(study_id||country||"study site"||subject||"visit id"||"actual date") as hash_key
,last_modified_date
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xl495_101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl495_101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl495_101') }})
union all
 
select
cast(trim(study_id) as varchar) as  study_id
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
-- ,cast("study site" as varchar) as  studysite
,cast(case when "study site" ='119' 
then '108' else "study site" end as varchar) as  studysite
,cast(investigator as varchar) as  investigator
,cast(subject as varchar) as  subject
-- ,'0'||substring(subject,1,3)||'-'||'0'||substring(subject,4,6) as subject 
,cast("discontinued rollback request approved?" as varchar) as  "discontinued rollback request approved?"
,null as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,case when "visit description" = 'Cycle 1 Day 1' then 'Randomization' 
	when "visit description" <> 'Cycle 1 Day 1' and "visit description" like '%Cycle%' then 'Drug Request'
	else "visit description" 
	end as "visit description"
,cast("scheduled date" as varchar) as  "scheduled date"
,cast("actual date" as varchar) as  "actual date"
,md5(study_id||country||"study site"||subject||"visit id"||"actual date") as hash_key
,last_modified_date
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xl309-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl309-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl309-101') }})
union all
select
cast(study_id as varchar) as  study_id
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("study site" as varchar) as  studysite
,cast(investigator as varchar) as  investigator
,cast(subject as varchar) as  subject
,cast("discontinue rollback approved?" as varchar) as  "discontinued rollback request approved?"
,cast("sf rollback approved?" as varchar) as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,case when "visit description" = 'Enrollment' then 'Randomization' else "visit description" end as "visit description"
,cast("scheduled date" as varchar) as  "scheduled date"
,cast("actual date" as varchar) as  "actual date"
,md5(study_id||country||"study site"||subject||"visit id"||"actual date") as hash_key
,last_modified_date
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xb628-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xb628-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xb628-101') }})

UNION all

select
cast(study_id as varchar) as  study_id
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("study site" as varchar) as  studysite
,cast(investigator as varchar) as  investigator
,cast(subject as varchar) as  subject
,Null as  "discontinued rollback request approved?"
,Null as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,case when "visit description" = 'W1D1' then 'Randomization' else "visit description" end as "visit description"
,cast("scheduled date" as varchar) as  "scheduled date"
,cast("actual date" as varchar) as  "actual date"
,md5(study_id||country||"study site"||subject||"visit id"||"actual date") as hash_key
,last_modified_date
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xl092-311') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl092-311') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl092-311') }})

union all
select
cast(study_id as varchar) as  study_id
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("study site" as varchar) as  studysite
,cast(investigator as varchar) as  investigator
,cast(subject as varchar) as  subject
,cast("discontinue rollback approved?" as varchar) as  "discontinued rollback request approved?"
,cast("sf rollback approved?" as varchar) as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,case when "visit description" = 'Enrollment' then 'Randomization' else "visit description" end as "visit description"
,cast("scheduled date" as varchar) as "scheduled date"
,cast("actual date" as varchar) as  "actual date"
,md5(study_id||country||"study site"||subject||"visit id"||"actual date") as hash_key
,last_modified_date
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xb371-101') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xb371-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xb371-101') }})
union all
select
cast(study_id as varchar) as  study_id
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("study site" as varchar) as  studysite
,cast("investigator name" as varchar) as  investigator
,cast("participant id" as varchar) as  subject
,null as  "discontinued rollback request approved?"
,null as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,case when "visit description" = 'Enrollment (Cycle 1)' then 'Randomization' else "visit description" end as "visit description"
--case when trim("scheduled date [local]") is not null and trim("scheduled date [local]") <> '' then to_char(to_date(trim("scheduled date [local]"),'DD-Mon-YY'),'YYYY-MM-DD') else null end as "scheduled date"
--to_char(to_date("visit date [local]",'DD-Mon-YY'),'YYYY-MM-DD') as  "actual date"
,cast("scheduled date" as varchar) as "scheduled date"
,cast("visit date" as varchar) as  "actual date"
,md5(study_id||country||"study site"||"participant id"||"visit id"||"visit date") as hash_key
,last_modified_date
FROM
    {{ source ('koios_raw', 'subject_visit_summary_report_xl092-201') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl092-201') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'subject_visit_summary_report_xl092-201') }})


union all
select
cast(study_id as varchar) as  study_id
,REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("study site" as varchar) as  studysite
,cast("investigator name" as varchar) as  investigator
,cast("participant id" as varchar) as  subject
,null as  "discontinued rollback request approved?"
,null as  "screen failure rollback request approved?"
,cast("visit id" as varchar) as  "visit id"
,null as "current participant status"
,case when "visit description" = 'Enrollment (Week 1)' then 'Randomization' else "visit description" end as "visit description"
--case when trim("scheduled date [local]") is not null and trim("scheduled date [local]") <> '' then to_char(to_date(trim("scheduled date [local]"),'DD-Mon-YY'),'YYYY-MM-DD') else null end as "scheduled date"
--to_char(to_date("visit date [local]",'DD-Mon-YY'),'YYYY-MM-DD') as  "actual date"
,cast("scheduled date [local]" as varchar) as "scheduled date"
,cast("visit date [local]" as varchar) as  "actual date"
,md5(study_id||country||"study site"||"participant id"||"visit id"||"visit date [local]") as hash_key
,last_modified_date
FROM
    {{ source ('koios_raw', 'participant_visit_summary_report_xl092-011') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'participant_visit_summary_report_xl092-011') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'participant_visit_summary_report_xl092-011') }})


)
select * from ctms_stg__subject_visit_summary_report
