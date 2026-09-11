with ctms_stg__subject_summary as 
(
select
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as "site"
,cast("subject number" as varchar) as "subject number"
,cast("subject status" as varchar) as "subject status"
,cast("study stage" as varchar) as "study stage"
,cast("date of first icf" as varchar) as "date of first icf"
,cast("enroll or screen fail date" as varchar) as "enroll or screen fail date"
,cast("cohort" as varchar) as cohort_desc
,case 
    when split_part(cohort,':',1) like 'COHORT B%' then 'Cohort B'
    when split_part(cohort,':',1) like 'COHORT F%' then 'Cohort F'
    when split_part(cohort,':',1) like 'COHORT H%' then 'Cohort H'
    else initcap(split_part(cohort,':',1))
end as cohort
,Null as treatment_arm
,case
    when split_part(cohort,':',1) like 'COHORT H%' then 'Esophageal SCC'
    else trim(split_part("cohort",':',2))
end as tumor_type
,Null as race
,Null as ethnic
,cast("treatment arm" as varchar) as dose_level_treatment_arm
,cast("most recent visit" as varchar) as "most recent visit"
,cast("most recent date of visit" as varchar) as "most recent date of visit"
,cast("eotxb date" as varchar) as "eotxb date"
,cast("death date" as varchar) as "death date"
,cast("eorfup date" as varchar) as "eorfup date"
,cast("eos date" as varchar) as "eos date"
,cast("eos reason" as varchar) as "eos reason"
,null as "esfu date"
,null as "esfu reason"
,cast("total expected pgs" as int) as "total expected pgs"
,cast("missing pgs (entry not started)" as int) as "missing pgs (entry not started)"
,cast("% missing pgs (entry not started)" as varchar) as "% missing pgs (entry not started)"
,cast("missing <30 days" as varchar) as "missing <30 days"
,cast("% missing <30 days" as varchar) as "% missing <30 days"
,cast("missing 31-60 days" as varchar) as "missing 31-60 days"
,cast("% missing 31-60 days" as varchar) as "% missing 31-60 days"
,cast("missing 61-90 days" as varchar) as "missing 61-90 days"
,cast("% missing 61-90 days" as varchar) as "% missing 61-90 days"
,cast("missing >90 days" as varchar) as "missing >90 days"
,cast("% missing >90 days" as varchar) as "% missing >90 days"
,cast("entered pgs" as int) as "entered pgs"
,cast("% entered pgs" as varchar) as "% entered pgs"
,cast("sv not started" as varchar) as "sv not started"
,cast("% sv not started" as varchar) as "% sv not started"
,cast("sv not started <30 days" as varchar) as "sv not started <30 days"
,cast("% sv not started <30 days" as varchar) as "% sv not started <30 days"
,cast("sv not started 31-60 days" as varchar) as "sv not started 31-60 days"
,cast("% sv not started 31-60 days" as varchar) as "% sv not started 31-60 days"
,cast("sv not started 61-90 days" as varchar) as "sv not started 61-90 days"
,cast("% sv not started 61-90 days" as varchar) as "% sv not started 61-90 days"
,cast("sv not started >90 days" as varchar) as "sv not started >90 days"
,cast("% sv not started >90 days" as varchar) as "% sv not started >90 days"
,cast("sv incomplete" as varchar) as "sv incomplete"
,cast("% sv incomplete" as varchar) as "% sv incomplete"
,cast("sv complete" as varchar) as "sv complete"
,cast("% sv complete" as varchar) as "% sv complete"
,cast("open" as varchar) as "open"
,cast("open <30 days" as varchar) as "open <30 days"
,cast("open 31-60 days" as varchar) as "open 31-60 days"
,cast("open 61-90 days" as varchar) as "open 61-90 days"
,cast("open >90 days" as varchar) as "open >90 days"
,cast("answered pending review" as varchar) as "answered pending review"
,cast("<30 days after report date" as varchar) as "<30 days after report date"
,cast("31-60 days after report date" as varchar) as "31-60 days after report date"
,cast("61-90 days after report date" as varchar) as "61-90 days after report date"
,cast("90-180 days after report date" as varchar) as "90-180 days after report date"
,cast("studyid" as varchar) as "studyid"
,partition_date as load_date
,last_modified_date as EDC_Data_Refreshed_Date
from
    {{ source ('koios_raw', 'xb002-101_subject_summary') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xb002-101_subject_summary') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xb002-101_subject_summary') }})

union all

select 
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as "site"
,cast("subject number" as varchar) as "subject number"
,cast("subject status" as varchar) as "subject status"
,Null  as "study stage"
,cast("date of first icf" as varchar) as "date of first icf"
,cast("randomization or screen fail date" as varchar) as "enroll or screen fail date"
,Null as cohort_desc
,Null as "cohort"
,Null as treatment_arm
,Null as tumor_type
,race
,ethnic
,cast("treatment arm" as varchar) as dose_level_treatment_arm
,cast("most recent visit" as varchar) as "most recent visit"
,cast("most recent date of visit" as varchar) as "most recent date of visit"
,cast("xl092  eot date" as varchar) as "eotxb date"
,cast("death date" as varchar) as "death date"
,cast("erfu date" as varchar) as "eorfup date"
,case 
	when "esfu reason" in ('Death','Lost To Follow-Up','Withdrew full consent from all study interventions and non-interventional study assessments.','Sponsor Decision') then "esfu date"
	else null 
end as "eos date"
,case 
	when "esfu reason" in ('Death','Lost To Follow-Up','Withdrew full consent from all study interventions and non-interventional study assessments.','Sponsor Decision') then "esfu reason" 
	else NULL
end as "eos reason"
,cast("esfu date" as varchar) as "esfu date"
,cast("esfu reason" as varchar) as "esfu reason"
,cast("total expected pgs" as int) as "total expected pgs"
,cast("missing pgs (entry not started)" as int) as "missing pgs (entry not started)"
,cast("% missing pgs (entry not started)" as varchar) as "% missing pgs (entry not started)"
,cast("missing <30 days" as varchar) as "missing <30 days"
,cast("% missing <30 days" as varchar) as "% missing <30 days"
,cast("missing 31-60 days" as varchar) as "missing 31-60 days"
,cast("% missing 31-60 days" as varchar) as "% missing 31-60 days"
,cast("missing 61-90 days" as varchar) as "missing 61-90 days"
,cast("% missing 61-90 days" as varchar) as "% missing 61-90 days"
,cast("missing >90 days" as varchar) as "missing >90 days"
,cast("% missing >90 days" as varchar) as "% missing >90 days"
,cast("entered pgs" as int) as "entered pgs"
,cast("% entered pgs" as varchar) as "% entered pgs"
,cast("sv not started" as varchar) as "sv not started"
,cast("% sv not started" as varchar) as "% sv not started"
,cast("sv not started <30 days" as varchar) as "sv not started <30 days"
,cast("% sv not started <30 days" as varchar) as "% sv not started <30 days"
,cast("sv not started 31-60 days" as varchar) as "sv not started 31-60 days"
,cast("% sv not started 31-60 days" as varchar) as "% sv not started 31-60 days"
,cast("sv not started 61-90 days" as varchar) as "sv not started 61-90 days"
,cast("% sv not started 61-90 days" as varchar) as "% sv not started 61-90 days"
,cast("sv not started >90 days" as varchar) as "sv not started >90 days"
,cast("% sv not started >90 days" as varchar) as "% sv not started >90 days"
,cast("sv incomplete" as varchar) as "sv incomplete"
,cast("% sv incomplete" as varchar) as "% sv incomplete"
,cast("sv complete" as varchar) as "sv complete"
,cast("% sv complete" as varchar) as "% sv complete"
,cast("open" as varchar) as "open"
,cast("open <30 days" as varchar) as "open <30 days"
,cast("open 31-60 days" as varchar) as "open 31-60 days"
,cast("open 61-90 days" as varchar) as "open 61-90 days"
,cast("open >90 days" as varchar) as "open >90 days"
,cast("answered pending review" as varchar) as "answered pending review"
,cast("<30 days after report date" as varchar) as "<30 days after report date"
,cast("31-60 days after report date" as varchar) as "31-60 days after report date"
,cast("61-90 days after report date" as varchar) as "61-90 days after report date"
,cast("90-180 days after report date" as varchar) as "90-180 days after report date"
,cast("studyid" as varchar) as "studyid"
,partition_date as load_date
,last_modified_date as EDC_Data_Refreshed_Date
from
    {{ source ('koios_raw', 'xl092-303_subject_summary') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-303_subject_summary') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-303_subject_summary') }})


union all

select 
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as "site"
,cast("subject number" as varchar) as "subject number"
,cast("subject status" as varchar) as "subject status"
,Null as "study stage"
,cast("date of first icf" as varchar) as "date of first icf"
,cast("enrolled randomized or screen fail date date" as varchar) as "enroll or screen fail date"
,cast("cohort" as varchar) as cohort_desc
,split_part("cohort",':',1) as cohort
,Null as treatment_arm
,split_part("cohort",':',2) as tumor_type
,Null as race
,Null as ethnic
,cast("treatment arm" as varchar) as dose_level_treatment_arm
,cast("most recent visit" as varchar) as "most recent visit"
,cast("most recent date of visit" as varchar) as "most recent date of visit"
,cast("xl092  eot date" as varchar) as "eotxb date"
,cast("death date" as varchar) as "death date"
,cast("erfu date" as varchar) as "eorfup date"
,cast("eos date" as varchar) as "eos date"
,cast("eos reason" as varchar) as "eos reason"
,null as "esfu date"
,null as "esfu reason"
,cast("total expected pgs" as int) as "total expected pgs"
,cast("missing pgs (entry not started)" as int) as "missing pgs (entry not started)"
,cast("% missing pgs (entry not started)" as varchar) as "% missing pgs (entry not started)"
,cast("missing <30 days" as varchar) as "missing <30 days"
,cast("% missing <30 days" as varchar) as "% missing <30 days"
,cast("missing 31-60 days" as varchar) as "missing 31-60 days"
,cast("% missing 31-60 days" as varchar) as "% missing 31-60 days"
,cast("missing 61-90 days" as varchar) as "missing 61-90 days"
,cast("% missing 61-90 days" as varchar) as "% missing 61-90 days"
,cast("missing >90 days" as varchar) as "missing >90 days"
,cast("% missing >90 days" as varchar) as "% missing >90 days"
,cast("entered pgs" as int) as "entered pgs"
,cast("% entered pgs" as varchar) as "% entered pgs"
,cast("sv not started" as varchar) as "sv not started"
,cast("% sv not started" as varchar) as "% sv not started"
,cast("sv not started <30 days" as varchar) as "sv not started <30 days"
,cast("% sv not started <30 days" as varchar) as "% sv not started <30 days"
,cast("sv not started 31-60 days" as varchar) as "sv not started 31-60 days"
,cast("% sv not started 31-60 days" as varchar) as "% sv not started 31-60 days"
,cast("sv not started 61-90 days" as varchar) as "sv not started 61-90 days"
,cast("% sv not started 61-90 days" as varchar) as "% sv not started 61-90 days"
,cast("sv not started >90 days" as varchar) as "sv not started >90 days"
,cast("% sv not started >90 days" as varchar) as "% sv not started >90 days"
,cast("sv incomplete" as varchar) as "sv incomplete"
,cast("% sv incomplete" as varchar) as "% sv incomplete"
,cast("sv complete" as varchar) as "sv complete"
,cast("% sv complete" as varchar) as "% sv complete"
,cast("open" as varchar) as "open"
,cast("open <30 days" as varchar) as "open <30 days"
,cast("open 31-60 days" as varchar) as "open 31-60 days"
,cast("open 61-90 days" as varchar) as "open 61-90 days"
,cast("open >90 days" as varchar) as "open >90 days"
,cast("answered pending review" as varchar) as "answered pending review"
,cast("<30 days after report date" as varchar) as "<30 days after report date"
,cast("31-60 days after report date" as varchar) as "31-60 days after report date"
,cast("61-90 days after report date" as varchar) as "61-90 days after report date"
,cast("90-180 days after report date" as varchar) as "90-180 days after report date"
,cast("studyid" as varchar) as "studyid"
,partition_date as load_date
,last_modified_date as EDC_Data_Refreshed_Date
from
    {{ source ('koios_raw', 'xl092-002_subject_summary') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-002_subject_summary') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-002_subject_summary') }})

union all

select 
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as "site"
,cast("subject number" as varchar) as "subject number"
,cast("subject status" as varchar) as "subject status"
,Null as "study stage"
,cast("date subject  first signed  informed consent" as varchar) as "date of first icf"
,cast("enrollment or screen fail date" as varchar) as "enroll or screen fail date"
,cast("cohort" as varchar) as cohort_desc
,cohort as "cohort"
,Null as treatment_arm
,Null as tumor_type
,Null as race
,Null as ethnic
,cast("treatment  description" as varchar) as dose_level_treatment_arm
,cast("most recent visit" as varchar) as "most recent visit"
,cast("most recent date of visit" as varchar) as "most recent date of visit"
,cast("xl092 eot date" as varchar) as "eotxb date"
,cast("death date" as varchar) as "death date"
,cast("eorfup date" as varchar) as "eorfup date"
,cast("eos date" as varchar) as "eos date"
,cast("eos reason" as varchar) as "eos reason"
,null as "esfu date"
,null as "esfu reason"
,cast("total expected pgs" as int) as "total expected pgs"
,cast("missing pgs (entry not started)" as int) as "missing pgs (entry not started)"
,Null as "% missing pgs (entry not started)"
,cast("missing pgs <30 days" as varchar) as "missing <30 days"
,Null "% missing <30 days"
,cast("missing pgs 31-60 days" as varchar) as "missing 31-60 days"
,Null as "% missing 31-60 days"
,cast("missing pgs 61-90 days" as varchar) as "missing 61-90 days"
,Null as "% missing 61-90 days"
,cast("missing pgs >90 days" as varchar) as "missing >90 days"
,Null as "% missing >90 days"
,cast("incomplete pgs" as int) as "entered pgs"
,Null as "% entered pgs"
,cast("completed pgs" as varchar) as "sv not started"
,Null as "% sv not started"
,cast("sv not started <30 days" as varchar) as "sv not started <30 days"
,Null as "% sv not started <30 days"
,cast("sv not started 31-60 days" as varchar) as "sv not started 31-60 days"
,Null as "% sv not started 31-60 days"
,cast("sv not started 61-90 days" as varchar) as "sv not started 61-90 days"
,Null as "% sv not started 61-90 days"
,cast("sv not started >90 days" as varchar) as "sv not started >90 days"
,Null as "% sv not started >90 days"
,cast("sv incomplete" as varchar) as "sv incomplete"
,Null as "% sv incomplete"
,cast("sv complete" as varchar) as "sv complete"
,Null as "% sv complete"
,cast("open" as varchar) as "open"
,cast("open <30 days" as varchar) as "open <30 days"
,cast("open 31-60 days" as varchar) as "open 31-60 days"
,cast("open 61-90 days" as varchar) as "open 61-90 days"
,cast("open >90 days" as varchar) as "open >90 days"
,cast("answered pending review" as varchar) as "answered pending review"
,cast("<30 days after report date" as varchar) as "<30 days after report date"
,cast("31-60 days after report date" as varchar) as "31-60 days after report date"
,cast("61-90 days after report date" as varchar) as "61-90 days after report date"
,Null as "90-180 days after report date"
,cast("studyid" as varchar) as "studyid"
,partition_date as load_date
,last_modified_date as EDC_Data_Refreshed_Date
from
    {{ source ('koios_raw', 'xl092-001_subject_summary') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-001_subject_summary') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-001_subject_summary') }})

union all

select 
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as "site"
,cast("subject number" as varchar) as "subject number"
,cast("subject status" as varchar) as "subject status"
,Null as "study stage"
,cast("date of first icf" as varchar) as "date of first icf"
,cast("enroll or screen fail date" as varchar) as "enroll or screen fail date"
,cast("cohort" as varchar) as cohort_desc
,cast("cohort" as varchar) as "cohort"
,Null as treatment_arm
,Null as tumor_type
,Null as race
,Null as ethnic
,cast("treatment arm" as varchar) as dose_level_treatment_arm
,cast("most recent visit" as varchar) as "most recent visit"
,cast("most recent date of visit" as varchar) as "most recent date of visit"
,cast("eotx date" as varchar) as "eotxb date"
,cast("death date" as varchar) as "death date"
,cast("eorfup date" as varchar) as "eorfup date"
,cast("eos date" as varchar) as "eos date"
,cast("eos reason" as varchar) as "eos reason"
,null as "esfu date"
,null as "esfu reason"
,cast("total expected pgs" as int) as "total expected pgs"
,cast("missing pgs (entry not started)" as int) as "missing pgs (entry not started)"
,cast("% missing pgs (entry not started)" as varchar) as "% missing pgs (entry not started)"
,cast("missing <30 days" as varchar) as "missing <30 days"
,cast("% missing <30 days" as varchar) as "% missing <30 days"
,cast("missing 31-60 days" as varchar) as "missing 31-60 days"
,cast("% missing 31-60 days" as varchar) as "% missing 31-60 days"
,cast("missing 61-90 days" as varchar) as "missing 61-90 days"
,cast("% missing 61-90 days" as varchar) as "% missing 61-90 days"
,cast("missing >90 days" as varchar) as "missing >90 days"
,cast("% missing >90 days" as varchar) as "% missing >90 days"
,cast("entered pgs" as int) as "entered pgs"
,cast("% entered pgs" as varchar) as "% entered pgs"
,cast("sv not started" as varchar) as "sv not started"
,cast("% sv not started" as varchar) as "% sv not started"
,cast("sv not started <30 days" as varchar) as "sv not started <30 days"
,cast("% sv not started <30 days" as varchar) as "% sv not started <30 days"
,cast("sv not started 31-60 days" as varchar) as "sv not started 31-60 days"
,cast("% sv not started 31-60 days" as varchar) as "% sv not started 31-60 days"
,cast("sv not started 61-90 days" as varchar) as "sv not started 61-90 days"
,cast("% sv not started 61-90 days" as varchar) as "% sv not started 61-90 days"
,cast("sv not started >90 days" as varchar) as "sv not started >90 days"
,cast("% sv not started >90 days" as varchar) as "% sv not started >90 days"
,cast("sv incomplete" as varchar) as "sv incomplete"
,cast("% sv incomplete" as varchar) as "% sv incomplete"
,cast("sv complete" as varchar) as "sv complete"
,cast("% sv complete" as varchar) as "% sv complete"
,cast("open" as varchar) as "open"
,cast("open <30 days" as varchar) as "open <30 days"
,cast("open 31-60 days" as varchar) as "open 31-60 days"
,cast("open 61-90 days" as varchar) as "open 61-90 days"
,cast("open >90 days" as varchar) as "open >90 days"
,cast("answered pending review" as varchar) as "answered pending review"
,cast("<30 days after report date" as varchar) as "<30 days after report date"
,cast("31-60 days after report date" as varchar) as "31-60 days after report date"
,cast("61-90 days after report date" as varchar) as "61-90 days after report date"
,cast("90-180 days after report date" as varchar) as "90-180 days after report date"
,cast("studyid" as varchar) as "studyid"
,partition_date as load_date
,last_modified_date as EDC_Data_Refreshed_Date
from
    {{ source ('koios_raw', 'xl102-101_subject_summary') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl102-101_subject_summary') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl102-101_subject_summary') }})

union all

select distinct
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as "site"
,cast("subject number" as varchar) as "subject number"
,cast("subject status" as varchar) as "subject status"
,Null as "study stage"
,cast("date of first icf" as varchar) as "date of first icf"
,cast("rand or screen fail date" as varchar) as "enroll or screen fail date"
,Null as cohort_desc
,Null as "cohort"
,Null as treatment_arm
,Null as tumor_type
,Null as race
,Null as ethnic
,cast("treatment arm" as varchar) as dose_level_treatment_arm
,cast("most recent visit" as varchar) as "most recent visit"
,cast("most recent date of visit" as varchar) as "most recent date of visit"
,cast("eotab date" as varchar) as "eotxb date"
,cast("death date" as varchar) as "death date"
,cast("eorfup date" as varchar) as "eorfup date"
,cast("eos date" as varchar) as "eos date"
,cast("eos reason" as varchar) as "eos reason"
,null as "esfu date"
,null as "esfu reason"
,cast("total expected pgs" as int) as "total expected pgs"
,cast("missing pgs (entry not started)" as int) as "missing pgs (entry not started)"
,cast("% missing pgs (entry not started)" as varchar) as "% missing pgs (entry not started)"
,cast("missing <30 days" as varchar) as "missing <30 days"
,cast("% missing <30 days" as varchar) as "% missing <30 days"
,cast("missing 31-60 days" as varchar) as "missing 31-60 days"
,cast("% missing 31-60 days" as varchar) as "% missing 31-60 days"
,cast("missing 61-90 days" as varchar) as "missing 61-90 days"
,cast("% missing 61-90 days" as varchar) as "% missing 61-90 days"
,cast("missing >90 days" as varchar) as "missing >90 days"
,cast("% missing >90 days" as varchar) as "% missing >90 days"
,cast("entered pgs" as int) as "entered pgs"
,cast("% entered pgs" as varchar) as "% entered pgs"
,cast("sv not started" as varchar) as "sv not started"
,cast("% sv not started" as varchar) as "% sv not started"
,cast("sv not started <30 days" as varchar) as "sv not started <30 days"
,cast("% sv not started <30 days" as varchar) as "% sv not started <30 days"
,cast("sv not started 31-60 days" as varchar) as "sv not started 31-60 days"
,cast("% sv not started 31-60 days" as varchar) as "% sv not started 31-60 days"
,cast("sv not started 61-90 days" as varchar) as "sv not started 61-90 days"
,cast("% sv not started 61-90 days" as varchar) as "% sv not started 61-90 days"
,cast("sv not started >90 days" as varchar) as "sv not started >90 days"
,cast("% sv not started >90 days" as varchar) as "% sv not started >90 days"
,cast("sv incomplete" as varchar) as "sv incomplete"
,cast("% sv incomplete" as varchar) as "% sv incomplete"
,cast("sv complete" as varchar) as "sv complete"
,cast("% sv complete" as varchar) as "% sv complete"
,cast("open" as varchar) as "open"
,cast("open <30 days" as varchar) as "open <30 days"
,cast("open 31-60 days" as varchar) as "open 31-60 days"
,cast("open 61-90 days" as varchar) as "open 61-90 days"
,cast("open >90 days" as varchar) as "open >90 days"
,cast("answered pending review" as varchar) as "answered pending review"
,cast("<30 days after report date" as varchar) as "<30 days after report date"
,cast("31-60 days after report date" as varchar) as "31-60 days after report date"
,cast("61-90 days after report date" as varchar) as "61-90 days after report date"
,cast("90-180 days after report date" as varchar) as "90-180 days after report date"
,cast("studyid" as varchar) as "studyid"
,partition_date as load_date
,last_modified_date as EDC_Data_Refreshed_Date
from
    {{ source ('koios_raw', 'xl184-315_subject_summary') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl184-315_subject_summary') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl184-315_subject_summary') }})
    and upper("country") not like 'RE-CONSENT%'
    and studyid is not null

union all

select 
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar) as "site"
,cast("subject number" as varchar) as "subject number"
,cast("subject status" as varchar) as "subject status"
,Null as "study stage"
,cast("date of first icf" as varchar) as "date of first icf"
,cast("rand or screen fail date" as varchar) as "enroll or screen fail date"
,Null as cohort_desc
,Null as "cohort"
,Null as treatment_arm
,Null as tumor_type
,cast(race as varchar) as race
,cast(ethnic as varchar) as ethnic
,cast("treatment arm" as varchar) as dose_level_treatment_arm
,cast("most recent visit" as varchar) as "most recent visit"
,cast("most recent date of visit" as varchar) as "most recent date of visit"
,cast("xl092  eot date" as varchar) as "eotxb date"
,cast("death date" as varchar) as "death date"
,cast("erfu date" as varchar) as "eorfup date"
,cast("eos date" as varchar) as "eos date"
,cast("eos reason" as varchar) as "eos reason"
,null as "esfu date"
,null as "esfu reason"
,cast("total expected pgs" as int) as "total expected pgs"
,cast("missing pgs (entry not started)" as int) as "missing pgs (entry not started)"
,cast("% missing pgs (entry not started)" as varchar) as "% missing pgs (entry not started)"
,cast("missing <30 days" as varchar) as "missing <30 days"
,cast("% missing <30 days" as varchar) as "% missing <30 days"
,cast("missing 31-60 days" as varchar) as "missing 31-60 days"
,cast("% missing 31-60 days" as varchar) as "% missing 31-60 days"
,cast("missing 61-90 days" as varchar) as "missing 61-90 days"
,cast("% missing 61-90 days" as varchar) as "% missing 61-90 days"
,cast("missing >90 days" as varchar) as "missing >90 days"
,cast("% missing >90 days" as varchar) as "% missing >90 days"
,cast("entered pgs" as int) as "entered pgs"
,cast("% entered pgs" as varchar) as "% entered pgs"
,cast("sv not started" as varchar) as "sv not started"
,cast("% sv not started" as varchar) as "% sv not started"
,cast("sv not started <30 days" as varchar) as "sv not started <30 days"
,cast("% sv not started <30 days" as varchar) as "% sv not started <30 days"
,cast("sv not started 31-60 days" as varchar) as "sv not started 31-60 days"
,cast("% sv not started 31-60 days" as varchar) as "% sv not started 31-60 days"
,cast("sv not started 61-90 days" as varchar) as "sv not started 61-90 days"
,cast("% sv not started 61-90 days" as varchar) as "% sv not started 61-90 days"
,cast("sv not started >90 days" as varchar) as "sv not started >90 days"
,cast("% sv not started >90 days" as varchar) as "% sv not started >90 days"
,cast("sv incomplete" as varchar) as "sv incomplete"
,cast("% sv incomplete" as varchar) as "% sv incomplete"
,cast("sv complete" as varchar) as "sv complete"
,cast("% sv complete" as varchar) as "% sv complete"
,cast("open" as varchar) as "open"
,cast("open <30 days" as varchar) as "open <30 days"
,cast("open 31-60 days" as varchar) as "open 31-60 days"
,cast("open 61-90 days" as varchar) as "open 61-90 days"
,cast("open >90 days" as varchar) as "open >90 days"
,cast("answered pending review" as varchar) as "answered pending review"
,cast("<30 days after report date" as varchar) as "<30 days after report date"
,cast("31-60 days after report date" as varchar) as "31-60 days after report date"
,cast("61-90 days after report date" as varchar) as "61-90 days after report date"
,cast("91-180 days after report date" as varchar) as "90-180 days after report date"
,cast("studyid" as varchar) as "studyid"
,partition_date as load_date
,last_modified_date as EDC_Data_Refreshed_Date
from
    {{ source ('koios_raw', 'xl092-304_subject_summary') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-304_subject_summary') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-304_subject_summary') }})


union all

select 
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,case when "site"='2124 - Richard Zuniga' then '1841 - Richard Zuniga'
else cast("site" as varchar) 
end as "site"
,cast("subject number" as varchar) as "subject number"
,cast("subject status" as varchar) as "subject status"
,Null as "study stage"
,cast("date of first icf" as varchar) as "date of first icf"
,cast("randomization or screen fail date"as varchar) as "enroll or screen fail date"
,Null as cohort_desc
,Null as "cohort"
,Null as treatment_arm
,case 
	when cohort like '%Cohort A %' then 'Advanced Solid Tumors'
	when cohort like '%Cohort B %' then '1L RCC'
	when cohort like '%Cohort 1 %' then '2L+RCC'
	when cohort like '%Cohort 2 %' then '1L RCC'
	else null
end as tumor_type
,cast(race as varchar) as race
,cast(ethnic as varchar) as ethnic
,Null as dose_level_treatment_arm
,cast("most recent visit" as varchar) as "most recent visit"
,cast("most recent date of visit" as varchar) as "most recent date of visit"
,cast("xl092  eot date" as varchar) as "eotxb date"
,cast("death date" as varchar) as "death date"
,Null as "eorfup date"
,case 
	when "esfu reason" in ('Death','Lost To Follow-Up','Withdrew full consent from all study interventions and non-interventional study assessments.','Sponsor Decision') then "esfu date"
	else null 
end as "eos date"
,case 
	when "esfu reason" in ('Death','Lost To Follow-Up','Withdrew full consent from all study interventions and non-interventional study assessments.','Sponsor Decision') then "esfu reason" 
	else NULL
end as "eos reason"
,cast("esfu date" as varchar) as "esfu date"
,cast("esfu reason" as varchar) as "esfu reason"
,cast("total expected pgs" as int) as "total expected pgs"
,cast("missing pgs (entry not started)" as int) as "missing pgs (entry not started)"
,cast("% missing pgs (entry not started)" as varchar) as "% missing pgs (entry not started)"
,cast("missing <30 days" as varchar) as "missing <30 days"
,cast("% missing <30 days" as varchar) as "% missing <30 days"
,cast("missing 31-60 days" as varchar) as "missing 31-60 days"
,cast("% missing 31-60 days" as varchar) as "% missing 31-60 days"
,cast("missing 61-90 days" as varchar) as "missing 61-90 days"
,cast("% missing 61-90 days" as varchar) as "% missing 61-90 days"
,cast("missing >90 days" as varchar) as "missing >90 days"
,cast("% missing >90 days" as varchar) as "% missing >90 days"
,cast("entered pgs" as int) as "entered pgs"
,cast("% entered pgs" as varchar) as "% entered pgs"
,cast("sv not started" as varchar) as "sv not started"
,cast("% sv not started" as varchar) as "% sv not started"
,cast("sv not started <30 days" as varchar) as "sv not started <30 days"
,cast("% sv not started <30 days" as varchar) as "% sv not started <30 days"
,cast("sv not started 31-60 days" as varchar) as "sv not started 31-60 days"
,cast("% sv not started 31-60 days" as varchar) as "% sv not started 31-60 days"
,cast("sv not started 61-90 days" as varchar) as "sv not started 61-90 days"
,cast("% sv not started 61-90 days" as varchar) as "% sv not started 61-90 days"
,cast("sv not started >90 days" as varchar) as "sv not started >90 days"
,cast("% sv not started >90 days" as varchar) as "% sv not started >90 days"
,cast("sv incomplete" as varchar) as "sv incomplete"
,cast("% sv incomplete" as varchar) as "% sv incomplete"
,cast("sv complete" as varchar) as "sv complete"
,cast("% sv complete" as varchar) as "% sv complete"
,cast("open" as varchar) as "open"
,cast("open <30 days" as varchar) as "open <30 days"
,cast("open 31-60 days" as varchar) as "open 31-60 days"
,cast("open 61-90 days" as varchar) as "open 61-90 days"
,cast("open >90 days" as varchar) as "open >90 days"
,cast("answered pending review" as varchar) as "answered pending review"
,cast("<30 days after report date" as varchar) as "<30 days after report date"
,cast("31-60 days after report date" as varchar) as "31-60 days after report date"
,cast("61-90 days after report date" as varchar) as "61-90 days after report date"
,cast("90-180 days after report date"as varchar) as "90-180 days after report date"
,cast("studyid" as varchar) as "studyid"
,partition_date as load_date
,last_modified_date as EDC_Data_Refreshed_Date
from 
    {{ source ('koios_raw', 'xl092-009_subject_summary') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-009_subject_summary') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-009_subject_summary') }})


union all

select 
REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') AS country
,cast("site" as varchar)  as "site"
,cast("subject number" as varchar) as "subject number"
,cast("subject status" as varchar) as "subject status"
,Null as "study stage"
,cast("date of first icf" as varchar) as "date of first icf"
,Null as "enroll or screen fail date"
,Null as cohort_desc
,Null as "cohort"
,Null as treatment_arm
,null as tumor_type
,Null as race
,Null as ethnic
,Null as dose_level_treatment_arm
,cast("most recent visit" as varchar) as "most recent visit"
,null as "most recent date of visit"
,null as "eotxb date"
,cast("death date" as varchar) as "death date"
,null as "eorfup date"
,null as "eos date"
,null as "eos reason"
,null as "esfu date"
,null as "esfu reason"
,cast("total expected pgs" as int) as "total expected pgs"
,cast("missing pgs (entry not started)" as int) as "missing pgs (entry not started)"
,cast("% missing pgs (entry not started)" as varchar) as "% missing pgs (entry not started)"
,cast("missing <30 days" as varchar) as "missing <30 days"
,cast("% missing <30 days" as varchar) as "% missing <30 days"
,cast("missing 31-60 days" as varchar) as "missing 31-60 days"
,cast("% missing 31-60 days" as varchar) as "% missing 31-60 days"
,cast("missing 61-90 days" as varchar) as "missing 61-90 days"
,cast("% missing 61-90 days" as varchar) as "% missing 61-90 days"
,cast("missing >90 days" as varchar) as "missing >90 days"
,cast("% missing >90 days" as varchar) as "% missing >90 days"
,cast("entered pgs" as int) as "entered pgs"
,cast("% entered pgs" as varchar) as "% entered pgs"
,cast("sv not started" as varchar) as "sv not started"
,cast("% sv not started" as varchar) as "% sv not started"
,cast("sv not started <30 days" as varchar) as "sv not started <30 days"
,cast("% sv not started <30 days" as varchar) as "% sv not started <30 days"
,cast("sv not started 31-60 days" as varchar) as "sv not started 31-60 days"
,cast("% sv not started 31-60 days" as varchar) as "% sv not started 31-60 days"
,cast("sv not started 61-90 days" as varchar) as "sv not started 61-90 days"
,cast("% sv not started 61-90 days" as varchar) as "% sv not started 61-90 days"
,cast("sv not started >90 days" as varchar) as "sv not started >90 days"
,cast("% sv not started >90 days" as varchar) as "% sv not started >90 days"
,cast("sv incomplete" as varchar) as "sv incomplete"
,cast("% sv incomplete" as varchar) as "% sv incomplete"
,cast("sv complete" as varchar) as "sv complete"
,cast("% sv complete" as varchar) as "% sv complete"
,cast("open" as varchar) as "open"
,cast("open <30 days" as varchar) as "open <30 days"
,cast("open 31-60 days" as varchar) as "open 31-60 days"
,cast("open 61-90 days" as varchar) as "open 61-90 days"
,cast("open >90 days" as varchar) as "open >90 days"
,cast("answered pending review" as varchar) as "answered pending review"
,cast("<30 days after report date" as varchar) as "<30 days after report date"
,cast("31-60 days after report date" as varchar) as "31-60 days after report date"
,cast("61-90 days after report date" as varchar) as "61-90 days after report date"
,cast("90-180 days after report date"as varchar) as "90-180 days after report date"
,cast("studyid" as varchar) as "studyid"
,partition_date as load_date
,last_modified_date as EDC_Data_Refreshed_Date
from 
    {{ source ('koios_raw', 'xl092-305_subject_summary') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-305_subject_summary') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-305_subject_summary') }})

)

select * from ctms_stg__subject_summary