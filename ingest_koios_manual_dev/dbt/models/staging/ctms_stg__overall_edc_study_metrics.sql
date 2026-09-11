
with ctm_stg__overall_edc_study_metrics as 
(
 select
 	md5(COALESCE("country",'country')||COALESCE("studyid",'studyid')) as overall_edc_id_sk,
    studyid,
    LEFT(REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES'), -9) AS country_name,
    "on treatment" as on_treatment,
    "screen failed subjects" as screen_failed_subjects,
    "total enrolled subjects" as total_randomized_subjects,
    "in survival fu" as in_survival_fu,
    "off study" as off_study,
    to_date(partition_date,'YYYY-MM-DD') as load_date
from
    {{ source ('koios_raw', 'xb002-101_overall_edc_study_metrics') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xb002-101_overall_edc_study_metrics') }})
    and country LIKE '%Subtotal'AND country != 'Subtotal'
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xb002-101_overall_edc_study_metrics') }})
union all
 select
 	md5(COALESCE("country",'country')||COALESCE("studyid",'studyid')) as overall_edc_id_sk,
    studyid,
    LEFT(REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES'), -9) AS country_name,
    "on treatment" as on_treatment,
    "screen failed subjects" as screen_failed_subjects,
    "total enrolled subjects" as total_randomized_subjects,
    "in survival fu" as in_survival_fu,
    "off study" as off_study,
    to_date(partition_date,'YYYY-MM-DD')load_date
from
    {{ source ('koios_raw', 'xl092-001_overall_edc_study_metrics') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-001_overall_edc_study_metrics') }})
    and country LIKE '%Subtotal'AND country != 'Subtotal'
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-001_overall_edc_study_metrics') }})
union all
 select
 	md5(COALESCE("country",'country')||COALESCE("studyid",'studyid')) as overall_edc_id_sk,
    studyid,
    LEFT(REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES'), -9) AS country_name,
    "on treatment" as on_treatment,
    "screen failed subjects" as screen_failed_subjects,
    "total randomized subjects" as total_randomized_subjects,
    "in survival fu" as in_survival_fu,
    "off study" as off_study,
    to_date(partition_date,'YYYY-MM-DD')load_date
from
    {{ source ('koios_raw', 'xl092-002_overall_edc_study_metrics') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-002_overall_edc_study_metrics') }})
    and country LIKE '%Subtotal'AND country != 'Subtotal'
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-002_overall_edc_study_metrics') }})
union all
 select 
 	md5(COALESCE("country",'country')||COALESCE("studyid",'studyid')) as overall_edc_id_sk,
    studyid,
    LEFT(REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES'), -9) AS country_name,
    "on treatment" as on_treatment,
    "screen failed subjects" as screen_failed_subjects,
    "total randomized subjects" as total_randomized_subjects,
    "in survival fu" as in_survival_fu,
    "off study" as off_study,
    to_date(partition_date,'YYYY-MM-DD')load_date
from
    {{ source ('koios_raw', 'xl092-303_overall_edc_study_metrics') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-303_overall_edc_study_metrics') }})
    and country LIKE '%Subtotal'AND country != 'Subtotal'
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-303_overall_edc_study_metrics') }})
union all
 select 
 	md5(COALESCE("country",'country')||COALESCE("studyid",'studyid')) as overall_edc_id_sk,
    studyid,
    LEFT(REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES'), -9) AS country_name,
    "on treatment" as on_treatment,
    "screen failed subjects" as screen_failed_subjects,
    "total enrolled subjects" as total_randomized_subjects,
    "in survival fu" as in_survival_fu,
    "off study" as off_study,
    to_date(partition_date,'YYYY-MM-DD')load_date
from
    {{ source ('koios_raw', 'xl102-101_overall_edc_study_metrics') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl102-101_overall_edc_study_metrics') }})
    and country LIKE '%Subtotal'AND country != 'Subtotal'
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl102-101_overall_edc_study_metrics') }})
union all
 select 
 	md5(COALESCE("country",'country')||COALESCE("studyid",'studyid')) as overall_edc_id_sk,
    studyid,
    LEFT(REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES'), -9) AS country_name,
    "on treatment" as on_treatment,
    "screen failed subjects" as screen_failed_subjects,
    "total randomized subjects" as total_randomized_subjects,
    "in survival fu" as in_survival_fu,
    "off study" as off_study,
    to_date(partition_date,'YYYY-MM-DD')load_date
from
    {{ source ('koios_raw', 'xl184-315_overall_edc_study_metrics') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl184-315_overall_edc_study_metrics') }})
    and country LIKE '%Subtotal'AND country != 'Subtotal'
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl184-315_overall_edc_study_metrics') }})
union all
 select 
 	md5(COALESCE("country",'country')||COALESCE("studyid",'studyid')) as overall_edc_id_sk,
    studyid,
    LEFT(REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES'), -9) AS country_name,
    "on treatment" as on_treatment,
    "screen failed subjects" as screen_failed_subjects,
    "total randomized subjects" as total_randomized_subjects,
    "in survival fu" as in_survival_fu,
    "off study" as off_study,
    to_date(partition_date,'YYYY-MM-DD')load_date
from
    {{ source ('koios_raw', 'xl092-304_overall_edc_study_metrics') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-304_overall_edc_study_metrics') }})
    and country LIKE '%Subtotal'AND country != 'Subtotal'
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-304_overall_edc_study_metrics') }})
union all
 select 
    distinct
 	md5(COALESCE("country",'country')||COALESCE("studyid",'studyid')) as overall_edc_id_sk,
    studyid,
    LEFT(REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES'), -9) AS country_name,
    "on treatment" as on_treatment,
    "screen failed subjects" as screen_failed_subjects,
    "total randomized subjects" as total_randomized_subjects,
    "in survival fu" as in_survival_fu,
    "off study" as off_study,
    to_date(partition_date,'YYYY-MM-DD')load_date
from
    {{ source ('koios_raw', 'xl092-009_overall_edc_study_metrics') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-009_overall_edc_study_metrics') }})
    and country LIKE '%Subtotal'AND country != 'Subtotal'
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-009_overall_edc_study_metrics') }})
union all
 select 
    distinct
 	md5(COALESCE("country",'country')||COALESCE("studyid",'studyid')) as overall_edc_id_sk,
    studyid,
    LEFT(REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES'), -9) AS country_name,
    "on treatment" as on_treatment,
    "screen failed subjects" as screen_failed_subjects,
    "total enrolled/ randomized subjects" as total_randomized_subjects,
    "in survival fu" as in_survival_fu,
    "off study" as off_study,
    to_date(partition_date,'YYYY-MM-DD')load_date
from
    {{ source ('koios_raw', 'xl092-305_overall_edc_study_metrics') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'xl092-305_overall_edc_study_metrics') }})
    and country LIKE '%Subtotal'AND country != 'Subtotal'
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'xl092-305_overall_edc_study_metrics') }})

)

select * from ctm_stg__overall_edc_study_metrics
