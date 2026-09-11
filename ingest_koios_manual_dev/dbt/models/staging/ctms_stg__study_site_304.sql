with ctms_stg__study_site_304 as (
    SELECT region,
    "standard country name",
    cast("study site number" as varchar),
    "pi name",
    "account name",
    "current study site status",
    "actual site selected date",
    "current month expected for activation",
    "week of month",
        CASE
            WHEN "actual site ready to enroll" = 'DROPPED' THEN NULL
            WHEN "actual site ready to enroll" ~~ '%-%-%'::text THEN (("actual site ready to enroll")::text)::date
            ELSE NULL::date
        END AS "actual site ready to enroll",
        CASE
            WHEN "actual site ready to enroll" = 'DROPPED' THEN NULL
            WHEN "actual site ready to enroll" ~~ '%-%-%'::text THEN (("actual site ready to enroll")::text)::date
            ELSE NULL::date
        END AS "actual site ready to enrolcase",
    partition_date,
    last_modified_date,
    study_id,
    md5(study_id || "study site number") AS study_site_id_sk
   FROM {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }})
)


select 
region,
    "standard country name",
    "study site number",
    "pi name",
    "account name",
    "current study site status",
    "actual site selected date",
    "current month expected for activation",
    "week of month",
        CASE
            WHEN "actual site ready to enroll" is not NULL
            THEN to_date(cast("actual site ready to enroll" as TEXT), 'YYYY-MM-DD')
            ELSE NULL::date
        END AS "actual site ready to enroll",
        CASE
            WHEN "actual site ready to enroll" is not NULL
            THEN to_date(cast("actual site ready to enroll" as TEXT), 'YYYY-MM-DD')
            ELSE NULL::date
        END AS "actual site ready to enrolcase",
    partition_date,
    last_modified_date,
    study_id,
    study_site_id_sk
from ctms_stg__study_site_304


-- to_date(cast("actual site ready to enroll" as TEXT), 'YYYY-MM-DD')