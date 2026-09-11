
with ctms_stg__site_contact_details_xl092_304 as (
SELECT cast("site #" as varchar),
    "pi name",
    "pi prefix",
    "site status",
    "account name",
    "account name 2",
    department,
    "address line 2",
    "address line 3",
    city,
    "state/province",
    "site country",
    "postal code",
    "pi phone #",
    "pi fax #",
    "pi email",
    "study coordinator name",
    "study coordinator email",
    "study coordinator phone #",
    partition_date,
    last_modified_date,
    study_id,
    md5(study_id::text || "site #") AS study_site_id_sk
   FROM {{ source ('koios_raw', 'site_contact_details_xl092-304') }}
   where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_xl092-304') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_xl092-304') }})

)


select * from ctms_stg__site_contact_details_xl092_304
