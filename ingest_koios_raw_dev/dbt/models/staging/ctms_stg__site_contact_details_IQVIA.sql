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
   FROM 
   {{ source ('koios_raw', 'site_contact_details_xl092-304') }}
   where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_xl092-304') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_xl092-304') }})
    and "site #" is not null
),
sc_info_009 as(
  select * from(
  select  distinct 
  "site #" ,
  "contact name" ,
  "contact phone #" ,
  "site id",
  "contact email" ,
  "contact row id",
  RANK()OVER(partition by "site #" 
		order by "contact row id" ,
	case 
		when "alternate phone #" is not null then 1
		else 2
	end
	) as CA_RANK
  from 
  {{ source ('koios_raw', 'site_contact_details_xl092-009') }}
  where "role" = 'Study Coordinator'
  and "site #" is not null 
  and last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_xl092-009') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_xl092-009') }})
) as a where ca_rank=1
   ),
ctms_stg__site_contact_details_xl092_009 as (
	select distinct 
	cast(ss."site #" as varchar),
	ss."site id",
    ss."contact name" as pi_name,
    case
    	when "site status" in ('Regulatory Green Light', 'Essential Documents in place') then 'Selected'
    	else "site status"
    end as "site status",
    ss."account name",
    ss.department,
    ss.address ,
    ss."address line 2",
    ss."address line 3",
    ss.city,
    ss."state/province",
    ss.country ,
    ss."postal code",
    ss."contact phone #" as pi_phone,
    ss."contact email" as pi_email,
    sc."contact name" as sc_name,
	sc."contact email" as sc_email,
	sc."contact email" as sc_phone,
    ss.partition_date,
    ss.last_modified_date,
    ss."protocol #" as study_id ,
    md5(ss."protocol #"::text || ss."site #") AS study_site_id_sk
from 
   {{ source ('koios_raw', 'site_contact_details_xl092-009') }} ss
   left join sc_info_009 sc
   on ss."site #" = sc."site #"
   and ss."site id" = sc."site id"
   where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_xl092-009') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_xl092-009') }})
   and ss."role" = 'Principal Investigator'
  and ss."site #" is not null
)
select 
study_id,
study_site_id_sk,
"site #",
"site status",
"account name" ,
department,
null as address,
"address line 2" ,
"address line 3",
city,
"state/province",
"site country",
"postal code",
"pi name",
"pi phone #",
"pi email",
"study coordinator name",
"study coordinator phone #",
"study coordinator email",
partition_date as load_date
from ctms_stg__site_contact_details_xl092_304
union all
select 
study_id,
study_site_id_sk,
"site #",
"site status",
"account name" ,
department,
address,
"address line 2" ,
"address line 3",
city,
"state/province",
country,
"postal code",
pi_name,
pi_phone,
pi_email,
sc_name,
sc_phone,
sc_email,
partition_date as load_date
from ctms_stg__site_contact_details_xl092_009
union all
select distinct 
    "protocol #" as study_id,
    md5("protocol #"||"site #") as study_site_id_sk,
    cast("site #" as varchar),
    "site status",
    "account name",
    department,
    address as address,
    "address line 2" ,
    "address line 3",
    city,
    "state/province",
    case
        when upper("site country") = 'KOREA, REPUBLIC OF' then 'SOUTH KOREA'
        when upper("site country") = 'TAIWAN, REPUBLIC OF CHINA' then 'TAIWAN' 
        else upper("site country")
    end as "site country",
    "postal code",
    "pi name",
    "pi phone #",
    "pi email",
    "study coordinator name",
    "study coordinator phone #",
    "study coordinator email",
    partition_date as load_date
    from {{ source ('koios_raw', 'site_contact_details_xl092-305') }}
   	where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_xl092-305') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_xl092-305') }})