with pi_info_309 as(
   select "study number" as study_id, "site number" as site_number,"external id" as external_id,
    "full name" as pi_name, phone as pi_phone, email as pi_email 
    FROM 
        {{ source ('koios_raw', 'site_contact_details_xl309-101') }}
        where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_xl309-101') }})
        and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_xl309-101') }})
        and "study team role" in('Primary Investigator','Principal Investigator')
),
pi_info_201 as(
    select study_id,site_number,external_id, pi_name, pi_email,  pi_phone from (
    select "study site > study number" as study_id, "study site > site number" as site_number,"study person > start date",
    "user > external id" as external_id,"study person > full name" as pi_name,"user > phone" as pi_phone,"user > email" as pi_email ,
    row_number()over(partition by "study site > study number"||"study site > site number" order by 
    (case when "study person > full name" is not null and "user > phone" is not null and "user > email" is not null then 1 else 2 end),
    "study site > site number",cast("study person > start date" as date) DESC) as pi_rank
      from {{ source ('koios_raw', 'site_contact_details_xl092-201') }}
      where "study person > study team role" in ('Principal Investigator')
      group by "study site > study number","study site > site number","study person > start date","user > external id","study person > full name","user > phone","user > email") as a where pi_rank=1
),
pi_info_011 as(
    select  study_id, id as site_number,null as external_id,trim(split_part("pi - contact", ';', 1)) as pi_name,trim(replace(split_part("pi - contact", ';', 2), 'Ph:', '')) as pi_phone,trim(split_part("pi - contact", ';', 4)) as pi_email
      from {{ source ('koios_raw', 'site_information_report_xl092-011') }}
       where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})
        and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})
    --   and "study person > study team role" in ('Principal Investigator')
),
sc_info_309 as(
    select study_id,site_number,external_id, sc_name, sc_email,  sc_phone from (
select 
		"study number" as study_id, "site number" as site_number,"start date","full name"as sc_name,email as sc_email, phone as sc_phone,"external id" as external_id,
		row_number()OVER(partition by "study number"||"site number"
		order by 
        (case when "full name" is not null and  "phone" is not null and  "email" is not null then  1 else 2 end),
        "site number","external id" desc, cast("start date" as date) DESC) as CA_RANK
		FROM 
        {{ source ('koios_raw', 'site_contact_details_xl309-101') }}
        where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_xl309-101') }})
        and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_xl309-101') }})
		and "study team role" in ('Study Coordinator','Study Coordinator - Unblinded')
        -- and "full name" is not null
		-- and phone is not null
		-- and email is not null
		group by "study number", "site number","start date","external id","full name",email,phone
	) as a where ca_rank= 1
),
sc_info_201 as(
    select study_id,site_number,external_id, sc_name, sc_email,  sc_phone from (
    select "study site > study number" as study_id, "study site > site number" as site_number,"study person > start date",
    "user > external id" as external_id,"study person > full name" as sc_name,"user > phone" as sc_phone,"user > email" as sc_email ,
    row_number()over(partition by "study site > study number"||"study site > site number" order by 
    (case when "study person > full name" is not null and "user > phone" is not null and "user > email" is not null then 1 else 2 end),
    "study site > site number",cast("study person > start date" as date) DESC) as sc_rank
      from {{ source ('koios_raw', 'site_contact_details_xl092-201') }}
      where "study person > study team role" in ('Clinical Research Coordinator')
      group by "study site > study number","study site > site number","study person > start date","user > external id","study person > full name","user > phone","user > email") as a where sc_rank=1
),
sc_info_011 as(
    select  study_id, id as site_number,null as external_id,trim(split_part("crc - contact", ';', 1)) as sc_name,trim(replace(split_part("crc - contact", ';', 2), 'Ph:', '')) as sc_phone,trim(split_part("crc - contact", ';', 4)) as sc_email
      from {{ source ('koios_raw', 'site_information_report_xl092-011') }}
       where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})
        and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})
)
-- ,
-- final_sc_309 as(
-- select 
-- c.study_id, c.site_number,
-- s."full name" as sc_name, s.phone as sc_phone, s.email as sc_email
-- from {{ source ('koios_raw', 'site_contact_details_xl309-101') }} s
-- left join sc_info_309 c
-- on s."site number"=c.site_number and s."external id" = c.external_id
-- where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_xl309-101') }})
--         and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_xl309-101') }})
-- )

select distinct 
	"study number" as study_id,
	md5("study number"||"site number") as study_site_id_sk,
    cast("site number" as varchar),
    "lifecycle state" as "site status",
    "site name",
    null as department,
    "address 1" as address,
    "address 2" ,
    "address 3",
    city,
    state,
    country as "site country",
    "postal / zip code",
    pi.pi_name,
    pi.pi_phone,
    pi.pi_email,
    sc.sc_name,
    sc.sc_phone,
    sc.sc_email,
    partition_date as load_date
	from 
	{{ source ('koios_raw', 'site_contact_details_xl309-101') }} si
    left join pi_info_309 pi
    on si."site number"= pi.site_number
    left join sc_info_309 sc
    on si."site number"= sc.site_number
   	where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_xl309-101') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_xl309-101') }})
union 
select distinct
    si.study_id,
	md5(si.study_id||"study site > site number") as study_site_id_sk,
    cast("study site > site number" as varchar),
    "study site > lifecycle state" as study_site_status,
    "location > site name" as "site name",
    null as department,
    "location > address 1" as address,
    "location > address 2" as "address 2" ,
    "location > address 3" as "address 3",
    "location > town / city" as city,
    "location > state / province / region" as state,
    REPLACE(upper("study site > country"), 'UNITED STATES OF AMERICA', 'UNITED STATES') as "site country",
    "location > postal / zip code" as "postal / zip code",
    pi.pi_name,
    pi.pi_phone,
    pi.pi_email,
    sc.sc_name,
    sc.sc_phone,
    sc.sc_email,
    partition_date as load_date
    from {{ source ('koios_raw', 'site_contact_details_xl092-201') }} si
    left join pi_info_201 pi
    on si."study site > site number"=pi.site_number
    left join sc_info_201 sc
    on si."study site > site number"=sc.site_number
    where last_modified_date=(select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_xl092-201') }})
    and partition_date=(select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_xl092-201') }})
    and si."study site > site number" ~ '^[0-9]+$'
union 
select distinct
    si.study_id,
	md5( si.study_id ||ID) as study_site_id_sk,
    cast(ID as varchar),
    "site - status" as "site status",
    "site - name" as "site name",
    null as department,
    "pi - address 1" as address,
    "pi - address 2" as "address 2",
    "pi - address 3" as "address 3",
    "pi - city" as city,
    "pi - state" as state,
    "pi - country" as "site country",
    "pi - zip" as "postal / zip code",
    pi.pi_name,
    pi.pi_phone,
    pi.pi_email,
    sc.sc_name,
    sc.sc_phone,
    sc.sc_email,
    partition_date as load_date
	from 
	{{ source ('koios_raw', 'site_information_report_xl092-011') }} si
    left join pi_info_011 pi
    on si.id = pi.site_number
    left join sc_info_011 sc
    on si.id = sc.site_number
   	where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})
