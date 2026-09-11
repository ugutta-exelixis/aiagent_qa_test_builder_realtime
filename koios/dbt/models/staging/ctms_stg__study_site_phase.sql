with country_region_mapping as 
(
    select distinct
    country as country_name,
    region
    ,cast(1000+dense_rank() over (order by country) as varchar) as study_country_id
    ,case 
    when region = 'NA' then 'North America'
    when region = 'APAC' then 'Asia/Pacific'
    when region = 'LATAM' then 'Latin America'
    when region = 'EMEA' then 'Europe/Africa'
    else region
	end as country_region
    from {{ ref('stg_country_region_mapping') }}
),
ctms_stg__study_site_309 as (
    select distinct 
	study_id,
	"region",
	case when country = 'AUS' then 'AUSTRALIA' 
         when country = 'SPN' then  'SPAIN'
         when country = 'SKOR' then 'SOUTH KOREA'
         when country = 'USA' then 'UNITED STATES'
         when country = 'UK' then 'UNITED KINGDOM'
         when country = 'FRA' then 'FRANCE'
         when country = 'ITA' then 'ITALY'
         else country
    end as country_name,
	cast("site number" as varchar) as site_number,
	cast("site name" as varchar) as site_name,
    cast (case when "site number" ='119' then '108' else "site number" end as varchar) as parent_site_number,
	cast("investigator name"  as varchar) as pi_name,
	case
    	when "site status"  = 'Site Activated' then 'Enrollment Open'
    	when "site status"  = 'Site Selected'then 'Selected'
    	else "site status" 
    end as study_site_status,
	case when length("site selection date")> 9 or length("site selection date")< 8  then null
    else cast("site selection date" as varchar) 
    end as actual_site_selected_date,
    null as current_month_expected_for_activation,
	case when length("site activation date")> 9 or length("site activation date")< 8 
    then NULL::date
    else to_date("site activation date" ,'DD-MON-YY') 
    end as activation_date,
	"partition_date" as load_date,
	last_modified_date,
    md5(study_id||country||"site number")as hash_key
	from 
    {{ source ('koios_raw', 'site_contact_details_actual_xl309-101') }}
    where last_modified_date = (select max(last_modified_date) from  {{ source ('koios_raw', 'site_contact_details_actual_xl309-101') }})
    and partition_date = (select max(partition_date) from  {{ source ('koios_raw', 'site_contact_details_actual_xl309-101') }})
    and "site number" is not null 
),
ctms_stg__study_site_201 as (
select distinct
	REPLACE(upper(country), 'UNITED STATES OF AMERICA', 'UNITED STATES') as country_name,
	cast("parexel site reference" as varchar) as site_number,
    cast("parexel site reference" as varchar) as parent_site_number,
	cast("primary investigator name" as varchar) as pi_name,
	cast("institution/center name" as varchar) as site_name,
	case when "site activation actual date" is not null and "selected date" is not null then 'Enrollment Open' 
	when "site activation actual date" is null and "selected date" is not null then 'Selected'
	else 'Planned' end as study_site_status,
	cast("selected date" as varchar) as actual_site_selected_date,
	null as current_month_expected_for_activation,
	null as week_of_month,
	null as actual_site_ready_to_enroll,
	partition_date as load_date,
	last_modified_date,
	study_id
    from {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }}
    where last_modified_date=(select max(last_modified_date) from {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }})
    and partition_date=(select max(partition_date) from {{ source ('koios_raw', 'site_activation_readiness_xl092-201') }})
),
ctms_stg__study_site_011 as (
select distinct
	REPLACE(upper("pi - country"), 'UNITED STATES OF AMERICA', 'UNITED STATES') as country_name,
	cast(id as varchar) as site_number,
    cast(id as varchar) as parent_site_number,
	cast("pi - contact" as varchar) as pi_name,
	cast("site - name" as varchar) as site_name,
    "site - status" as study_site_status,
	cast("startup - activation date - planned" as varchar) as actual_site_selected_date,
	null as current_month_expected_for_activation,
	null as week_of_month,
	null as actual_site_ready_to_enroll,
	partition_date as load_date,
	last_modified_date,
	study_id
    from {{ source ('koios_raw', 'site_information_report_xl092-011') }}
    where last_modified_date=(select max(last_modified_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})
    and partition_date=(select max(partition_date) from {{ source ('koios_raw', 'site_information_report_xl092-011') }})

)

select 
	rm.country_region as region,
	sc.country_name,
	rm.study_country_id,
	site_number,
    parent_site_number,
	pi_name,
	site_name,
	study_site_status,
	actual_site_selected_date,
	current_month_expected_for_activation,
	null as week_of_month,
	to_char(activation_date, 'DD-MON-YY') as actual_site_ready_to_enroll,
	load_date,
	last_modified_date,
	cast(study_id as varchar) as study_id
	from ctms_stg__study_site_309 sc
    left join country_region_mapping rm
    on sc.country_name = rm.country_name
    union
    select 
	rm.country_region as region,
	ps.country_name,
	rm.study_country_id,
	site_number,
    parent_site_number,
	pi_name,
	site_name,
	study_site_status,
	actual_site_selected_date,
	current_month_expected_for_activation,
	week_of_month,
	actual_site_ready_to_enroll,
	ps.load_date,
	ps.last_modified_date,
	ps.study_id
    from ctms_stg__study_site_201 ps
    left join country_region_mapping rm
    on ps.country_name = rm.country_name
	union
    select 
	rm.country_region as region,
	ms.country_name,
	rm.study_country_id,
	site_number,
    parent_site_number,
	pi_name,
	site_name,
	study_site_status,
	actual_site_selected_date,
	current_month_expected_for_activation,
	week_of_month,
	actual_site_ready_to_enroll,
	ms.load_date,
	ms.last_modified_date,
	ms.study_id
    from ctms_stg__study_site_011 ms
    left join country_region_mapping rm
    on ms.country_name = rm.country_name 