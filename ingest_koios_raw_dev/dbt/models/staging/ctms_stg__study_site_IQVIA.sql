with country_region_mapping as 
(
    select distinct
    country as country_name
    ,cast(1000+dense_rank() over (order by country) as varchar) as study_country_id
    ,region as country_region
    from {{ ref('stg_country_region_mapping') }}
),
ctms_stg__study_site_304 as (
    SELECT region,
    case
        when upper("standard country name") = 'KOREA, REPUBLIC OF' then 'SOUTH KOREA'
        when upper("standard country name") = 'TAIWAN, REPUBLIC OF CHINA' then 'TAIWAN' 
        else upper("standard country name")
    end as country_name,
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
    partition_date,
    last_modified_date,
    study_id,
    md5(study_id||"standard country name"||"study site number")as hash_key
    FROM {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }}
    where last_modified_date = (select max(last_modified_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }})
    and partition_date = (select max(partition_date) from {{ source ('koios_raw', 'site_contact_details_actual_xl092-304') }})
),
ctms_stg__study_site_009 as 
(
	select distinct 
	study_id,
	null as region,
	case
        when upper(country) = 'KOREA, REPUBLIC OF' then 'SOUTH KOREA'
        when upper(country) = 'TAIWAN, REPUBLIC OF CHINA' then 'TAIWAN' 
        else upper(country)
    end as country_name,
	cast("site number" as varchar) as site_number,
	cast("account name" as varchar) as site_name,
	cast("pi name" as varchar) as pi_name,
	case
    	when "current study site status" in ('Regulatory Green Light', 'Essential Documents in place') then 'Selected'
    	else "current study site status"
    end as study_site_status,
	cast("actual site selected date" as varchar) as actual_site_selected_date,
	cast("planned site activation date" as varchar) as current_month_expected_for_activation,
	case when "actual site activation date" = '00-Jan-00'
	then NULL::date 
	else to_date(cast("actual site activation date" as text),'DD-MON-YY')
	end as activation_date,
	"partition_date" as load_date,
	last_modified_date,
    md5(study_id||country||"site number")as hash_key
	from 
    {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }}
    where last_modified_date = (select max(last_modified_date) from  {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }})
    and partition_date = (select max(partition_date) from  {{ source ('koios_raw', 'site_contact_details_actual_xl092-009') }})
),
ctms_stg__study_site_305 as 
(select distinct 
	study_id,
	null as region,
	case
        when upper("standard country name") = 'KOREA, REPUBLIC OF' then 'SOUTH KOREA'
        when upper("standard country name") = 'TAIWAN, REPUBLIC OF CHINA' then 'TAIWAN' 
        else upper("standard country name")
    end as country_name,
	cast("study site number" as varchar) as site_number,
	cast("account name" as varchar) as site_name,
	cast("pi name" as varchar) as pi_name,
	case
    	when "current study site status" in ('Regulatory Green Light', 'Essential Documents in place') then 'Selected'
    	else "current study site status"
    end as study_site_status,
	cast("actual site selected date" as varchar) as actual_site_selected_date,
	cast("planned site ready to enroll" as varchar) as current_month_expected_for_activation,
	case when "actual site ready to enroll" = '00-Jan-00'
	then NULL::date 
	else to_date(cast("actual site ready to enroll" as text),'DD-MON-YY')
	end as activation_date,
	"partition_date" as load_date,
	last_modified_date,
    md5(study_id||"standard country name"||"study site number")as hash_key
	from 
    {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }}
    where last_modified_date = (select max(last_modified_date) from  {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }})
    and partition_date = (select max(partition_date) from  {{ source ('koios_raw', 'site_contact_details_actual_xl092-305') }})
)
select 
	cast(region as varchar) as region,
    upper(cast(sc.country_name as varchar)) as country_name,
    rm.study_country_id,
    cast("study site number"as varchar) as site_number,
    cast("pi name"as varchar) as pi_name,
    cast("account name"as varchar) as site_name,
    cast("current study site status"as varchar) as study_site_status,
    cast("actual site selected date"as varchar) as actual_site_selected_date,
    cast("current month expected for activation"as varchar) as current_month_expected_for_activation,
    cast("week of month"as varchar) as week_of_month,
        CASE
            WHEN "actual site ready to enroll" is not NULL
            THEN to_date(cast("actual site ready to enroll" as TEXT), 'YYYY-MM-DD')
            ELSE NULL::date
        END AS actual_site_ready_to_enroll,
    partition_date as load_date,
    last_modified_date,
    cast(study_id as varchar) as study_id
    from ctms_stg__study_site_304 sc
    left join country_region_mapping rm
    on sc.country_name = rm.country_name
union all 
select 
	rm.country_region as region,
	sc.country_name,
	rm.study_country_id,
	site_number,
	pi_name,
	site_name,
	study_site_status,
	actual_site_selected_date,
	current_month_expected_for_activation,
	null as week_of_month,
	activation_date as actual_site_ready_to_enroll,
	load_date,
	last_modified_date,
	cast(study_id as varchar) as study_id
	from ctms_stg__study_site_009 sc
    left join country_region_mapping rm
    on sc.country_name = rm.country_name
union all 
select 
	rm.country_region as region,
	sc.country_name,
	rm.study_country_id,
	site_number,
	pi_name,
	site_name,
	study_site_status,
	actual_site_selected_date,
	current_month_expected_for_activation,
	null as week_of_month,
	activation_date as actual_site_ready_to_enroll,
	load_date,
	last_modified_date,
	cast(study_id as varchar) as study_id
	from ctms_stg__study_site_305 sc
    left join country_region_mapping rm
    on sc.country_name = rm.country_name