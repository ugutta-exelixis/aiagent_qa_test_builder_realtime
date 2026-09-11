with site_address as (
    select distinct study_id,address_id,associated_record_id,address_type,
    site_address,city,state_province,postal_code,country from (
select
	a.study_protocol_number as study_id,
	a.address_id,
	a.associated_record_id,
	a.type as address_type,
	concat(c.line1,' ',c.line2,' ',c.line3) as site_address,
	c.city,
	c.state_province,
	c.postal_code,
	c.country ,
	ROW_NUMBER() OVER (
        PARTITION BY associated_record_id 
        ORDER BY 
            CASE 
                WHEN type = 'Physical Address' THEN 1 
                ELSE 2 
            END, 
            type)as rnk
	from 
		{{ref ('ctms_stg__address_association')}} a 
		left JOIN {{ref ('ctms_stg__address')}} c 
		on a.address_id = c.address_id		
) a
where rnk = 1
union all
select 
distinct study_id
,null as address_id
,"site number" as associated_record_id
,'Physical Address' as address_type
,address site_address
,city
,state as state_province
,"postal / zip code" as postal_code
,upper("site country") as country
from {{ref ('ctms_stg__site_contact_details_phase')}}

)
select * from site_address