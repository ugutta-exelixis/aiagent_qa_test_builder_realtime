with date_range as (
    select GENERATE_SERIES('2015-01-01'::timestamp, '2030-12-31'::timestamp, '1 day'::INTERVAL) as date_actual
),

dim_calendar as (
select
    date_actual,
    date(date_actual) as date,
    EXTRACT(YEAR FROM date_actual) as year,
    EXTRACT(QUARTER FROM date_actual) as quarter_num,
    EXTRACT(MONTH FROM date_actual) as month_num,
    EXTRACT(DAY FROM date_actual) as day_num,
    EXTRACT(DOW FROM date_actual) as dow_num,
    TO_CHAR(date_actual, 'Day') as day,
    CASE WHEN EXTRACT(ISODOW FROM date_actual) IN (6, 7) THEN TRUE ELSE FALSE END as is_weekend
from date_range
)

select * from dim_calendar