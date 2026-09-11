with study_ids as (
    select 'XB002-101' as study_id union all
    select 'XL092-002' union all
    select 'XB010-101' union all
    select 'XL092-303' union all
    select 'XL092-304' union all
    select 'XL092-305' union all
    select 'XL092-009' union all
    select 'XL495-101' union all
    select 'XL309-101' union all
    select 'XB628-101'
),

kpis as (
    select 'Actual sites ≤ Planned sites on site activation chart' as kpi_name union all
    select 'Actual countries ≤ Planned countries On country Activation Drill down' union all
    select 'Actual enrolled participant count ≤ Planned Participant count' union all
    select 'Today''s Active country count ≥ Previous Day''s Active country count' union all
    select 'Today''s Enrolled Participant ≥ Previous Day''s Enrolled Participant' union all
    select 'Today''s Active site count ≥ Previous Day''s Active site count' union all
    select 'Active country count(country activation widget) = Drill down Active country count' union all
    select 'Active site count (Site activation Widget) = Drill down Active site count' union all
    select 'Total Enrolled Participant(Enrolment Progress widget) + Drill down enrolled participant count' union all
    select 'Screen Failure %(Screen failure widget) = Drill down Screen failure %' union all
    select 'Total Screened (Total Screened Widget) = Drill Down Total Screen count' union all
    select 'In screening in last 30 days(In screening since 30 days widget) = Drill down in screening in last 30 days count' union all
    select 'Currently in screeining(Currently in screening widget) = Drill down currently in screening count' union all
    select 'Enrolled in last 30 days(Widget count) = Drill down Enrolled in 30 days count' union all
    select 'Data refresh date on the Dashboard is previous day' union all
    select 'Currently in screening  ≤ Total Screened' union all
    select 'In-screening in 30 days ≤ Total Screened' union all
    select 'Enrolled in 30 days ≤ Total Enrolled' union all
    select 'Active on IP ≤ Total Enrolled and Active on Study' union all
    select 'Active on study ≤ Total Enrolled' union all
    select 'Total number on Pie chart = Total Enrolled Participant'
)
 
select
    s.study_id,
    k.kpi_name
from study_ids s
cross join kpis k
