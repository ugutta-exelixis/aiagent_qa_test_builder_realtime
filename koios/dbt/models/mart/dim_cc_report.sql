with today_data as (
    select activity_type_actual,
    protocol_number,
    study_phase_norm,
--  to_char(to_date(planned_finish, 'MM/DD/YY'), 'MM/DD/YYYY') as Today_Planned_Date,
    planned_finish as Today_Planned_Date,
    actual_finish as Today_Actual_Date,
--     to_char(to_date(actual_finish, 'MM/DD/YY'), 'MM/DD/YYYY') as Today_Actual_Date,
    load_date as today_load,
    protocol_activity_type_actual_sk
    from {{ref ('planisware_daily_hist')}}
        where load_date=(select max(load_date) from {{ref ('planisware_daily_hist')}})
        and status='ok'
        and protocol_number ~ '^X[A-Z][0-9]{3}-[0-9]{3}$'),
y_data as (
        select 
        activity_type_actual,
        protocol_number,
        study_phase_norm,
        -- to_char(to_date(planned_finish, 'MM/DD/YY'), 'MM/DD/YYYY') as Yesterday_Planned_Date,
        planned_finish as Yesterday_Planned_Date,
        -- to_char(to_date(actual_finish, 'MM/DD/YY'), 'MM/DD/YYYY') as Yesterday_Actual_Date,
        actual_finish as Yesterday_Actual_Date,
        load_date as yesterday_load,
        protocol_activity_type_actual_sk
        from {{ref ('planisware_daily_hist')}}
        where load_date=(select distinct load_date from {{ref ('planisware_daily_hist')}}
        order by load_date desc offset 1 limit 1)
        and status ='ok'
        and protocol_number ~ '^X[A-Z][0-9]{3}-[0-9]{3}$'),
pln_dt as (
select td.activity_type_actual,
        td.protocol_number,
        td.study_phase_norm,
        td.Today_Planned_Date,
        yd.Yesterday_Planned_Date,
        null as Today_Actual_Date,
        null as Yesterday_Actual_Date,
        today_load,
        yesterday_load,
        td.protocol_activity_type_actual_sk
        from today_data td
join y_data yd on 
td.protocol_activity_type_actual_sk=yd.protocol_activity_type_actual_sk
    where 
    (yd.Yesterday_Planned_Date IS NULL AND td.Today_Planned_Date IS NOT NULL)
  -- case 2: today is null, yesterday has a date
  OR (yd.Yesterday_Planned_Date IS NOT NULL AND td.Today_Planned_Date IS NULL)
  -- case 3: date value changed
  OR (yd.Yesterday_Planned_Date IS NOT NULL AND td.Today_Planned_Date IS NOT NULL AND yd.Yesterday_Planned_Date <> td.Today_Planned_Date)),
act_dt as (
select td.activity_type_actual,
        td.protocol_number,
        td.study_phase_norm,
        null as Today_Planned_Date,
        null as Yesterday_Planned_Date,
        td.Today_Actual_Date,
        yd.Yesterday_Actual_Date,
        td.protocol_activity_type_actual_sk
        from today_data td
join y_data yd on td.protocol_activity_type_actual_sk=yd.protocol_activity_type_actual_sk
    where 
    (yd.Yesterday_Actual_Date IS NULL AND td.Today_Actual_Date IS NOT NULL)
  -- case 2: today is null, yesterday has a date
  OR (yd.Yesterday_Actual_Date IS NOT NULL AND td.Today_Actual_Date IS NULL)
  -- case 3: date value changed
  OR (yd.Yesterday_Actual_Date IS NOT NULL AND td.Today_Actual_Date IS NOT NULL AND yd.Yesterday_Actual_Date <> td.Today_Actual_Date))
  select 
        pd.activity_type_actual,
        pd.protocol_number,
        pd.study_phase_norm,
        pd.Today_Planned_Date,
        pd.Yesterday_Planned_Date,
        ad.Today_Actual_Date,
        ad.Yesterday_Actual_Date,
        today_load as Load_Date,
        yesterday_load,
        pd.protocol_activity_type_actual_sk
  from pln_dt pd left join act_dt ad on pd.protocol_activity_type_actual_sk = ad.protocol_activity_type_actual_sk
    union
select 
        acd.activity_type_actual,
        acd.protocol_number,
        acd.study_phase_norm,
        null as Today_Planned_Date,
        null as Yesterday_Planned_Date,
        Today_Actual_Date,
        Yesterday_Actual_Date,
        null as Load_Date,
        null as yesterday_load,
        acd.protocol_activity_type_actual_sk
        from act_dt as acd
        where acd.protocol_activity_type_actual_sk not in (select pd.protocol_activity_type_actual_sk from pln_dt pd)
