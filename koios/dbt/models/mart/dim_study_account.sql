with account_association as(
    select 
    study_protocol_number as study_id
    ,associated_record_id as study_site_id
    ,account_id
    ,to_date(load_date,'YYYY-MM-DD') as load_date
    from {{ref('ctms_stg__account_association')}}
),

account as (
    select
    account_id
    ,name
    from {{ref('ctms_stg__account')}}
),

dim_study_account as(
    select distinct
    md5(study_id) as study_id_sk
    ,md5(a.account_id) as account_id_sk
    ,a.account_id
    ,aa.study_site_id
    ,a.name
    ,aa.load_date
    ,current_timestamp as aud_created_date
    ,current_timestamp as aud_updated_date
    from account_association aa
    inner join account a
    on aa.account_id = a.account_id
)

select * from dim_study_account