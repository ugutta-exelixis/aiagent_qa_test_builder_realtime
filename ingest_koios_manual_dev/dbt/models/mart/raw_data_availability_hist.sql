{{
    config(
        materialized='incremental'
    )
}}

select
    *
    from koios_mart.raw_data_availability

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  where load_date > (select max(load_date) from {{ this }})

{% endif %}