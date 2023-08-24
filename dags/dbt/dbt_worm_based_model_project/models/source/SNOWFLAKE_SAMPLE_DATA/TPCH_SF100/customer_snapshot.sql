{{
  config(
    materialized = "incremental",
    pre_hook="delete from {{this}} where snapshot_date={{ var('refresh_date') }} "
  )
}}


with raw_data as(
  select *
  from {{ source('TPCH_SF100','CUSTOMER') }}
)
select
  current_date() as snapshot_date,
  *
from raw_data
