{{
  config(
    materialized = "incremental",
    pre_hook="delete from {{this}} where snapshot_date='{{ var('refresh_date') }}' "
  )
}}


with raw_data as(
  select *
  from {{ source('TPCH_SF1','CUSTOMER') }}
)
select
  TO_DATE('{{ var('refresh_date') }}') as snapshot_date,
  *
from raw_data
