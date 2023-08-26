{{
  config(
    materialized = "incremental",
    pre_hook="delete from {{this}} where period='1992'"
  )
}}

with raw_data as(
  select
      year(cust_order.o_orderdate) as period,
      cust.c_name,
      cust.c_mktsegment,
      cust_order.o_orderstatus,
      cust_order.o_orderpriority,
      sum(O_TOTALPRICE) as total_price,
      current_timestamp() as data_refresh_timestamp
  from {{ source('TPCH_SF1','ORDERS') }} cust_order
  left join {{ ref('CUSTOMER_SNAPSHOT') }} cust
      on cust_order.o_custkey = cust.c_custkey
  where year(cust_order.o_orderdate) = '1992'
      and cust.snapshot_date = '{{ var('refresh_date') }}'
  and cust_order.o_orderpriority in (
      '1-URGENT',
      '2-HIGH',
      '3-MEDIUM',
      '4-NOT SPECIFIED',
      '5-LOW'
  )
  group by
  period,
  c_name,
  c_mktsegment,
  o_orderstatus,
  o_orderpriority
)
select
  to_varchar(period) as period,
  c_name,
  c_mktsegment,
  o_orderstatus,
  o_orderpriority,
  total_price,
  data_refresh_timestamp
from raw_data
