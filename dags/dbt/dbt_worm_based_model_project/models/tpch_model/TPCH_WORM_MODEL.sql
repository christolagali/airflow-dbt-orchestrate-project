{{
  config(
    materialized = "incremental",
    pre_hook="delete from {{this}} where period='1994' and date(data_refresh_timestamp) = TO_DATE('{{ var('refresh_date') }}')"
  )
}}

with raw_data as(
  select
      year(orders.o_orderdate) as Period,
      orders.O_CLERK as Clerk_ID,
      cust.C_MKTSEGMENT as Market_Segment,
      sum(lineitems.l_extendedprice * (1-lineitems.l_discount) * (1+lineitems.l_tax)) as Amount_Sold,
      current_timestamp() as data_refresh_timestamp
  from {{ source('TPCH_SF1','LINEITEM') }} lineitems
  left join {{ source('TPCH_SF1','ORDERS') }} orders
      on lineitems.L_ORDERKEY = orders.o_orderkey
  inner join {{ ref('CUSTOMER_SNAPSHOT') }} cust
      on orders.o_custkey = cust.c_custkey
  where year(orders.o_orderdate) = '1994'
      and cust.snapshot_date = '{{ var('refresh_date') }}'
  group by
    Period,
    Clerk_ID,
    Market_Segment
)
select
  Period,
  Clerk_ID,
  Market_Segment,
  Amount_Sold,
  data_refresh_timestamp
from raw_data
