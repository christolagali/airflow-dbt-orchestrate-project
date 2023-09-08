{% if var('execution_mode') == 'partial_refresh' %}

/*
*  Partial Refresh
*/

  {{
    config(
      materialized = "incremental",
      pre_hook="delete from {{this}} where period={{ var('refresh_year') }} and Market_Segment in ('MACHINERY','AUTOMOBILE')"
    )
  }}

  with raw_data as(
    select
        year(orders.o_orderdate) as Period,
        orders.O_CLERK as Clerk_ID,
        cust.C_MKTSEGMENT as Market_Segment,
        sum(lineitems.l_extendedprice * (1-lineitems.l_discount) * (1+lineitems.l_tax)) as Amount_Sold,
        current_timestamp() as data_refresh_timestamp
    from {{ source('TPCH_SF100','LINEITEM') }} lineitems
    left join {{ source('TPCH_SF100','ORDERS') }} orders
        on lineitems.L_ORDERKEY = orders.o_orderkey
    left join {{ source('TPCH_SF100','CUSTOMER') }} cust
        on orders.o_custkey = cust.c_custkey
    where year(orders.o_orderdate) = '{{ var("refresh_year") }}'
    and cust.C_MKTSEGMENT in (
      'MACHINERY',
      'AUTOMOBILE'
  )
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


{% else %}

/*
*  Complete Refresh
*/

  {{
    config(
      materialized = "incremental",
      pre_hook="delete from {{this}} where period={{ var('refresh_year') }} and Market_Segment in ('MACHINERY','AUTOMOBILE','FURNITURE','HOUSEHOLD','BUILDING')"
    )
  }}

  with raw_data as(
    select
        year(orders.o_orderdate) as Period,
        orders.O_CLERK as Clerk_ID,
        cust.C_MKTSEGMENT as Market_Segment,
        sum(lineitems.l_extendedprice * (1-lineitems.l_discount) * (1+lineitems.l_tax)) as Amount_Sold,
        current_timestamp() as data_refresh_timestamp
    from {{ source('TPCH_SF100','LINEITEM') }} lineitems
    left join {{ source('TPCH_SF100','ORDERS') }} orders
        on lineitems.L_ORDERKEY = orders.o_orderkey
    left join {{ source('TPCH_SF100','CUSTOMER') }} cust
        on orders.o_custkey = cust.c_custkey
    where year(orders.o_orderdate) = '{{ var("refresh_year") }}'
    and cust.C_MKTSEGMENT in (
      'MACHINERY',
      'AUTOMOBILE',
      'FURNITURE',
      'HOUSEHOLD',
      'BUILDING'
  )
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

{% endif %}
