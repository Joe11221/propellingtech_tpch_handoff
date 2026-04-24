-- silver_sales.orders — typed header + calendar parts for joins. Order status
-- stays as source codes; human-readable labels belong in Gold. ADR-11.

{{
    config(
        materialized = 'table',
        tags         = ['silver', 'sales', 'orders']
    )
}}

select
    o_orderkey::bigint                              as order_id,
    o_custkey::bigint                               as customer_id,
    o_orderstatus::varchar                          as order_status_code,
    o_totalprice::number(12,2)                      as order_total_price,
    o_orderdate::date                               as order_date,
    trim(o_orderpriority)::varchar                  as order_priority,
    trim(o_clerk)::varchar                          as clerk_id,
    o_shippriority::int                             as ship_priority,
    trim(o_comment)::varchar                        as order_comment,

    extract(year  from o_orderdate)::int            as order_year,
    extract(month from o_orderdate)::int            as order_month,
    extract(quarter from o_orderdate)::int          as order_quarter,

    _ingested_at,
    _source_system,
    _batch_id

from {{ ref('tpch_orders') }}

where o_orderkey is not null
  and o_custkey  is not null
  and o_orderdate is not null
