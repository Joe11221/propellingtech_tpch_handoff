-- tpch_orders → bronze_tpch
-- TPC-H `orders` — parent of `lineitem`; `o_custkey` points at customer. ADR-05, ADR-11.

{{
    config(
        materialized = 'table',
        tags         = ['bronze', 'orders']
    )
}}

select
    o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment,
    {{ add_ingestion_metadata() }}
from {{ source('tpch_sf1', 'orders') }}
