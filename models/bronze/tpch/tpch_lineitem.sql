-- tpch_lineitem → bronze_tpch
-- Line-level sales; (l_orderkey, l_linenumber) grain, ~6M rows SF1. Feeds the star-schema fact after Silver.
-- ADR-05, ADR-11.

{{
    config(
        materialized = 'table',
        tags         = ['bronze', 'lineitem', 'tier_1']
    )
}}

select
    l_orderkey,
    l_partkey,
    l_suppkey,
    l_linenumber,
    l_quantity,
    l_extendedprice,
    l_discount,
    l_tax,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_commitdate,
    l_receiptdate,
    l_shipinstruct,
    l_shipmode,
    l_comment,
    {{ add_ingestion_metadata() }}
from {{ source('tpch_sf1', 'lineitem') }}
