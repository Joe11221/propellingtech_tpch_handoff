-- =============================================================================
-- tpch_lineitem  (bronze_tpch.tpch_lineitem)
-- =============================================================================
-- Source-faithful ingestion of TPC-H lineitem table.
-- This is the primary fact source for fct_sales_lineitem downstream.
-- Grain: one row per (orderkey, linenumber). ~6M rows at SF1.
-- See ADR-05. Schema segmentation convention: ADR-11.
-- =============================================================================

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
