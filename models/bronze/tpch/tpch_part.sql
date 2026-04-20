-- =============================================================================
-- tpch_part  (bronze_tpch.tpch_part)
-- =============================================================================
-- Source-faithful ingestion of TPC-H part table.
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags         = ['bronze', 'part']
    )
}}

select
    p_partkey,
    p_name,
    p_mfgr,
    p_brand,
    p_type,
    p_size,
    p_container,
    p_retailprice,
    p_comment,
    {{ add_ingestion_metadata() }}
from {{ source('tpch_sf1', 'part') }}
