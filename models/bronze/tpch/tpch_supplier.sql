-- =============================================================================
-- tpch_supplier  (bronze_tpch.tpch_supplier)
-- =============================================================================
-- Source-faithful ingestion of TPC-H supplier table.
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags         = ['bronze', 'supplier']
    )
}}

select
    s_suppkey,
    s_name,
    s_address,
    s_nationkey,
    s_phone,
    s_acctbal,
    s_comment,
    {{ add_ingestion_metadata() }}
from {{ source('tpch_sf1', 'supplier') }}
