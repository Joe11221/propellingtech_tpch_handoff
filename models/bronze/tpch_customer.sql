-- =============================================================================
-- tpch_customer  (bronze_tpch.tpch_customer)
-- =============================================================================
-- Source-faithful ingestion of TPC-H customer table.
-- Column names preserved as-is (C_*). No business logic. No type coercion.
-- Three metadata columns appended via add_ingestion_metadata() macro.
-- See ADR-05. Schema segmentation convention: ADR-11.
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags         = ['bronze', 'customer']
    )
}}

select
    c_custkey,
    c_name,
    c_address,
    c_nationkey,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment,
    {{ add_ingestion_metadata() }}
from {{ source('tpch_sf1', 'customer') }}
