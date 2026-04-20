-- =============================================================================
-- tpch_partsupp  (bronze_tpch.tpch_partsupp)
-- =============================================================================
-- Source-faithful ingestion of TPC-H partsupp table (part-supplier availability).
-- Carries ps_supplycost — the only source of per-unit supply cost in TPC-H,
-- required for margin calculation in Gold. See ADR-06.
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags         = ['bronze', 'partsupp']
    )
}}

select
    ps_partkey,
    ps_suppkey,
    ps_availqty,
    ps_supplycost,
    ps_comment,
    {{ add_ingestion_metadata() }}
from {{ source('tpch_sf1', 'partsupp') }}
