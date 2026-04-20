-- =============================================================================
-- tpch_nation  (bronze_tpch.tpch_nation)
-- =============================================================================
-- Source-faithful ingestion of TPC-H nation reference table.
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags         = ['bronze', 'nation', 'reference']
    )
}}

select
    n_nationkey,
    n_name,
    n_regionkey,
    n_comment,
    {{ add_ingestion_metadata() }}
from {{ source('tpch_sf1', 'nation') }}
