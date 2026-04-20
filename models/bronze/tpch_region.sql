-- =============================================================================
-- tpch_region  (bronze_tpch.tpch_region)
-- =============================================================================
-- Source-faithful ingestion of TPC-H region reference table.
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags         = ['bronze', 'region', 'reference']
    )
}}

select
    r_regionkey,
    r_name,
    r_comment,
    {{ add_ingestion_metadata() }}
from {{ source('tpch_sf1', 'region') }}
