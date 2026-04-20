-- =============================================================================
-- nation  (silver_geography.nation)
-- =============================================================================
-- Silver nation reference. Clean names, enforced types.
-- Joined with region in Gold (dim_geography).
--
-- See ADR-11 — nation / region are the Geography/reference domain.
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags         = ['silver', 'geography', 'nation', 'reference']
    )
}}

select
    n_nationkey::bigint                             as nation_id,
    trim(n_name)::varchar                           as nation_name,
    n_regionkey::bigint                             as region_id,
    trim(n_comment)::varchar                        as nation_comment,

    _ingested_at,
    _source_system,
    _batch_id

from {{ ref('tpch_nation') }}
