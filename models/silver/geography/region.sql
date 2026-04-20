-- =============================================================================
-- region  (silver_geography.region)
-- =============================================================================
-- Silver region reference. Clean names, enforced types.
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags         = ['silver', 'geography', 'region', 'reference']
    )
}}

select
    r_regionkey::bigint                             as region_id,
    trim(r_name)::varchar                           as region_name,
    trim(r_comment)::varchar                        as region_comment,

    _ingested_at,
    _source_system,
    _batch_id

from {{ ref('tpch_region') }}
