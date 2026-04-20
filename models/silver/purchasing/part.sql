-- =============================================================================
-- part  (silver_purchasing.part)
-- =============================================================================
-- Silver part entity. Clean names, enforced types, standardized string casing
-- on descriptive fields (brand, type) so downstream groupings are consistent.
--
-- See ADR-11 — part / supplier / partsupp together form the Purchasing domain.
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags         = ['silver', 'purchasing', 'part']
    )
}}

select
    p_partkey::bigint                               as part_id,
    trim(p_name)::varchar                           as part_name,
    trim(p_mfgr)::varchar                           as manufacturer,
    -- Standardize brand to upper — TPC-H already delivers this consistently,
    -- but the explicit cast is a contract: downstream aggregations will NOT
    -- fracture because of casing drift from the source.
    upper(trim(p_brand))::varchar                   as brand,
    trim(p_type)::varchar                           as part_type,
    p_size::int                                     as part_size,
    trim(p_container)::varchar                      as container,
    p_retailprice::number(12,2)                     as retail_price,
    trim(p_comment)::varchar                        as part_comment,

    _ingested_at,
    _source_system,
    _batch_id

from {{ ref('tpch_part') }}

where p_partkey is not null
