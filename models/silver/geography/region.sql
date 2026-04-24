-- silver_geography.region — top of geo hierarchy under nation (SF1: five regions). ADR-11.

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
