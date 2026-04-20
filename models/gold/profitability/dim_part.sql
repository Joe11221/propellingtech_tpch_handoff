-- =============================================================================
-- dim_part
-- =============================================================================
-- SCD Type 1 part dimension. Overwrite on change — part master is less
-- analytically material than customer for the profitability use case
-- (see ADR-04).
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags         = ['gold', 'dimension', 'scd1', 'part']
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['p.part_id']) }}
                                            as part_key,

    p.part_id,
    p.part_name,
    p.manufacturer,
    p.brand,
    p.part_type,
    p.part_size,
    p.container,
    p.retail_price

from {{ ref('part') }} as p
