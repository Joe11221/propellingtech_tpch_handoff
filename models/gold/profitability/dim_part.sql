-- dim_part — SCD1 part master; overwrites on refresh. SCD2 wasn’t needed for
-- this use case; see ADR-04.

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
