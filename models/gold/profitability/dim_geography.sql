-- dim_geography — one row per nation, region name rolled up. Same dim for
-- customer and supplier geographies on the fact. SCD1 (ADR-04); static in practice for TPC-H.

{{
    config(
        materialized = 'table',
        tags         = ['gold', 'dimension', 'conformed']
    )
}}

select
    -- Surrogate key for BI-side joins. Deterministic on nation_id so it is
    -- stable across rebuilds — important for any downstream caching.
    {{ dbt_utils.generate_surrogate_key(['n.nation_id']) }}
                                            as geography_key,

    n.nation_id,
    n.nation_name,
    r.region_id,
    r.region_name

from {{ ref('nation') }}  as n
left join {{ ref('region') }} as r
       on n.region_id = r.region_id
