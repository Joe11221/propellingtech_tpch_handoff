-- =============================================================================
-- dim_geography
-- =============================================================================
-- Conformed geography dimension. One row per TPC-H nation, denormalized with
-- its parent region. Used for BOTH customer-ship and supplier-ship geography
-- analytics — centralizing the nation+region rollup here prevents drift.
--
-- Grain: one row per nation_id.
-- SCD strategy: Type 1 (nations/regions don't churn in TPC-H; see ADR-04).
-- =============================================================================

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
