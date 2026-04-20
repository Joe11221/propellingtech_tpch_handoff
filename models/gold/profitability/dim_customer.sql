-- =============================================================================
-- dim_customer
-- =============================================================================
-- SCD Type 2 customer dimension. One row per (customer_id, SCD version).
--
-- Keys:
--   customer_key    — surrogate, unique per VERSION. Use this as the FK from
--                     fct_sales_lineitem to get point-in-time correct
--                     attribution.
--   customer_id     — natural key, stable across versions.
--
-- SCD semantics:
--   Join fact.order_date between valid_from and coalesce(valid_to, '9999-12-31')
--   to resolve the customer version that was active when the order happened.
--   See ADR-04 for the reasoning.
--
-- Gold-specific derivations (not in Silver — see ADR-02):
--   customer_tier   — account_balance quartile bucketed to Strategic / Growth /
--                     Maintain / Watch. Computed over the CURRENT version only
--                     so tier assignments don't oscillate as history is observed.
--
-- Denormalization:
--   Nation + region pulled in for BI convenience. The same nation/region
--   facts live in dim_geography — duplication here is intentional: dim_customer
--   should be independently queryable without a geography join.
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags         = ['gold', 'dimension', 'scd2', 'customer']
    )
}}

with customer_versions as (

    select
        c.customer_id,
        c.customer_name,
        c.customer_address,
        c.customer_phone,
        c.account_balance,
        c.market_segment,
        c.customer_comment,
        c.nation_id,
        c.valid_from,
        c.valid_to,
        c.is_current,
        c.scd_version_id
    from {{ ref('customer') }} as c

),

with_geography as (

    select
        cv.*,
        n.nation_name,
        n.region_id,
        r.region_name
    from customer_versions              as cv
    left join {{ ref('nation') }}       as n on cv.nation_id = n.nation_id
    left join {{ ref('region') }}       as r on n.region_id  = r.region_id

),

-- Tier is computed ONCE, against the current version of each customer.
-- Historical versions inherit the CURRENT tier — tier is a strategic lens
-- on the customer, not a point-in-time attribute of the relationship.
current_tier as (

    select
        customer_id,
        ntile(4) over (order by account_balance desc) as balance_quartile
    from with_geography
    where is_current = true

),

tier_lookup as (

    select
        customer_id,
        case balance_quartile
            when 1 then 'Strategic'
            when 2 then 'Growth'
            when 3 then 'Maintain'
            when 4 then 'Watch'
        end as customer_tier
    from current_tier

)

select
    -- Surrogate per version — deterministic so rebuilds don't churn FK values.
    {{ dbt_utils.generate_surrogate_key(['wg.scd_version_id']) }}
                                                        as customer_key,

    -- Natural key
    wg.customer_id,

    -- Descriptive attributes
    wg.customer_name,
    wg.customer_address,
    wg.customer_phone,
    wg.customer_comment,

    -- SCD2-tracked attributes
    wg.account_balance,
    wg.market_segment,
    wg.nation_id,
    wg.nation_name,
    wg.region_id,
    wg.region_name,

    -- Gold-derived lens
    tl.customer_tier,

    -- SCD bookkeeping
    wg.valid_from,
    coalesce(wg.valid_to, cast('9999-12-31' as timestamp))  as valid_to,
    wg.is_current

from with_geography           as wg
left join tier_lookup         as tl on wg.customer_id = tl.customer_id
