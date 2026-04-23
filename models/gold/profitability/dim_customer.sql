-- dim_customer — SCD2: one row per (customer_id, version). `customer_key` is the
-- fact FK; join facts on order_date between valid_from/valid_to for correct segment.
-- `customer_tier` is Gold-only (quartiles on current balance) — ADR-02, ADR-04.
-- Nation/region repeated here and in dim_geography on purpose so this dim stands alone in reports.

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
