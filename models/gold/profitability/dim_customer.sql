-- dim_customer — SCD2: one row per (customer_id, version). `customer_key` is the
-- fact FK; join facts on order_date between valid_from/valid_to for correct segment.
-- `customer_tier` is Gold-only — ADR-02, ADR-04. Tiering rule:
--   - account_balance < 0 → 'Watch' (at-risk / delinquency signal)
--   - positive balances → tercile(desc) into 'Strategic' / 'Growth' / 'Maintain'
--   - non-current-only customers (hard-deleted via invalidate_hard_deletes)
--     → 'Lapsed' via coalesce in the final projection.
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
--
-- Negative balances get their own bucket ('Watch') rather than falling into
-- the bottom slice of a linear rank — a credit owed to a customer is an
-- at-risk signal that deserves an explicit label, not an accidental one.
-- Positive balances are tercile-ranked (desc) into Strategic / Growth /
-- Maintain. Customers with no current version at all land as 'Lapsed'
-- via coalesce in the final select (handles invalidate_hard_deletes=true).
current_customers as (

    select
        customer_id,
        account_balance
    from with_geography
    where is_current = true

),

active_tercile as (

    -- Tercile only across customers with a positive-or-zero balance; negatives
    -- are excluded here and classified as 'Watch' in tier_lookup below.
    select
        customer_id,
        ntile(3) over (order by account_balance desc) as balance_tercile
    from current_customers
    where account_balance >= 0

),

tier_lookup as (

    select
        cc.customer_id,
        case
            when cc.account_balance < 0   then 'Watch'
            when atr.balance_tercile = 1  then 'Strategic'
            when atr.balance_tercile = 2  then 'Growth'
            when atr.balance_tercile = 3  then 'Maintain'
        end                                              as customer_tier
    from current_customers       as cc
    left join active_tercile     as atr on cc.customer_id = atr.customer_id

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

    -- Gold-derived lens. Non-current-only customers (hard-deleted via
    -- invalidate_hard_deletes=true) miss tier_lookup and fall back to 'Lapsed'.
    coalesce(tl.customer_tier, 'Lapsed')              as customer_tier,

    -- SCD bookkeeping
    wg.valid_from,
    coalesce(wg.valid_to, cast('9999-12-31' as timestamp))  as valid_to,
    wg.is_current

from with_geography           as wg
left join tier_lookup         as tl on wg.customer_id = tl.customer_id
