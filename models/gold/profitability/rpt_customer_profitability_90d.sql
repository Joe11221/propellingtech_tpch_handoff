-- rpt_customer_profitability_90d — view: customer-level KPIs for the last 90
-- order days in the fact (anchor = max order_date; static TPC-H, not wall-clock).
--
-- Grain is one row per customer_id (natural key) — NOT per SCD2 version. This
-- is deliberate: Sarah's dashboard is "how is customer X doing right now",
-- which is a customer-grain question. SCD2 point-in-time attribution is still
-- available at the fact grain for any deeper analysis.
--
-- Descriptive attributes (name, segment, tier, geography) come from the
-- LATEST SCD2 version of each customer — current if they still exist,
-- otherwise the most recent historical version. This means customers who
-- were hard-deleted (invalidate_hard_deletes=true) after transacting in
-- the window still appear, flagged `is_lapsed = true` and carrying
-- `customer_tier = 'Lapsed'` (assigned upstream in dim_customer). Sarah
-- can see "recent activity from customers we've since lost" without
-- duplicating the active-customer grain.
-- ADR-02 / ADR-04 on why tier + segment current-ness is a Gold concern;
-- ADR-09 on the view materialization choice.

{{
    config(
        materialized = 'view',
        tags         = ['gold', 'rpt', 'customer_profitability']
    )
}}

with window_anchor as (

    -- Anchor the "last 90 days" window on the fact's latest order_date.
    -- Swap this for `current_date` when running against a live source.
    select
        max(order_date)                      as window_end,
        dateadd('day', -90, max(order_date)) as window_start
    from {{ ref('fct_sales_lineitem') }}

),

windowed_fact as (

    select f.*
    from {{ ref('fct_sales_lineitem') }} as f
    cross join window_anchor             as w
    where f.order_date between w.window_start and w.window_end

),

-- Resolve the version-level customer_key on the fact to the natural
-- customer_id so we can aggregate across SCD2 versions inside the window.
fact_with_customer_id as (

    select
        f.*,
        dc.customer_id
    from windowed_fact                   as f
    inner join {{ ref('dim_customer') }} as dc
            on f.customer_key = dc.customer_key

),

by_customer as (

    select
        customer_id,
        sum(quantity)                    as total_quantity,
        sum(extended_price)              as total_gross_revenue,
        sum(discount_amount)             as total_discount,
        sum(net_revenue)                 as total_net_revenue,
        sum(supply_cost)                 as total_supply_cost,
        sum(gross_margin)                as total_gross_margin,
        count(distinct order_id)         as order_count,
        count(*)                         as lineitem_count
    from fact_with_customer_id
    group by customer_id

),

-- Descriptive attributes come from the LATEST SCD2 version per customer_id.
-- For active customers that's the current version (is_current = true). For
-- hard-deleted customers that's the most recent historical version — whose
-- customer_tier is already 'Lapsed' from the dim_customer coalesce rule.
latest_customer as (

    select
        customer_id,
        customer_name,
        market_segment,
        customer_tier,
        nation_name,
        region_name,
        is_current
    from {{ ref('dim_customer') }}
    qualify row_number() over (
        partition by customer_id
        order by valid_from desc
    ) = 1

)

select
    lc.customer_id,
    lc.customer_name,
    lc.market_segment,
    lc.customer_tier,
    lc.nation_name,
    lc.region_name,

    -- Explicit BI-facing flag so dashboards can badge/filter lapsed rows
    -- without needing to reason about 'Lapsed' string matching on tier.
    not lc.is_current                    as is_lapsed,

    bc.order_count,
    bc.lineitem_count,
    bc.total_quantity,

    bc.total_gross_revenue,
    bc.total_discount,
    bc.total_net_revenue,
    bc.total_supply_cost,
    bc.total_gross_margin,

    case
        when bc.total_net_revenue > 0
        then (bc.total_gross_margin / bc.total_net_revenue)::number(7,6)
    end                                  as margin_rate,

    case
        when bc.total_gross_revenue > 0
        then (bc.total_discount / bc.total_gross_revenue)::number(7,6)
    end                                  as discount_rate_effective

from by_customer                as bc
inner join latest_customer      as lc on bc.customer_id = lc.customer_id
order by bc.total_gross_margin desc
