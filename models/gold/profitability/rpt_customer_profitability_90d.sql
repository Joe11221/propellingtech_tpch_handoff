-- =============================================================================
-- rpt_customer_profitability_90d
-- =============================================================================
-- Consumption-ready view. This is what Sarah (the VP of Commercial Finance)
-- actually queries in Power BI or Tableau. No further transformations
-- required between this view and a dashboard tile.
--
-- Grain: one row per (customer_id, CURRENT version). Shows the trailing-90-day
-- window ending at the most recent order_date in the fact — TPC-H is static
-- and ends in mid-1998, so anchoring to current_date would return an empty set.
-- In production this would be a rolling window anchored on current_date.
--
-- Materialization: view. Cheap, always fresh, no rebuild cost.
-- See ADR-09.
-- =============================================================================

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
        max(order_date)                as window_end,
        dateadd('day', -90, max(order_date)) as window_start
    from {{ ref('fct_sales_lineitem') }}

),

windowed_fact as (

    select f.*
    from {{ ref('fct_sales_lineitem') }} as f
    cross join window_anchor             as w
    where f.order_date between w.window_start and w.window_end

),

by_customer as (

    select
        f.customer_key,
        sum(f.quantity)                  as total_quantity,
        sum(f.extended_price)            as total_gross_revenue,
        sum(f.discount_amount)           as total_discount,
        sum(f.net_revenue)               as total_net_revenue,
        sum(f.supply_cost)               as total_supply_cost,
        sum(f.gross_margin)              as total_gross_margin,
        count(distinct f.order_id)       as order_count,
        count(*)                         as lineitem_count
    from windowed_fact as f
    group by f.customer_key

)

select
    dc.customer_id,
    dc.customer_name,
    dc.market_segment,
    dc.customer_tier,
    dc.nation_name,
    dc.region_name,

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
inner join {{ ref('dim_customer') }} as dc on bc.customer_key = dc.customer_key
order by bc.total_gross_margin desc
