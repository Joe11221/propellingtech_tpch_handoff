-- =============================================================================
-- rpt_segment_margin_concentration
-- =============================================================================
-- Consumption-ready view. Answers Sarah's "where does margin actually
-- concentrate?" question — Pareto lens on market segment.
--
-- Grain: one row per market_segment.
-- Columns include share-of-total for revenue and margin, and a simple margin
-- concentration index (segment's margin share ÷ segment's revenue share).
-- A value >1 means the segment punches above its weight on margin; <1 means
-- the segment is a revenue-heavy / margin-light block of the business.
-- =============================================================================

{{
    config(
        materialized = 'view',
        tags         = ['gold', 'rpt', 'segment_concentration']
    )
}}

with fact_joined as (

    select
        dc.market_segment,
        f.net_revenue,
        f.gross_margin,
        f.discount_amount,
        f.extended_price
    from {{ ref('fct_sales_lineitem') }} as f
    inner join {{ ref('dim_customer') }} as dc
            on f.customer_key = dc.customer_key

),

by_segment as (

    select
        market_segment,
        sum(extended_price)             as total_gross_revenue,
        sum(discount_amount)             as total_discount,
        sum(net_revenue)                 as total_net_revenue,
        sum(gross_margin)                as total_gross_margin
    from fact_joined
    group by market_segment

),

totals as (

    select
        sum(total_net_revenue)           as grand_net_revenue,
        sum(total_gross_margin)          as grand_gross_margin
    from by_segment

)

select
    bs.market_segment,

    bs.total_gross_revenue,
    bs.total_discount,
    bs.total_net_revenue,
    bs.total_gross_margin,

    case
        when bs.total_net_revenue > 0
        then (bs.total_gross_margin / bs.total_net_revenue)::number(7,6)
    end                                  as margin_rate,

    (bs.total_net_revenue  / t.grand_net_revenue)::number(7,6)
                                         as revenue_share,
    (bs.total_gross_margin / t.grand_gross_margin)::number(7,6)
                                         as margin_share,

    -- Concentration index: >1 = segment overindexes on margin vs. revenue.
    case
        when (bs.total_net_revenue / t.grand_net_revenue) > 0
        then ((bs.total_gross_margin / t.grand_gross_margin) /
              (bs.total_net_revenue  / t.grand_net_revenue))::number(7,4)
    end                                  as margin_concentration_index

from by_segment          as bs
cross join totals        as t
order by bs.total_gross_margin desc
