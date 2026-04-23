{#
    margin_reconciliation (ADR-08) — sum(gross_margin) on fct_sales_lineitem
    should match the customer rpt rollup (within a small float tolerance).
    Same kind of check we used when reconciling migration row totals: if fact
    and report disagree, something’s wrong in the join or the measure.
#}

with fact_total as (

    select sum(gross_margin) as total_gross_margin
    from {{ ref('fct_sales_lineitem') }}

),

-- Rebuild the same window the rpt view uses so the reconciliation is
-- apples-to-apples. Anchored on max(order_date) in the fact, not current_date.
window_anchor as (

    select
        max(order_date)                      as window_end,
        dateadd('day', -90, max(order_date)) as window_start
    from {{ ref('fct_sales_lineitem') }}

),

fact_windowed_total as (

    select sum(f.gross_margin) as total_gross_margin
    from {{ ref('fct_sales_lineitem') }} as f
    cross join window_anchor             as w
    where f.order_date between w.window_start and w.window_end

),

rpt_total as (

    select sum(total_gross_margin) as total_gross_margin
    from {{ ref('rpt_customer_profitability_90d') }}

)

select
    fwt.total_gross_margin          as fact_windowed_total,
    rpt.total_gross_margin          as rpt_total,
    abs(fwt.total_gross_margin - rpt.total_gross_margin) as discrepancy
from fact_windowed_total as fwt
cross join rpt_total     as rpt
where abs(fwt.total_gross_margin - rpt.total_gross_margin) > 0.01
