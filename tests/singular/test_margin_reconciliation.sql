{#
    test_margin_reconciliation
    --------------------------
    Singular test (ADR-08).

    Asserts that total gross_margin at the lineitem (fact) grain reconciles
    with the sum of per-customer gross_margin in the consumption view,
    within floating-point tolerance.

    This is the dbt analog of the reconciliation tests I wrote into the
    Cencora migration-validation framework: if the atomic grain and the
    rollup disagree, one of them is wrong. The test fails when they
    disagree by more than a cent across ~6M lineitem rows — which is the
    right sensitivity: any real double-counting or drop would show up as
    dollars, not cents.

    Failing this test is a tier-1 incident: it means either the join in
    the rpt view is lossy, a row is being double-counted, or the measure
    is being redefined between layers. In production it would page.
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
