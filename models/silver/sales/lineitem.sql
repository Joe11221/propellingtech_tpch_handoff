-- silver_sales.lineitem — (order_id, line_number) grain (ADR-03). `net_revenue`
-- and discount/tax $ here; margin uses supply cost in Gold (ADR-02). Ranges
-- and $ tests in _silver__models.yml.

{{
    config(
        materialized = 'table',
        tags         = ['silver', 'sales', 'lineitem', 'tier_1']
    )
}}

with source as (

    select
        l_orderkey::bigint                          as order_id,
        l_linenumber::int                           as line_number,
        l_partkey::bigint                           as part_id,
        l_suppkey::bigint                           as supplier_id,
        l_quantity::number(10,2)                    as quantity,
        l_extendedprice::number(12,2)               as extended_price,
        l_discount::number(5,4)                     as discount_rate,
        l_tax::number(5,4)                          as tax_rate,
        trim(l_returnflag)::varchar                 as return_flag,
        trim(l_linestatus)::varchar                 as line_status,
        l_shipdate::date                            as ship_date,
        l_commitdate::date                          as commit_date,
        l_receiptdate::date                         as receipt_date,
        trim(l_shipinstruct)::varchar               as ship_instruction,
        trim(l_shipmode)::varchar                   as ship_mode,
        trim(l_comment)::varchar                    as line_comment,

        _ingested_at,
        _source_system,
        _batch_id

    from {{ ref('tpch_lineitem') }}

    -- Source-boundary hygiene.
    -- These are hard rules; violations cannot propagate into analytics.
    where l_orderkey    is not null
      and l_linenumber  is not null
      and l_quantity        > 0
      and l_extendedprice   > 0
      and l_discount        between 0 and 1
      and l_tax             between 0 and 1

),

enriched as (

    select
        *,

        -- --------------------------------------------------------------
        -- Silver-computed measures (ADR-02: universal business rules)
        -- --------------------------------------------------------------

        -- Discount amount: what the customer saved vs. list.
        (extended_price * discount_rate)::number(12,4)
                                                    as discount_amount,

        -- Net revenue: what the customer actually paid, pre-tax.
        -- This is the canonical revenue number for all downstream
        -- analytics. Do not recompute it elsewhere.
        (extended_price * (1 - discount_rate))::number(12,4)
                                                    as net_revenue,

        -- Tax amount: informational; not added to net_revenue.
        (extended_price * tax_rate)::number(12,4)
                                                    as tax_amount

    from source

)

select
    * exclude (discount_amount, net_revenue, tax_amount),
    round(discount_amount, 2)::number(12, 2) as discount_amount,
    round(net_revenue, 2)::number(12, 2)     as net_revenue,
    round(tax_amount, 2)::number(12, 2)     as tax_amount
from enriched
