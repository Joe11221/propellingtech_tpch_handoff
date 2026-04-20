-- =============================================================================
-- lineitem  (silver_sales.lineitem)
-- =============================================================================
-- Silver lineitem — the keystone Silver model for customer profitability.
--
-- Grain preserved from source: one row per (order_id, line_number). This
-- is the atomic commercial event and MUST NOT be aggregated here. See ADR-03.
--
-- Business rules applied at THIS layer (ADR-02 placement rubric):
--   - net_revenue = extended_price * (1 - discount)
--       Universal definition. Any downstream use case defines revenue-after-
--       discount the same way. Putting this in Silver prevents redefinition
--       risk across analytical products.
--   - discount_amount = extended_price * discount
--       Same reasoning.
--   - tax_amount = extended_price * tax
--       Same reasoning.
--
-- Business rules DEFERRED to Gold:
--   - gross_margin = net_revenue - supply_cost_total
--       Margin definition is use-case-specific. See ADR-02.
--   - customer tier derivation, segment rollups, etc.
--
-- Data quality enforced here:
--   - discount rate in [0, 1]
--   - tax rate in [0, 1]  (TPC-H tax is typically 0.00–0.08)
--   - quantity > 0
--   - extended_price > 0
-- Rows violating these are filtered out and surfaced via a test in the
-- schema YAML; in a production pipeline these would go to a quarantine
-- table (the Cencora pattern).
--
-- See ADR-11 for schema segmentation (this lives in silver_sales).
-- =============================================================================

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
        (extended_price * discount_rate)::number(12,2)
                                                    as discount_amount,

        -- Net revenue: what the customer actually paid, pre-tax.
        -- This is the canonical revenue number for all downstream
        -- analytics. Do not recompute it elsewhere.
        (extended_price * (1 - discount_rate))::number(12,2)
                                                    as net_revenue,

        -- Tax amount: informational; not added to net_revenue.
        (extended_price * tax_rate)::number(12,2)
                                                    as tax_amount

    from source

)

select * from enriched
