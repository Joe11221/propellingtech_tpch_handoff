-- =============================================================================
-- partsupp  (silver_purchasing.partsupp)
-- =============================================================================
-- Silver partsupp entity. Clean names, enforced types.
--
-- This is the ONLY source of per-unit supply cost in TPC-H, so it is
-- essential to the margin calculation downstream:
--
--   gross_margin = net_revenue - (ps_supplycost * l_quantity)
--
-- The join in Gold is (part_id, supplier_id) from silver_sales.lineitem to
-- (part_id, supplier_id) here. See fct_sales_lineitem (Gold).
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags         = ['silver', 'purchasing', 'partsupp']
    )
}}

select
    ps_partkey::bigint                              as part_id,
    ps_suppkey::bigint                              as supplier_id,
    ps_availqty::int                                as available_quantity,
    ps_supplycost::number(12,2)                     as supply_cost_per_unit,
    trim(ps_comment)::varchar                       as partsupp_comment,

    _ingested_at,
    _source_system,
    _batch_id

from {{ ref('tpch_partsupp') }}

where ps_partkey    is not null
  and ps_suppkey    is not null
  and ps_supplycost >= 0   -- supply cost can be zero in edge cases but never negative
