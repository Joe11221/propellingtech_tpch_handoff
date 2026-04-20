-- =============================================================================
-- orders  (silver_sales.orders)
-- =============================================================================
-- Silver orders entity. Clean names, enforced types, and a small set of
-- derived date attributes (year, month) that are universally useful
-- downstream without committing to a use-case-specific aggregation.
--
-- Note: o_orderstatus is left as the source single-character code.
-- Expansion to human-readable ('Finished' / 'Open' / 'Partial') is
-- a Gold concern — reporting-friendly labels are a consumption question,
-- not a conformance question.
--
-- See ADR-11 for schema segmentation (this lives in silver_sales alongside
-- lineitem — the two entities the Sales/Commercial domain owns).
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags         = ['silver', 'sales', 'orders']
    )
}}

select
    -- Natural key
    o_orderkey::bigint                              as order_id,

    -- Foreign key to customer
    o_custkey::bigint                               as customer_id,

    -- Attributes
    o_orderstatus::varchar                          as order_status_code,
    o_totalprice::number(12,2)                      as order_total_price,
    o_orderdate::date                               as order_date,
    trim(o_orderpriority)::varchar                  as order_priority,
    trim(o_clerk)::varchar                          as clerk_id,
    o_shippriority::int                             as ship_priority,
    trim(o_comment)::varchar                        as order_comment,

    -- Derived date parts — universal, reusable, cheap.
    -- These are Silver-appropriate because any downstream use case
    -- would derive them identically.
    extract(year  from o_orderdate)::int            as order_year,
    extract(month from o_orderdate)::int            as order_month,
    extract(quarter from o_orderdate)::int          as order_quarter,

    -- Provenance (preserved from Bronze)
    _ingested_at,
    _source_system,
    _batch_id

from {{ ref('tpch_orders') }}

-- Sanity filters — source data hygiene at the Silver boundary.
-- Any row failing these is a source defect we refuse to propagate.
where o_orderkey is not null
  and o_custkey  is not null
  and o_orderdate is not null
