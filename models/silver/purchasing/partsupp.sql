-- silver_purchasing.partsupp — part–supplier bridge; ps_supplycost is the only
-- TPC-H unit cost (margin joins on part_id + supplier_id in Gold). ADR-06, ADR-11.

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
