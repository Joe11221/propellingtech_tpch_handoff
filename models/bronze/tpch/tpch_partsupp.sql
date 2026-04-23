-- tpch_partsupp → bronze_tpch
-- Part–supplier bridge; `ps_supplycost` is the unit cost TPC-H exposes (needed for margin; ADR-06).

{{
    config(
        materialized = 'table',
        tags         = ['bronze', 'partsupp']
    )
}}

select
    ps_partkey,
    ps_suppkey,
    ps_availqty,
    ps_supplycost,
    ps_comment,
    {{ add_ingestion_metadata() }}
from {{ source('tpch_sf1', 'partsupp') }}
