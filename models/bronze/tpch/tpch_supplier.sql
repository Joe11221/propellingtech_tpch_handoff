-- tpch_supplier → bronze_tpch
-- TPC-H supplier; links to `nation` for geography on the supply side.

{{
    config(
        materialized = 'table',
        tags         = ['bronze', 'supplier']
    )
}}

select
    s_suppkey,
    s_name,
    s_address,
    s_nationkey,
    s_phone,
    s_acctbal,
    s_comment,
    {{ add_ingestion_metadata() }}
from {{ source('tpch_sf1', 'supplier') }}
