-- tpch_customer → bronze_tpch
-- TPC-H `customer` with original column names. Extra columns: _ingested_at, _source_system, _batch_id
-- (add_ingestion_metadata). ADR-05; schema layout ADR-11.

{{
    config(
        materialized = 'table',
        tags         = ['bronze', 'customer']
    )
}}

select
    c_custkey,
    c_name,
    c_address,
    c_nationkey,
    c_phone,
    c_acctbal,
    c_mktsegment,
    c_comment,
    {{ add_ingestion_metadata() }}
from {{ source('tpch_sf1', 'customer') }}
