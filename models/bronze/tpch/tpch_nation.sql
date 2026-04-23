-- tpch_nation → bronze_tpch
-- 25 nations; `n_regionkey` → region. Small ref table.

{{
    config(
        materialized = 'table',
        tags         = ['bronze', 'nation', 'reference']
    )
}}

select
    n_nationkey,
    n_name,
    n_regionkey,
    n_comment,
    {{ add_ingestion_metadata() }}
from {{ source('tpch_sf1', 'nation') }}
