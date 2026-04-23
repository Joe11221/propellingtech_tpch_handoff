-- tpch_region → bronze_tpch
-- Five regions at SF1; top of the geography hierarchy under nation.

{{
    config(
        materialized = 'table',
        tags         = ['bronze', 'region', 'reference']
    )
}}

select
    r_regionkey,
    r_name,
    r_comment,
    {{ add_ingestion_metadata() }}
from {{ source('tpch_sf1', 'region') }}
