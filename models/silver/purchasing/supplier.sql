-- silver_purchasing.supplier — conformed supplier row; carries nation_id only.
-- Shared nation→region logic lives once in Gold dim_geography. ADR-11.

{{
    config(
        materialized = 'table',
        tags         = ['silver', 'purchasing', 'supplier']
    )
}}

select
    s_suppkey::bigint                               as supplier_id,
    trim(s_name)::varchar                           as supplier_name,
    trim(s_address)::varchar                        as supplier_address,
    s_nationkey::bigint                             as nation_id,
    trim(s_phone)::varchar                          as supplier_phone,
    s_acctbal::number(12,2)                         as account_balance,
    trim(s_comment)::varchar                        as supplier_comment,

    _ingested_at,
    _source_system,
    _batch_id

from {{ ref('tpch_supplier') }}

where s_suppkey is not null
