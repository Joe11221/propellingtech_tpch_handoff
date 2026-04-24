-- silver_customer.customer — SCD2 from snap (not raw Bronze); valid_from/to
-- and is_current for as-of joins into Gold dim_customer. ADR-02, ADR-04, ADR-11.

{{
    config(
        materialized = 'table',
        tags         = ['silver', 'customer']
    )
}}
select
    -- Natural key (stable across versions)
    c_custkey::bigint                               as customer_id,
    -- Attributes
    trim(c_name)::varchar                           as customer_name,
    trim(c_address)::varchar                        as customer_address,
    c_nationkey::bigint                             as nation_id,
    trim(c_phone)::varchar                          as customer_phone,
    c_acctbal::number(12,2)                         as account_balance,
    trim(c_mktsegment)::varchar                     as market_segment,
    trim(c_comment)::varchar                        as customer_comment,
    -- SCD Type 2 metadata (from dbt snapshot)
    dbt_scd_id::varchar                             as scd_version_id,
    -- Backdate the FIRST observed version of each customer to "beginning of
    -- time" so historical facts (orders predating our first snapshot run)
    -- resolve against it. Subsequent versions keep the real dbt_valid_from
    -- from the moment the change was detected. This is the standard Kimball
    -- open-ended-initial-load pattern for SCD2 backfills.
    case
        when dbt_valid_from = min(dbt_valid_from) over (partition by c_custkey)
            then cast('1900-01-01' as timestamp)
        else dbt_valid_from
    end::timestamp                                  as valid_from,
    dbt_valid_to::timestamp                         as valid_to,
    case when dbt_valid_to is null then true else false end
                                                    as is_current
from {{ ref('snap_customer_scd2') }}
