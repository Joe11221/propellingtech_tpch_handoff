-- =============================================================================
-- customer  (silver_customer.customer)
-- =============================================================================
-- Silver customer entity. Reads from the SCD2 snapshot, NOT directly from
-- bronze. This is what makes point-in-time customer attribution possible
-- downstream (see dim_customer in Gold).
--
-- Transformations applied here:
--   1. Clean column names (C_* → business-readable)
--   2. Explicit type declarations
--   3. String hygiene (trim)
--   4. Surface an is_current convenience flag from snapshot metadata
--   5. Retain SCD2 valid_from / valid_to for point-in-time lookups
--
-- See ADR-02 for the placement rubric, ADR-04 for the SCD strategy,
-- ADR-11 for schema segmentation (this lives in silver_customer).
-- =============================================================================

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
    dbt_valid_from::timestamp                       as valid_from,
    dbt_valid_to::timestamp                         as valid_to,

    -- Convenience flag — true when this is the latest version of the row.
    -- Always filter on this for "current state" analytics; leave unfiltered
    -- for "as-of-date" analytics.
    case when dbt_valid_to is null then true else false end
                                                    as is_current

from {{ ref('snap_customer_scd2') }}
