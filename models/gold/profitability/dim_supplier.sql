-- =============================================================================
-- dim_supplier
-- =============================================================================
-- SCD Type 1 supplier dimension. One row per supplier_id — overwrite on change.
--
-- Denormalized with nation + region for BI ergonomics. See ADR-04 for why
-- supplier is Type 1 and customer is Type 2.
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags         = ['gold', 'dimension', 'scd1', 'supplier']
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['s.supplier_id']) }}
                                            as supplier_key,

    s.supplier_id,
    s.supplier_name,
    s.supplier_address,
    s.supplier_phone,
    s.account_balance,

    s.nation_id,
    n.nation_name,
    r.region_id,
    r.region_name

from {{ ref('supplier') }}          as s
left join {{ ref('nation') }}       as n on s.nation_id = n.nation_id
left join {{ ref('region') }}       as r on n.region_id  = r.region_id
