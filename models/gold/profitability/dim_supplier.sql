-- dim_supplier — SCD1, one row per supplier; nation + region on the row for
-- fewer joins in BI. Rationale for Type 1 vs customer Type 2: ADR-04.

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
