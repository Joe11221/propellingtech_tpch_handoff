{#
    SCD2 snapshot on `tpch_customer` (ADR-04). `check` on segment, balance,
    nation, address — TPC-H has no updated_at, so not `timestamp` strategy.
    Data is static; you still get one version per key today. dbt appends
    dbt_scd_*, dbt_valid_*. Gold `dim_customer` adds is_current, tier, geo.
#}

{% snapshot snap_customer_scd2 %}

    {{
        config(
            target_schema = 'silver_customer',
            unique_key    = 'c_custkey',
            strategy      = 'check',
            check_cols    = [
                'c_mktsegment',
                'c_acctbal',
                'c_nationkey',
                'c_address'
            ],
            invalidate_hard_deletes = true
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
        c_comment
    from {{ ref('tpch_customer') }}

{% endsnapshot %}
