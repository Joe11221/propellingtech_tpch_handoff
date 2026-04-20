{#
    snap_customer_scd2
    ------------------
    SCD Type 2 snapshot on customer. See ADR-04.

    Strategy choice — check_cols vs timestamp:
      TPC-H has no natural updated_at column on customer, so `timestamp`
      strategy isn't directly applicable against the source. We use
      `check` strategy and watch the columns that actually drive
      margin attribution:
        - c_mktsegment  (changes a customer's segment membership)
        - c_acctbal     (changes a customer's financial standing)
        - c_nationkey   (changes geographic attribution)
        - c_address     (address moves — less analytically material but
                         commonly SCD-tracked in practice)

    Honest caveat: TPC-H is a static snapshot, so this snapshot will
    only ever see one version of each row. The point of implementing
    it is to prove the pattern is in place and correctly wired — any
    real commercial source of customer master data will exhibit
    drift on these exact columns.

    Output columns added by dbt:
      dbt_scd_id       - surrogate key for this version
      dbt_updated_at   - system timestamp the version was observed
      dbt_valid_from   - version start timestamp
      dbt_valid_to     - version end timestamp (NULL for current)

    Downstream (dim_customer in Gold) is responsible for:
      - exposing is_current convenience flag
      - deriving use-case-specific attributes (customer_tier)
      - joining nation+region for the denormalized geography view
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
