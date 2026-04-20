{#
    add_ingestion_metadata
    ----------------------
    Injects three metadata columns into every Bronze model so that
    every row is traceable back to its load time, source system,
    and dbt invocation.

    See ADR-05 for the reasoning.

    Usage (in a Bronze model):

        select
            *,
            {{ add_ingestion_metadata() }}
        from {{ source('tpch_sf1', 'customer') }}

    The trailing comma convention means this macro is always used
    AFTER the source columns — so the macro emits columns WITHOUT
    a leading comma (the caller provides it).
#}

{% macro add_ingestion_metadata() -%}
    current_timestamp()                                                    as _ingested_at,
    '{{ var("source_system", "SNOWFLAKE_SAMPLE_DATA.TPCH_SF1") }}'         as _source_system,
    '{{ invocation_id }}'                                                  as _batch_id
{%- endmacro %}
