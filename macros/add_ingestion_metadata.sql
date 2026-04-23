{#
    add_ingestion_metadata — _ingested_at, _source_system, _batch_id (ADR-05).

    Use after the source select list, with a comma before the macro call:

        select
            *,
            {{ add_ingestion_metadata() }}
        from {{ source('tpch_sf1', 'customer') }}

    Macro output has no leading comma; caller supplies the comma after `*`.
#}

{% macro add_ingestion_metadata() -%}
    current_timestamp()                                                    as _ingested_at,
    '{{ var("source_system", "SNOWFLAKE_SAMPLE_DATA.TPCH_SF1") }}'         as _source_system,
    '{{ invocation_id }}'                                                  as _batch_id
{%- endmacro %}
