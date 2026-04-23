{#
    positive_value — generic test: values must be > 0 (or >= 0 with allow_zero).
    Nulls are skipped; add `not_null` if nulls are invalid. Same idea as the
    column checks we used in a big SQL Server → Databricks cutover (tighter
    tests further down the DAG).

    columns:
      - name: net_revenue
        tests: [ positive_value ]
      - name: tax_amount
        tests: [ { positive_value: { allow_zero: true } } ]
#}

{% test positive_value(model, column_name, allow_zero=false) %}

    select *
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} {% if allow_zero %} < 0 {% else %} <= 0 {% endif %}

{% endtest %}
