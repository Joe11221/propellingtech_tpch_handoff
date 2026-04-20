{#
    positive_value
    --------------
    Custom generic test.

    Asserts that a column contains only positive values. Nulls are ignored
    by default (pair with `not_null` if that matters). By default, zero
    fails — pass allow_zero=true to treat zero as acceptable.

    This is the dbt-native cousin of the column-level quarantine tests
    I built at Cencora during the legacy SQL Server → Databricks migration.
    The same principle — progressive tests, stricter downstream — applies.

    Usage:

        columns:
          - name: net_revenue
            tests:
              - positive_value
          - name: tax_amount
            tests:
              - positive_value:
                  allow_zero: true
#}

{% test positive_value(model, column_name, allow_zero=false) %}

    select *
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} {% if allow_zero %} < 0 {% else %} <= 0 {% endif %}

{% endtest %}
