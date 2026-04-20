-- =============================================================================
-- dim_date
-- =============================================================================
-- Conformed date dimension. Generated via dbt_utils.date_spine rather than a
-- CSV seed so the range is driven by code and reproducible.
--
-- Range chosen to bracket TPC-H's order_date domain (1992-01-02 → 1998-08-02)
-- with a generous buffer either side. Grain: one row per calendar day.
--
-- date_key is an integer YYYYMMDD — the canonical integer surrogate that
-- keeps the fact table's date joins fast and readable in BI tools.
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags         = ['gold', 'dimension', 'conformed']
    )
}}

with spine as (

    {{ dbt_utils.date_spine(
        datepart    = "day",
        start_date  = "cast('1990-01-01' as date)",
        end_date    = "cast('2001-01-01' as date)"
    ) }}

),

calendar as (

    select
        cast(date_day as date)                                      as date_value,
        cast(to_char(date_day, 'YYYYMMDD') as int)                  as date_key,

        extract(year     from date_day)::int                        as year_number,
        extract(quarter  from date_day)::int                        as quarter_number,
        extract(month    from date_day)::int                        as month_number,
        to_char(date_day, 'Mon')                                    as month_short_name,
        to_char(date_day, 'Month')                                  as month_long_name,
        extract(day      from date_day)::int                        as day_of_month,
        extract(dayofweek from date_day)::int                       as day_of_week,
        to_char(date_day, 'Dy')                                     as day_short_name,
        to_char(date_day, 'Day')                                    as day_long_name,
        extract(week     from date_day)::int                        as week_of_year,

        case when extract(dayofweek from date_day) in (0, 6)
             then false else true
        end                                                          as is_weekday,

        cast(to_char(date_day, 'YYYYMM') as int)                    as year_month_key,
        cast(to_char(date_day, 'YYYY') || extract(quarter from date_day) as int)
                                                                     as year_quarter_key

    from spine

)

select * from calendar
