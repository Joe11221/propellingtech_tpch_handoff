-- dim_date — date spine 1990–2000 (dbt_utils) so the window isn’t a hand-maintained seed.
-- Covers TPC-H order dates with margin; one row per day; `date_key` = YYYYMMDD int for fact FKs.

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
