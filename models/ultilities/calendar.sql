WITH date_spine AS (
    {{ dbt_utils.date_spine(
        start_date = "cast('2023-06-01' as Date)",
        datepart = "day",
        end_date = "date_add( current_date() , interval 6 month)"
    ) }}
)


    SELECT
        DATE(date_day) AS date,
        extract(day from date_day) as day_of_month,
        format_date(
            '%A',
            date_day
        ) AS day_name,
        DATE_TRUNC(DATE(date_day), MONTH) AS start_of_month,
        LAST_DAY(
            date_day,
            MONTH
        ) AS end_of_month,
        DATE_TRUNC(DATE(date_day), isoweek) AS start_of_week,
        LAST_DAY(DATE(date_day), isoweek) AS end_of_week,
        format_date(
            '%B',
            date_day
        ) AS month_name,
        format_date(
            '%Y.%m',
            date_day
        ) AS year_month,
        EXTRACT(
            YEAR
            FROM
                date_day
        ) AS year,
--        {# b.milestone_name #}
    FROM
        date_spine d
--        {# left join budget_period b on date(d.date_day) = b.date #}
