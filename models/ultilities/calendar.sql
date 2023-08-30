WITH date_spine AS (
    {{ dbt_utils.date_spine(
        start_date = "cast('2023-06-01' as Date)",
        datepart = "day",
        end_date = "date_add( current_date() , interval 6 month)"
    ) }}
)
    SELECT
        DATE(date_day) AS date,
        format_date(
            '%A',
            date_day
        ) AS dayName,
        DATE_TRUNC(DATE(date_day), MONTH) AS startOfMonth,
        LAST_DAY(
            date_day,
            MONTH
        ) AS endOfMonth,
        DATE_TRUNC(DATE(date_day), isoweek) AS startOfWeek,
        LAST_DAY(DATE(date_day), isoweek) AS endOfWeek,
        format_date(
            '%B',
            date_day
        ) AS monthName,
        format_date(
            '%Y.%m',
            date_day
        ) AS yearMonth,
        EXTRACT(
            YEAR
            FROM
                date_day
        ) AS year,
    FROM
        date_spine
