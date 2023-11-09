WITH date_spine AS (
    





with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
    
    

    )

    select *
    from unioned
    where generated_number <= 342
    order by generated_number



),

all_periods as (

    select (
        

        datetime_add(
            cast( cast('2023-06-01' as Date) as datetime),
        interval row_number() over (order by 1) - 1 day
        )


    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= date_add( current_date() , interval 6 month)

)

select * from filtered


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
    FROM
        date_spine