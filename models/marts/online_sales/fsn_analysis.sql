{{ config(
    materialized = 'incremental',
    partition_by ={ 'field': 'date',
    'data_type': 'date',
    'granularity': 'day' },
    incremental_strategy = 'insert_overwrite',
    unique_key = 'concat(class_code,date)',
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'daily', 'nhanhvn'],
) }}

WITH inventory AS (

    SELECT
        products.class_code,
        balance.date,
        SUM(
            balance.daily_beginning_remain
        ) daily_beginning_remain,
        SUM(
            balance.daily_ending_remain
        ) daily_ending_remain,
        SUM(
            balance.quantity_in
        ) quantity_in,
        SUM(
            balance.quantity_out_issue
        ) quantity_out,
        SUM(
            balance.quantity_out_adjust
        ) adjustment,
    FROM
        {{ ref('online_inventory_balance') }}
        balance
        LEFT JOIN dbt_dev.stg_nhanhvn__products products
        ON balance.product_id = products.product_id
    WHERE
        products.class_code IS NOT NULL
        AND balance.daily_ending_remain IS NOT NULL
    group by 1,2
),
aggregated_data AS (
    SELECT
        *,
        FIRST_VALUE(daily_beginning_remain) over w1 AS openning_balance,
        SUM(daily_ending_remain) over w1 AS inventory_holding_days,
        SUM(quantity_in) over w1 AS receive_qty,
        SUM(quantity_out) over w1 AS issue_qty,
        ROUND(
            safe_divide(SUM(daily_ending_remain) over w1,(SUM(quantity_in) over w1 + COALESCE(FIRST_VALUE(daily_beginning_remain) over w1, 0))),
            2) AS avg_stay,
            ROUND(safe_divide(SUM(quantity_out) over w1, 7), 2) AS consumption_rate
            FROM
                inventory window w1 AS (
                    PARTITION BY class_code
                    ORDER BY
                        unix_date(DATE) RANGE BETWEEN 7 preceding
                        AND CURRENT ROW
                )
        ),
        classification_data AS (
            SELECT
                *,
                SUM(avg_stay) over w1 AS cummulative_avg_stay,
                SUM(consumption_rate) over w3 AS cummulative_consumption_rate,
            FROM
                aggregated_data d
            WHERE
                DATE >= '2023-09-25' window w1 AS (
                    PARTITION BY DATE
                    ORDER BY
                        avg_stay desc RANGE BETWEEN unbounded preceding
                        AND CURRENT ROW
                ),
                w2 AS (
                    PARTITION BY DATE
                ),
                w3 AS (
                    PARTITION BY DATE
                    ORDER BY
                        consumption_rate DESC RANGE BETWEEN unbounded preceding
                        AND CURRENT ROW
                )
            ORDER BY
                DATE,
                consumption_rate DESC
        ),
        RESULT AS (
            SELECT
                *
            EXCEPT(
                    cummulative_avg_stay,
                    cummulative_consumption_rate
                ),
                CASE
                    WHEN avg_stay IS NOT NULL THEN NTILE(10) over (
                        PARTITION BY DATE
                        ORDER BY
                            cummulative_avg_stay desc
                    )
                    ELSE 10
                END AS avg_stay_score,
                CASE
                    WHEN consumption_rate IS NOT NULL THEN NTILE(10) over (
                        PARTITION BY DATE
                        ORDER BY
                            cummulative_consumption_rate
                    )
                    ELSE 10
                END AS consumption_rate_score,
            FROM
                classification_data
                where (avg_stay is not null or consumption_rate <> 0)
        )
    SELECT
        *
    EXCEPT
        (
            avg_stay_score,
            consumption_rate_score
        ),
        CASE
            WHEN avg_stay_score IN (1) THEN 'F'
            WHEN avg_stay_score IN (
                2,
                3
            ) THEN 'S'
            ELSE 'N'
        END AS inventory_stay_classification,
        CASE
            WHEN consumption_rate_score IN (1) THEN 'F'
            WHEN consumption_rate_score IN (
                2,
                3
            ) THEN 'S'
            ELSE 'N'
        END AS consumption_classification,
        CASE NTILE(3) over (PARTITION BY DATE
    ORDER BY
        (avg_stay_score + consumption_rate_score) DESC)
        WHEN 3 THEN 'F'
        WHEN 2 THEN 'S'
        ELSE 'N'END AS fsn_classification
    FROM
        RESULT
