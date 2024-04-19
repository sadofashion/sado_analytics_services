SELECT
    DISTINCT customer_id AS account_id,
    FIRST_VALUE(customer_descriptive_name) over w1 AS account_name,
    FIRST_VALUE(customer_currency_code) over w1 AS currency_code,
    FIRST_VALUE(customer_manager) over w1 AS is_manager_account
FROM
    {{ source(
        'googleads',
        'customer'
    ) }}
    where _LATEST_DATE = _DATA_DATE
    window w1 as ( PARTITION BY customer_id
        ORDER BY
            ORDER BY _DATA_DATE desc, _LATEST_DATE desc rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING)
