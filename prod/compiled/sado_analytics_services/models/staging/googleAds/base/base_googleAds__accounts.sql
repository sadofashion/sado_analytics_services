SELECT
    DISTINCT customer_id AS account_id,
    FIRST_VALUE(customer_descriptive_name) over w1 AS account_name,
    FIRST_VALUE(customer_currency_code) over w1 AS currency_code,
    FIRST_VALUE(customer_manager) over w1 AS is_manager_account
FROM
    
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`GoogleAds`.`ads_Customer_1322374205`
                LIMIT
                    1000
            )
        

        
    where _LATEST_DATE = _DATA_DATE
    window w1 as ( PARTITION BY customer_id
        ORDER BY
            _LATEST_DATE DESC rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING)