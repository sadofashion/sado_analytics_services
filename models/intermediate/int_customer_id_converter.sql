SELECT
        kiotviet_customer_id,
        nhanhvn_customer_id
    FROM
        {{ ref("fct__customers") }}
    WHERE
        kiotviet_customer_id IS NOT NULL
        AND nhanhvn_customer_id IS NOT NULL