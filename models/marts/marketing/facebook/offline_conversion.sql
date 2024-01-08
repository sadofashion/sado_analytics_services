{{ config(
    materialized = 'view',
    tags = ['fb','integration','view']
) }}

SELECT
    invoices.transaction_code,
    invoices.created_date,
    unix_seconds(
        timestamp_add(
            invoices.transaction_date,
            INTERVAL -7 HOUR
        )
    ) transaction_date,
    invoices.total,
    to_hex(
        sha256(REGEXP_REPLACE(customers.contact_number, "^0", "84"))
    ) AS contact_number,
    to_hex(
        sha256(
            CASE
                WHEN customers.gender THEN 'm'
                ELSE 'f'
            END
        )
    ) AS gender,
    branches.branch_name,
FROM
    {{ ref('stg_kiotviet__invoices') }}
    invoices
    LEFT JOIN {{ ref('stg_kiotviet__branches') }}
    branches
    ON invoices.branch_id = branches.branch_id
    LEFT JOIN {{ ref('stg_kiotviet__customers') }}
    customers
    ON invoices.customer_id = customers.customer_id
WHERE
    1 = 1
    AND invoices.transaction_status = 'Hoàn thành'
    AND invoices.total > 0
    AND customers.contact_number <> ''
    AND customers.contact_number IS NOT NULL
    AND transaction_code NOT LIKE 'HDD_%'
