WITH customers AS (
    SELECT
        customers.customer_id,
        customers.customer_name,
        customers.gender,
        customers.birth_month,
        customers.contact_number,
        customers.customer_type,
        customers.customer_groups,
        customers.debt,
        customers.total_invoiced,
        customers.total_point,
        customers.total_revenue AS monetary,
        customers.rewardpoint,
        customers.created_date,
        CASE
            WHEN DATE_TRUNC(DATE(customers.created_date), MONTH) < DATE_TRUNC(CURRENT_DATE(), MONTH) THEN 'old'
            ELSE 'new'END AS customer_recency_group,
            LAST_VALUE(
                invoices.transaction_date
            ) over w1 AS first_purchase,
            FIRST_VALUE(
                invoices.transaction_date
            ) over w1 AS last_purchase,
            FIRST_VALUE(
                invoices.transaction_id
            ) over w1 AS last_transaction_id,
            COUNT(DISTINCT DATE(invoices.transaction_date)) over w2 AS frequency,
            FROM
                {{ ref('stg_kiotviet__customers') }}
                customers
                LEFT JOIN {{ ref('stg_kiotviet__invoices') }}
                invoices
                ON customers.customer_id = invoices.customer_id window w1 AS (
                    PARTITION BY customers.customer_id
                    ORDER BY
                        invoices.transaction_date DESC
                ),
                w2 AS (
                    PARTITION BY customers.customer_id
                )
        )
    SELECT
        *,
        date_diff(DATE(last_purchase), CURRENT_DATE(), DAY) AS recency,
    FROM
        customers