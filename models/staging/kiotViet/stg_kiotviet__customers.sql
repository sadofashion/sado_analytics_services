WITH staging AS (
    SELECT
        id AS customer_id,
        code AS customer_code,
        NAME AS customer_name,
        gender,
        EXTRACT(
            MONTH
            FROM
                birthDate
        ) AS birth_month,
        birthDate AS birth_date,
        contactNumber AS contact_number,
        branchId AS branch_id,
        TYPE AS customer_type,
        C.groups AS customer_groups,
        debt,
        totalInvoiced AS total_invoiced,
        totalPoint AS total_point,
        totalRevenue AS total_revenue,
        rewardPoint AS rewardpoint,
        createdDate AS created_date
    FROM
        {{ ref('base_kiotViet__customers') }} C
),
customers AS (
    SELECT
        DISTINCT staging.customer_id,
        staging.customer_name,
        staging.gender,
        staging.birth_month,
        staging.contact_number,
        staging.branch_id,
        staging.customer_type,
        staging.customer_groups,
        staging.debt,
        staging.total_invoiced,
        staging.total_point,
        staging.total_revenue,
        staging.rewardpoint,
        staging.created_date,
        CASE
            WHEN DATE_TRUNC(DATE(staging.created_date), MONTH) < DATE_TRUNC(CURRENT_DATE(), MONTH) THEN 'old'
            ELSE 'new'END AS customer_recency_group,
            FIRST_VALUE(
                invoices.transaction_date
            ) over w3 AS first_purchase,
            FIRST_VALUE(
                invoices.transaction_date
            ) over w1 AS last_purchase,
            FIRST_VALUE(
                invoices.transaction_id
            ) over w1 AS last_transaction_id,

            count(invoices.transaction_id) over w2 as frequency,
            sum(invoices.total) over w2 as monetary

            FROM
                staging
                LEFT JOIN {{ ref('stg_kiotviet__invoices') }}
                invoices
                ON staging.customer_id = invoices.customer_id 
                Window w1 AS (
                    PARTITION BY staging.customer_id
                    ORDER BY
                        invoices.transaction_date DESC
                ),
                w3 AS (
                    PARTITION BY staging.customer_id
                    ORDER BY
                        invoices.transaction_date ASC
                ),
                w2 AS (
                    PARTITION BY staging.customer_id order by
                    unix_date(date_trunc(date(invoices.transaction_date),month)) desc range between 90 preceding and current row 
                )
        )
    SELECT
        DISTINCT *,
        date_diff(CURRENT_TIMESTAMP(), last_purchase, DAY) AS recency,
    FROM
        customers
