with staging as (
    SELECT
    id as customer_id,
    code as customer_code,
    name as customer_name,
    gender,
    extract(month from birthDate) as birth_month,
    birthDate as birth_date,
    contactNumber as contact_number,
    branchId as branch_id,
    type as customer_type,
    c.groups as customer_groups,
    debt,
    totalInvoiced as total_invoiced,
    totalPoint as total_point,
    totalRevenue as total_revenue,
    rewardPoint as rewardpoint,
    createdDate as created_date
FROM
    {{ ref('base_kiotViet__customers') }} c
),
customers AS (
    SELECT
        staging.customer_id,
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
        staging.total_revenue AS monetary,
        staging.rewardpoint,
        staging.created_date,
        CASE
            WHEN DATE_TRUNC(DATE(staging.created_date), MONTH) < DATE_TRUNC(CURRENT_DATE(), MONTH) THEN 'old'
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
                staging
                LEFT JOIN {{ ref('stg_kiotviet__invoices') }}
                invoices
                ON staging.customer_id = invoices.customer_id window w1 AS (
                    PARTITION BY staging.customer_id
                    ORDER BY
                        invoices.transaction_date DESC
                ),
                w2 AS (
                    PARTITION BY staging.customer_id
                )
        )
    SELECT
        *,
        date_diff(current_timestamp(),DATE(last_purchase), DAY) AS recency,
    FROM
        customers