

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
        `agile-scheme-394814`.`dbt_dev`.`base_kiotViet__customers` C
)
SELECT
    DISTINCT staging.customer_id,
    staging.customer_name,
    staging.gender,
    staging.birth_month,
    staging.birth_date,
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
        FROM
            staging