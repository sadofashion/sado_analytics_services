{{ config(
    tags = ['view', 'dimension','kiotviet']
) }}

WITH staging AS (

    SELECT
        C.id AS customer_id,
        C.code AS customer_code,
        C.NAME AS customer_name,
        C.gender,
        EXTRACT(
            MONTH
            FROM
                C.birthDate
        ) AS birth_month,
        C.birthDate AS birth_date,
        regexp_replace(C.contactNumber,r'\W','') AS contact_number,
        C.branchId AS branch_id,
        C.TYPE AS customer_type,
        C.groups AS customer_groups,
        C.debt,
        C.totalInvoiced AS total_invoiced,
        C.totalPoint AS total_point,
        C.totalRevenue AS total_revenue,
        C.rewardPoint AS rewardpoint,
        C.createdDate AS created_date,
        B.branch_name,
        C.modifieddate as modified_date,
    FROM
        {{ ref('base_kiotViet__customers') }} C
    left join  {{ref('stg_kiotviet__branches')}} B on C.branchId = B.id
)
SELECT
    DISTINCT staging.customer_id,
    staging.customer_name,
    staging.gender,
    staging.birth_month,
    staging.birth_date,
    case 
        when 
            length(staging.contact_number) = 9 
            and regexp_contains(staging.contact_number,r'^[1-9]') 
            then concat('0',staging.contact_number) 
        else 
            regexp_replace(staging.contact_number,r'^(84|840)','0') 
        end as contact_number,
    staging.branch_id,
    staging.customer_type,
    staging.customer_groups,
    staging.debt,
    staging.total_invoiced,
    staging.total_point,
    staging.total_revenue,
    staging.rewardpoint,
    staging.created_date,
    staging.branch_name,
    staging.modified_date,
    CASE
        WHEN DATE_TRUNC(DATE(staging.created_date), MONTH) < DATE_TRUNC(CURRENT_DATE(), MONTH) THEN 'old'
        ELSE 'new'END AS customer_recency_group,
    FROM
        staging
