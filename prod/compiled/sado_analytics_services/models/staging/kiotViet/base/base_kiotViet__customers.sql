WITH source AS (
    SELECT
        *
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`KiotViet`.`p_customers_list_*`
                LIMIT
                    1000
            )
        

        
    UNION ALL
    SELECT
        *
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`KiotViet`.`p_webhook_customer_update`
                LIMIT
                    1000
            )
        

        
),
raw_ AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                _batched_at DESC,
                modifiedDate DESC
        ) AS rn_
    FROM
        source
)
SELECT
    id,
    code,
    NAME,
    gender,
    birthDate,
    contactNumber,
    branchId,
    TYPE,
    raw_.groups,
    debt,
    totalInvoiced,
    totalPoint,
    totalRevenue,
    rewardPoint,
    createdDate,
    modifiedDate,
FROM
    raw_
WHERE
    rn_ = 1