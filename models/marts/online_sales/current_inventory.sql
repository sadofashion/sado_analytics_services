{{ config(
    tags = ['table', 'fact','nhanhvn']
) }}

SELECT
    *
EXCEPT(rn_)
FROM
    (
        SELECT
            *,
            ROW_NUMBER() over (
                PARTITION BY product_id,
                depot_name
                ORDER BY
                    updated_at DESC
            ) AS rn_
        FROM
            {{ ref('stg_nhanhvn__inventories') }}
        WHERE
            1 = 1
            AND depot_name = 'KHO ONLINE HÀ NỘI'
    )
WHERE
    rn_ = 1
