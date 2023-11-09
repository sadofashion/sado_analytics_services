WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY idNhanh
            ORDER BY
                _batched_at DESC
        ) rn_
    FROM
        {{ source(
            'nhanhvn',
            'p_products_*'
        ) }}
)
SELECT
    *
EXCEPT(rn_)
FROM
    source
WHERE
    rn_ = 1
