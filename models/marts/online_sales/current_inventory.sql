{{
  config(
    tags=['table', 'fact','nhanhvn']
  )
}}

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
           where 1=1  and depot_name = 'KHO ONLINE HÀ NỘI'
    )
WHERE
    rn_ = 1
