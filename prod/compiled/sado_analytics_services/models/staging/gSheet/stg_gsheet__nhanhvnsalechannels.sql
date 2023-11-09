

WITH source AS (

    SELECT
        *,
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`GSheet`.`_ext_nhanhvn_salechannel`
                LIMIT
                    1000
            )
        

        
)
SELECT
    safe_cast(
        sale_channel_id AS int64
    ) sale_channel_id,
    sale_channel,
FROM
    source