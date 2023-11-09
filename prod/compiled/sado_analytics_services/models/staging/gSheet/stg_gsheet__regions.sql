

WITH source AS (

    SELECT
        *,
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`GSheet`.`_ext_region`
                LIMIT
                    1000
            )
        

        
        where branch_name is not null
)
SELECT
    *
FROM
    source