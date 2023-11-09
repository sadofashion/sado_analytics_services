WITH source AS (
    SELECT
        *,
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`GSheet`.`_ext_asm_list`
                LIMIT
                    1000
            )
        

        
)
SELECT
    asm,
    store_name,
    phone,
    email
FROM
    source
WHERE
    asm <> 'ASM'