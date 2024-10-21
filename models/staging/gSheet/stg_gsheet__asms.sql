{{ config(
    materialized = 'table',
    tags = ['gsheet','dimension','table']
) }}

SELECT
    asm AS asm_name,
    CASE
        WHEN store_name = '5S Thái Bình 1' THEN '5S Thái bình 1'
        WHEN store_name LIKE '5S Thanh Hoá%' THEN REGEXP_REPLACE(
            store_name,
            'Thanh Hoá',
            'Thanh Hóa'
        )
        ELSE store_name
    END AS store_name,
    * except(store_name,asm)
FROM
    {{ source(
        'gSheet',
        'asm_list'
    ) }}
WHERE
    asm <> "ASM"
