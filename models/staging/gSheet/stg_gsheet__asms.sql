{{ config(
    materialized = 'table',
    tags = ['gsheet','dimension','table']
) }}

{# WITH asm_list AS ( #}

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
        phone,
        email,
        local_page,
        region_page,
        pic,

    FROM
        {{ ref('base_gSheet__asms') }}
{# )
SELECT
    asm_list.asm_name,
    branches.branch_id,
    asm_list.phone,
    asm_list.email,
    asm_list.local_page,
    asm_list.region_page,
    asm_list.pic,
FROM
    asm_list
    LEFT JOIN {{ ref('stg_kiotviet__branches') }}
    branches
    ON asm_list.store_name = branches.branch_name #}
