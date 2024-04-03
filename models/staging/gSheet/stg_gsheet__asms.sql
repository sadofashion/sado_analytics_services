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
        opening_day,
        close_date,
        region,
        province,

    FROM
        {{ ref('base_gSheet__asms') }}