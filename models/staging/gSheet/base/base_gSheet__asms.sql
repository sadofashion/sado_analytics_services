WITH source AS (
    SELECT
        *,
    FROM
        {{ source(
            'gSheet',
            '_ext_asm_list2'
        ) }}
)
SELECT
    asm,
    store_name,
    phone,
    email,
    source.local_page,
    source.region_page,
    source.pic,
FROM
    source
WHERE
    asm <> 'ASM'
