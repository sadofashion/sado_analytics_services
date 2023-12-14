WITH source AS (
    SELECT
        *,
    FROM
        {{ source(
            'gSheet',
            '_ext_asm_list'
        ) }}
)
SELECT
    asm,
    store_name,
    phone,
    email,
    source.page,
    source.pic,
FROM
    source
WHERE
    asm <> 'ASM'
