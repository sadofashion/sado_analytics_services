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
    source.asm,
    source.store_name,
    source.phone,
    source.email,
    source.local_page,
    source.region_page,
    source.pic,
    source.opening_day,
    source.close_date,
    source.province,
    source.region,
FROM
    source
WHERE
    asm <> 'ASM'
