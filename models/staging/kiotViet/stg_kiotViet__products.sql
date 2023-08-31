SELECT
    p.productId,
    p.categoryId,
    p.fullName AS productName,
    REGEXP_REPLACE(
        p.fullName,
        r'\s\-.*$',
        ''
    ) AS className,
    p.code,
    REGEXP_REPLACE(
        p.code,
        r'\d{2}$',
        ''
    ) AS classCode,
    p.tradeMarkName,
    p.isActive,
    p.type,
    p.attributes,
    C.subProductLine,
    C.category,
    C.productLine
FROM
    {{ ref('base_kiotViet__products') }}
    p
    INNER JOIN {{ ref('stg_kiotviet__categories') }} AS C
    ON p.categoryId = C.categoryId
