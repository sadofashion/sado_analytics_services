WITH kiotviet_products AS (
    SELECT
        kiot.product_name,
        kiot.product_code,
        kiot.class_code,
        kiot.class_name,
        kiot.sub_productline,
        kiot.productline,
        kiot.product_group,
        kiot.ads_product_mapping,
        kiot.category,
        kiot.product_id AS kiotviet_product_id
    FROM
        {{ ref("stg_kiotviet__products") }}
        kiot
    WHERE
        kiot.productline NOT IN ('NGUYÊN PHỤ LIỆU')
        AND kiot.type NOT IN ('Combo')
),
nhanhvn__products AS (
    SELECT
        product_id AS nhanhvn_product_id,
        product_name,
        product_code,
        class_code,
        category_name,
    FROM
        {{ ref("stg_nhanhvn__products") }}
    WHERE
        type_name NOT IN ('Combo')
)
SELECT
    COALESCE(
        p1.product_code,
        p2.product_code
    ) AS product_code,
    COALESCE(
        p1.product_name,
        p2.product_name
    ) AS product_name,
    COALESCE(
        p1.class_code,
        p2.class_code
    ) AS class_code,
    p1.class_name,
    p1.sub_productline,
    p1.productline,
    p1.product_group,
    p1.ads_product_mapping,
    COALESCE(
        p1.category,
        p2.category_name
    ) AS category,
    p1.kiotviet_product_id,
    p2.nhanhvn_product_id,
FROM
    kiotviet_products p1 
    full OUTER JOIN nhanhvn__products p2
    ON p1.product_code = p2.product_code
