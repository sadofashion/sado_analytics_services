{{
  config(
    tags=['table','dimension']
    )
}}

WITH kiotviet_products AS (
    SELECT
        kiot.product_id AS kiotviet_product_id,
        kiot.product_name,
        kiot.product_code,

        kiot.class_code,
        kiot.class_name,

        kiot.category,
        kiot.sub_productline,
        kiot.productline,
        kiot.product_group,

        kiot.ads_product_mapping,
    FROM
        {{ ref("stg_kiotviet__products") }}
        kiot
    WHERE
        kiot.productline NOT IN ('NGUYÊN PHỤ LIỆU','NPL 2023','DANH MỤC VẬN CHUYỂN','VẬT TƯ - QUÀ TẶNG')
        {# AND kiot.type NOT IN ('Combo') #}
),
nhanhvn__products AS (
    SELECT
        product_id AS nhanhvn_product_id,
        product_name,
        product_code,
        class_code,
        category_name,
        sub_productline,
        productline,
        ads_product_mapping
    FROM
        (
            {{ dbt_utils.deduplicate(relation=ref("stg_nhanhvn__products"),
                partition_by = 'product_code',
                order_by = 'product_id desc') }}
        )
    WHERE 1=1
        {# and type_name NOT IN ('Combo') #}
)
SELECT
    coalesce(
        p1.product_code,
        p2.product_code
    ) AS product_code,
    coalesce(
        p1.product_name,
        p2.product_name
    ) AS product_name,
    coalesce(
        p1.class_code,
        p2.class_code
    ) AS class_code,
    p1.class_name,
    coalesce(p1.sub_productline,p2.sub_productline) as sub_productline,
    coalesce(p1.productline,p2.productline) as productline,
    CASE WHEN regexp_contains(lower(coalesce(p1.productline,p2.productline)),r'thu đông') THEN 'Hàng đông'
    WHEN regexp_contains(lower(coalesce(p1.productline,p2.productline)),r'xuân hè') THEN 'Hàng hè'
    when coalesce(p1.productline,p2.productline) is not null then 'Quanh năm' 
    else 'Chưa phân loại' END AS product_group,
    {# p1.product_group, #}
    coalesce(p1.ads_product_mapping,p2.ads_product_mapping) as ads_product_mapping,
    coalesce(
        p1.category,
        p2.category_name
    ) AS category,
    p1.kiotviet_product_id,
    p2.nhanhvn_product_id,
FROM
    kiotviet_products p1 
    full OUTER JOIN nhanhvn__products p2
    ON p1.product_code = p2.product_code
    where length(COALESCE(
        p1.product_code,
        p2.product_code
    )) >=8
    and (p1.productline not in ('ĐỒNG PHỤC') or p1.productline is null)
    
