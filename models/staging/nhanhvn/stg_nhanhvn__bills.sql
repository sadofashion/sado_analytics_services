{{ config(
  tags = ['view','nhanhvn']
) }}

{% set bill_types ={ "1" :"Nhập kho",
"2" :"Xuất kho" } %}
{% set bill_modes ={ "1" :"Giao hàng",
"2" :"Bán lẻ",
"3" :"Chuyển kho",
"4" :"Quà tặng",
"5" :"Nhà cung cấp",
"6" :"Bán sỉ",
"8" :"Kiểm kho",
"10" :"Khác" } %}


WITH source AS (
    {{ dbt_utils.deduplicate(
        relation = source(
            'nhanhvn',
            'p_bills_*'
        ),
        partition_by = 'id',
        order_by = "_batched_at desc",
    ) }}
)
SELECT
    safe_cast(b.id AS int64) AS bill_id,
    safe_cast(b.relatedBillId AS int64) AS related_bill_id,
    safe_cast(b.depotId AS int64) AS depot_id,
    safe_cast(b.orderId AS int64) AS order_id,
    safe_cast(b.requirementBillId AS int64) AS requirement_bill_id,
    safe_cast(b.inventoryCheckId AS int64) AS inventory_check_id,
    DATE(b.date) AS bill_date,
    b.createdDateTime AS created_date,
    safe_cast(b.customerId AS int64) AS customer_id,
    safe_cast(b.saleId AS int64) AS sale_id,
    CASE b.type
    {% for key, value in bill_types.items() %}
    WHEN '{{key}}' THEN '{{value}}'
{% endfor %} END AS bill_type,
CASE b.mode
{% for key, value in bill_modes.items() %}
WHEN '{{key}}' THEN '{{value}}'
{% endfor %} END AS bill_mode,
safe_cast(b.createdByID AS int64) AS created_by_id,
safe_cast(b.technicalId AS int64) AS technical_id,
b.discount,
b.points,
b.usedPoints AS used_points,
b.money,
b.saleBonus AS sale_bonus,
b.moneyTransfer AS money_transfer,
b.cash,
b.installmentMoney AS installment_money,
b.creditMoney AS credit_money,
b.usedPointsMoney AS money_used_points,
b.returnFee AS return_fee,
b.payment,
b.description,
b.supplierId AS supplier_id,
b.couponCode AS coupon_code,
safe_cast(b.couponValue AS int64) AS coupon_value,
safe_cast(b.customerMoney AS int64) AS customer_money,
b.moneyReturn AS money_return,
b.cashAccount AS cash_account,
b.transferAccount AS transfer_account,
b.creditCode AS credit_code,
b.installmentAccount AS installment_account,
ARRAY_AGG(
    STRUCT(
        safe_cast(products.id AS int64) AS product_id, 
        products.code AS product_code, 
        products.name AS product_name, 
        safe_cast(products.quantity AS int64) AS quantity, 
        safe_cast(products.price AS int64) AS price, 
        safe_cast(products.discount AS int64) AS discount, 
        safe_cast(products.vat AS int64) AS vat, 
        safe_cast(products.extendedWarrantyMoney AS int64) AS extended_warranty_money
        )
) AS products
FROM
    source b,
    unnest(products) products
    {{ dbt_utils.group_by(36) }}
