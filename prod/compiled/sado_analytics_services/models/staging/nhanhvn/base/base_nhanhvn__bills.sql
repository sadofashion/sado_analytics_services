WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                _batched_at DESC
        ) AS rn_
    FROM
    
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`Nhanhvn`.`p_bills_*`
                LIMIT
                    1000
            )
        

        
),
raw_ as (SELECT
    * except(rn_)
FROM
    source
WHERE
    rn_ = 1
)

SELECT
safe_cast(b.id as int64) as bill_id,
safe_cast(b.relatedBillId as int64) as related_bill_id,
safe_cast(b.depotId as int64) as depot_id,
safe_cast(b.orderId as int64) as order_id,
safe_cast(b.requirementBillId as int64) as requirement_bill_id,
safe_cast(b.inventoryCheckId as int64) as inventory_check_id,
date(b.date) as bill_date,
b.createdDateTime as created_date,
safe_cast(b.customerId as int64) as customer_id,
safe_cast(b.saleId as int64) as sale_id,
case b.type
    when '2' then 'Xuất kho'
    when '1' then 'Nhập kho'
    end as bill_type,
case b.mode
    when '1' then 'Giao hàng'
    when '2' then 'Bán lẻ'
    when '3' then 'Chuyển kho'
    when '4' then 'Quà tặng'
    when '5' then 'Nhà cung cấp'
    when '6' then 'Bán sỉ'
    when '8' then 'Kiểm kho'
    when '10' then 'Khác'
    end as bill_mode,
safe_cast(b.createdByID as int64) as created_by_id,
safe_cast(b.technicalId as int64) as technical_id,
b.discount,
b.points,
b.usedPoints as used_points,
b.money,
b.saleBonus as sale_bonus,
b.moneyTransfer as money_transfer,
b.cash,
b.installmentMoney as installment_money,
b.creditMoney as credit_money,
b.usedPointsMoney as money_used_points,
b.returnFee as return_fee,
b.payment,
b.description,
b.supplierId as supplier_id,
b.couponCode as coupon_code,
safe_cast(b.couponValue as int64) as coupon_value,
safe_cast(b.customerMoney as int64) as customer_money,
b.moneyReturn as money_return,
b.cashAccount as cash_account,
b.transferAccount as transfer_account,
b.creditCode as credit_code,
b.installmentAccount as installment_account,
array_agg(struct(
    safe_cast(products.id as int64) as product_id,
    products.code as product_code,
    products.name as product_name,
    safe_cast(products.quantity as int64) as quantity,
    safe_cast(products.price as int64) as price,
    safe_cast(products.discount as int64) as discount,
    safe_cast(products.vat as int64) as vat,
    safe_cast(products.extendedWarrantyMoney as int64) as extended_warranty_money
)) as products
FROM
    raw_ b, unnest(products) products
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36