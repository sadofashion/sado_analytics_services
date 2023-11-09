WITH source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                _batched_at DESC
        ) rn_
    FROM
        
        
            (
                SELECT
                    *
                FROM
                    `agile-scheme-394814`.`Nhanhvn`.`p_orders_*`
                LIMIT
                    1000
            )
        

        
),
raw_ AS (
    SELECT
        *
    EXCEPT(rn_)
    FROM
        source
    WHERE
        rn_ = 1
)

SELECT
    safe_cast(orders.id AS int64) AS order_id,
    orders.shopOrderId AS shop_order_id,
    salechannels.sale_channel,
    orders.merchantTrackingNumber AS tracking_number,
    safe_cast(orders.depotId AS int64) AS depot_id,
    orders.depotName AS depot_name,
    orders.type as order_type,
    safe_cast(orders.moneyDiscount AS int64) AS order_discount,
    safe_cast(orders.moneyDeposit AS int64) AS money_deposit,
    safe_cast(orders.moneyTransfer AS int64) AS money_transfer,
    safe_cast(orders.depositAccount as int64) AS deposit_account,
    safe_cast(orders.transferAccount as int64) AS transfer_account,
    safe_cast(orders.usedPoints as int64) AS used_points,
    safe_cast(orders.moneyUsedPoints as int64) AS money_used_points,
    safe_cast(orders.carrierId AS int64) AS carrier_id,
    safe_cast(orders.serviceId as int64) AS service_id,
    orders.carrierCode AS carrier_code,
    safe_cast(orders.shipFee AS int64) AS ship_fee,
    safe_cast(orders.codFee AS int64) AS cod_fee,
    safe_cast(orders.customerShipFee AS int64) AS customer_ship_fee,
    safe_cast(orders.returnFee AS int64) AS return_fee,
    safe_cast(orders.overWeightShipFee AS int64) AS overweight_fee,
    safe_cast(orders.declaredFee AS int64) AS declared_fee,
    orders.description,
    safe_cast(orders.customerId AS int64) AS customer_id,
    orders.customerMobile AS customer_contact_number,
    ARRAY_TO_STRING(
        [orders.customerWard, orders.customerDistrict, orders.customerCity],
        ', '
    ) AS ship_address,
    safe_cast(orders.createdById AS int64) AS created_by_id,
    safe_cast(orders.saleId AS int64) AS sale_id,
    orders.createdDateTime AS created_date,
    DATE(orders.deliveryDate) AS delivery_date,
    DATE(orders.sendCarrierDate) AS send_carrier_date,
    orders.statusName AS order_status,
    safe_cast(orders.calcTotalMoney AS int64) AS receivables,
    case when coalesce(orders.trafficSourceName, salechannels.sale_channel) in ('API','Admin') then 'Chưa phân loại nguồn' else coalesce(orders.trafficSourceName, salechannels.sale_channel) end AS traffic_source_name,
    orders.couponCode AS coupon_code,
array_agg(struct(
    safe_cast(products.productId as int64) AS product_id,
    products.productCode AS product_code,
    safe_cast(products.price as int64) as price,
    safe_cast(products.quantity as int64) as quantity,
    safe_cast(products.discount as int64) as item_discount
    )) products
FROM
    raw_ orders, unnest(products) products
    LEFT JOIN `agile-scheme-394814`.`dbt_dev`.`stg_gsheet__nhanhvnsalechannels` salechannels
    ON safe_cast(orders.saleChannel AS int64) = salechannels.sale_channel_id
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36