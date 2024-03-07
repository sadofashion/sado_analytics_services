WITH source AS (
    {{ dbt_utils.deduplicate(
        relation = source(
            'nhanhvn',
            'p_orders_*'
        ),
        partition_by = 'id',
        order_by = "_batched_at desc",
    ) }}
)
SELECT
    safe_cast(
        orders.id AS int64
    ) AS order_id,
    orders.shopOrderId AS shop_order_id,
    salechannels.sale_channel,
    orders.merchantTrackingNumber AS tracking_number,
    safe_cast(
        orders.depotId AS int64
    ) AS depot_id,
    orders.depotName AS depot_name,
    orders.type AS order_type,
    safe_cast(
        orders.moneyDiscount AS int64
    ) AS order_discount,
    safe_cast(
        orders.moneyDeposit AS int64
    ) AS money_deposit,
    safe_cast(
        orders.moneyTransfer AS int64
    ) AS money_transfer,
    safe_cast(
        orders.depositAccount AS int64
    ) AS deposit_account,
    safe_cast(
        orders.transferAccount AS int64
    ) AS transfer_account,
    safe_cast(
        orders.usedPoints AS int64
    ) AS used_points,
    safe_cast(
        orders.moneyUsedPoints AS int64
    ) AS money_used_points,
    safe_cast(
        orders.carrierId AS int64
    ) AS carrier_id,
    safe_cast(
        orders.serviceId AS int64
    ) AS service_id,
    orders.carrierCode AS carrier_code,
    safe_cast(
        orders.shipFee AS int64
    ) AS ship_fee,
    safe_cast(
        orders.codFee AS int64
    ) AS cod_fee,
    safe_cast(
        orders.customerShipFee AS int64
    ) AS customer_ship_fee,
    safe_cast(
        orders.returnFee AS int64
    ) AS return_fee,
    safe_cast(
        orders.overWeightShipFee AS int64
    ) AS overweight_fee,
    safe_cast(
        orders.declaredFee AS int64
    ) AS declared_fee,
    orders.description,
    safe_cast(
        orders.customerId AS int64
    ) AS customer_id,
    orders.customerMobile AS customer_contact_number,
    ARRAY_TO_STRING(
        [orders.customerWard, orders.customerDistrict, orders.customerCity],
        ', '
    ) AS ship_address,
    safe_cast(
        orders.createdById AS int64
    ) AS created_by_id,
    safe_cast(
        orders.saleId AS int64
    ) AS sale_id,
    orders.createdDateTime AS created_date,
    DATE(
        orders.deliveryDate
    ) AS delivery_date,
    DATE(
        orders.sendCarrierDate
    ) AS send_carrier_date,
    orders.statusName AS order_status,
    safe_cast(
        orders.calcTotalMoney AS int64
    ) AS receivables,
    CASE
        WHEN COALESCE(
            orders.trafficSourceName,
            salechannels.sale_channel
        ) IN (
            'API',
            'Admin'
        ) THEN 'Chưa phân loại nguồn'
        ELSE COALESCE(
            orders.trafficSourceName,
            salechannels.sale_channel
        )
    END AS traffic_source_name,
    orders.couponCode AS coupon_code,
    ARRAY_AGG(
        STRUCT(safe_cast(products.productId AS int64) AS product_id, products.productCode AS product_code, safe_cast(products.price AS int64) AS price, safe_cast(products.quantity AS int64) AS quantity, safe_cast(products.discount AS int64) AS item_discount)
    ) products
FROM
    source orders,
    unnest(products) products
    LEFT JOIN {{ ref('stg_gsheet__nhanhvnsalechannels') }}
    salechannels
    ON safe_cast(
        orders.saleChannel AS int64
    ) = salechannels.sale_channel_id 
    {{ dbt_utils.group_by(36) }}
