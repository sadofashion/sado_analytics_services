{{
  config(
    materialized = 'incremental',
    partition_by = {"field": "created_date", "data_type": "datetime", "granularity": "day"},
    incremental_strategy = 'insert_overwrite',
    unique_key = 'order_id',
    on_schema_change = 'sync_all_columns',
    tags = ['incremental', 'hourly','nhanhvn']
    )
}}

WITH source AS (
    select * from 
    {{ source('nhanhvn', 'p_orders') }}
    where 1=1
    and createdDateTime is not null
    {% if is_incremental() %}
    and date(createdDateTime) in (
        select distinct date(createdDateTime) 
        from {{ source('nhanhvn', 'p_orders') }} 
        where parse_date('%Y%m%d',_TABLE_SUFFIX) >= date_add(current_date, interval -1 day)
        )
    {# and parse_date('%Y%m%d',_TABLE_SUFFIX) >= date_add(current_date, interval -7 day) #}
    {% endif %}
),

deduplicate as (
    {{ dbt_utils.deduplicate(
        relation = 'source',
        partition_by = 'id',
        order_by = "_batched_at desc",
    ) }}
),

deleted_orders as (
    {{dbt_utils.deduplicate(
    relation=source('nhanhvn','p_webhook_orderDelete'),
    partition_by='orderId',
    order_by='orderId desc'
)}}
),

salechannels AS (
    SELECT
        *
    FROM
        unnest([ 
            struct<sale_channel_id NUMERIC, sale_channel string> 
            (1, 'Admin'), (2, 'Website'), (10, 'API'), 
            (20, 'Facebook'), (21, 'Instagram'), (41, 'Lazada.vn'), (42, 'Shopee.vn'), 
            (43, 'Sendo.vn'), (45, 'Tiki.vn'), (46, 'Zalo Shop'), (47, '1Landing.vn'), 
            (48, 'Tiktok Shop'), (49, 'Zalo OA'), (50, 'Shopee Chat'), (51, 'Lazada Chat')
            ])
)
SELECT
    safe_cast(orders.id AS int64) AS order_id,
    orders.shopOrderId AS shop_order_id,
    farm_fingerprint(salechannels.sale_channel) sales_channel_id,
    orders.merchantTrackingNumber AS tracking_number,
    safe_cast(orders.depotId AS int64) AS depot_id,
    orders.depotName AS depot_name,
    orders.type AS order_type,
    safe_cast(orders.moneyDiscount AS int64) AS order_discount,
    safe_cast(orders.moneyDeposit AS int64) AS money_deposit,
    safe_cast(orders.moneyTransfer AS int64) AS money_transfer,
    safe_cast(orders.depositAccount AS int64) AS deposit_account,
    safe_cast(orders.transferAccount AS int64) AS transfer_account,
    safe_cast(orders.usedPoints AS int64) AS used_points,
    safe_cast(orders.moneyUsedPoints AS int64) AS money_used_points,
    safe_cast(orders.carrierId AS int64) AS carrier_id,
    safe_cast(orders.serviceId AS int64) AS service_id,
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
    ARRAY_TO_STRING([orders.customerWard, orders.customerDistrict, orders.customerCity],', ') AS ship_address,
    safe_cast(orders.createdById AS int64) AS created_by_id,
    safe_cast(orders.saleId AS int64) AS sale_id,
    orders.createdDateTime AS created_date,
    DATE(nullif(orders.deliveryDate,'0000-00-00')) AS delivery_date,
    DATE(nullif(orders.sendCarrierDate,'0000-00-00')) AS send_carrier_date,
    orders.statusName AS order_status,
    safe_cast(orders.calcTotalMoney AS int64) AS receivables,
    farm_fingerprint(
        CASE
            WHEN COALESCE(orders.trafficSourceName,salechannels.sale_channel) IN ('API','Admin') THEN 'Chưa phân loại nguồn'
            ELSE COALESCE(orders.trafficSourceName,salechannels.sale_channel)
        END
    ) AS traffic_source_id,
    orders.couponCode AS coupon_code,
    safe_cast(orders.returnFromOrderId AS int64) return_from_order_id,
    _batched_at as last_sync,
    ARRAY_AGG(
        STRUCT(
            safe_cast(products.productId AS int64) AS product_id, 
            products.productCode AS product_code, 
            safe_cast(products.price AS int64) AS price, 
            safe_cast(products.quantity AS int64) AS quantity, 
            safe_cast(products.discount AS int64) AS item_discount
            )
    ) products,
FROM
    deduplicate orders,
    unnest(products) products
    LEFT JOIN salechannels
    ON orders.saleChannel = salechannels.sale_channel_id 
    left join deleted_orders
    on orders.id = deleted_orders.orderId
    where deleted_orders.orderId is null
{{ dbt_utils.group_by(38) }}