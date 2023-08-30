SELECT
    distinct
    supplierId,
    supplierName,
    supplierCode
FROM
    {{ ref('base_kiotViet__purchaseOrders') }}
