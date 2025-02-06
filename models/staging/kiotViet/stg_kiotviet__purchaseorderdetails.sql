SELECT
    purchaseOrders.id AS transaction_id,
    purchaseOrders.code AS transaction_code,
    purchaseOrders.purchaseDate AS transaction_date,
    purchaseOrders.transaction_status,
    purchaseOrders.branchId as branch_id,
    purchaseOrders.purchaseById AS employee_id,
    purchaseOrders.supplierId as supplier_id,
    purchaseOrderDetails.productId as product_id,
    purchaseOrderDetails.productCode as product_code,
    purchaseOrderDetails.quantity,
    purchaseOrderDetails.price,
    purchaseOrderDetails.discountRatio as discount_ratio,
    purchaseOrderDetails.discount,
FROM
    {{ ref('base_kiotViet__purchaseOrders') }} purchaseOrders
    left join unnest(purchaseOrderDetails) purchaseOrderDetails 