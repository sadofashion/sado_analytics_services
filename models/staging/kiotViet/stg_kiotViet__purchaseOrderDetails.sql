SELECT
    purchaseOrders.id AS transaction_id,
    purchaseOrders.code AS transactionCode,
    purchaseOrders.purchaseDate AS transaction_date,
    CASE
        WHEN purchaseOrders.status = 3 THEN "Đã nhập hàng"
        WHEN purchaseOrders.status = 1 THEN "Phiếu tạm"
        WHEN purchaseOrders.status = 4 THEN "Đã huỷ"
    END AS transactionStatus,
    purchaseOrders.branchId,
    purchaseOrders.purchaseById AS employeeId,
    purchaseOrders.supplierId,
    purchaseOrderDetails.productId,
    purchaseOrderDetails.productCode,
    purchaseOrderDetails.quantity,
    purchaseOrderDetails.price,
    purchaseOrderDetails.discountRatio,
    purchaseOrderDetails.discount,
FROM
    {{ ref('base_kiotViet__purchaseOrders') }} purchaseOrders,
    unnest(purchaseOrderDetails) purchaseOrderDetails 
