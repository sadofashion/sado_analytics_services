SELECT
    purchaseOrders.id AS transaction_id,
    purchaseOrders.code AS transaction_code,
    purchaseOrders.purchaseDate AS transaction_date,
    CASE
        WHEN purchaseOrders.status = 3 THEN "Đã nhập hàng"
        WHEN purchaseOrders.status = 1 THEN "Phiếu tạm"
        WHEN purchaseOrders.status = 4 THEN "Đã huỷ"
    END AS transaction_status,
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
    `agile-scheme-394814`.`dbt_dev`.`base_kiotViet__purchaseOrders` purchaseOrders
    left join unnest(purchaseOrderDetails) purchaseOrderDetails