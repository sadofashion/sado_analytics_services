SELECT
    id AS transactionId,
    code AS transactionCode,
    purchaseDate AS transactionDate,
    branchId,
    purchaseById AS employeeId,
    supplierId,
    partnerType,
    total,
    totalPayment,
    discount,
    discountRatio,
    CASE
        WHEN status = 3 THEN "Đã nhập hàng"
        WHEN status = 1 THEN "Phiếu tạm"
        WHEN status = 4 THEN "Đã huỷ"
    END AS transactionStatus,
    createdDate,
FROM
    {{ ref('base_kiotViet__purchaseOrders') }}
