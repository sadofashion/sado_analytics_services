SELECT
    invoices.transaction_id,
    invoices.transaction_code,
    CAST(
        NULL AS int64
    ) AS reference_transaction_id,
    invoices.transaction_date,
    invoices.transaction_status,
    invoices.branch_id,
    invoices.employee_id,
    invoices.customer_id,
    invoices.product_id,
    invoices.product_code,
    invoices.quantity,
    invoices.price,
    invoices.discount_ratio,
    invoices.discount,
    invoices.subTotal,
    invoices.transaction_type,
FROM
    {{ ref('stg_kiotviet__invoicedetails') }}
    invoices
WHERE
    invoices.transaction_status = 'Hoàn thành'
    and invoices.subTotal <> 0
UNION ALL
SELECT
    returns.transaction_id,
    returns.transaction_code,
    returns.reference_transaction_id,
    returns.transaction_date,
    returns.transaction_status,
    returns.branch_id,
    returns.employee_id,
    returns.customer_id,
    returns.product_id,
    returns.product_code,
    returns.quantity,
    returns.price,
    cast(null as float64) as discount_ratio,
    cast(null as float64) as discount,
    returns.subTotal,
    returns.transaction_type
FROM
    {{ ref('stg_kiotviet__returndetails') }}
    returns
WHERE
    returns.transaction_status = 'Đã trả'