{{
  config(
    tags=['view', 'dimension','kiotviet']
  )
}}

SELECT
    distinct
    supplierId as supplier_id,
    supplierName as supplier_name,
    supplierCode as supplier_code
FROM
    {{ ref('base_kiotViet__purchaseOrders') }}
