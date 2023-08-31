SELECT
    id AS bankAccount_id,
    bankName as bankAccount_name,
FROM
    {{ ref('base_kiotViet__bankAccounts') }}
