SELECT
    id AS bankAccountId,
    bankName as bankAccountName,
FROM
    {{ ref('base_kiotViet__bankAccounts') }}
