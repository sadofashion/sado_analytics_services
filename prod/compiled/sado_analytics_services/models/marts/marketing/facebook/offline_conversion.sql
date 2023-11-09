

SELECT
    invoices.transaction_code,
    invoices.created_date,
    unix_seconds(timestamp_add(invoices.transaction_date, interval -7 hour)) transaction_date,
    invoices.total,
    to_hex(sha256(regexp_replace(customers.contact_number,"^0","84"))) as contact_number,
    to_hex(sha256(case when customers.gender then 'm' else 'f' end)) as gender,
    branches.branch_name,
FROM
    `agile-scheme-394814`.`dbt_dev`.`stg_kiotviet__invoices`
    invoices
    LEFT JOIN `agile-scheme-394814`.`dbt_dev`.`stg_kiotviet__branches`
    branches
    ON invoices.branch_id = branches.branch_id
    LEFT JOIN `agile-scheme-394814`.`dbt_dev`.`stg_kiotviet__customers`
    customers
     ON invoices.customer_id = customers.customer_id
WHERE 1=1
    and invoices.transaction_status = 'Hoàn thành'
    AND invoices.total > 0
    AND customers.contact_number <> ''
    AND customers.contact_number IS NOT NULL
    and transaction_code not like 'HDD_%'