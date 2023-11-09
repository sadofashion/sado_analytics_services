


SELECT
    safe_cast(id as int64) AS user_id,
    userName AS user_name,
    email AS email,
    mobile AS contact_number,
    roleName AS role,
FROM
    `agile-scheme-394814`.`dbt_dev`.`base_nhanhvn__users`