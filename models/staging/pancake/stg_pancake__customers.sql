SELECT
    customer_id,
    customer_name,
    gender,
    inserted_at,
    phone_numbers,
    city,
    customer_facebook_id,
FROM
    {{ ref("base_pancake__customers") }}
