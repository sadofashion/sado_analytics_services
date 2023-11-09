

WITH contacts AS (
    SELECT
        *
    FROM
        `agile-scheme-394814`.`dbt_dev`.`base_caresoft__contacts`
),
contact_details AS (
    SELECT
        *
    FROM
        `agile-scheme-394814`.`dbt_dev`.`base_caresoft__contact_details`
)
SELECT
    C.created_at,
    C.id AS contact_id,
    C.phone_no,
    C.updated_at,
    C.username,
    cd.account_id,
    cd.address,
    cd.campaign_handler_id,
    cd.city_id,
    cd.created_from,
    cd.custom_fields,
    cd.district_id,
    cd.email,
    cd.email2,
    cd.gender,
    cd.organization,
    cd.organization_id,
    cd.phone_no2,
    cd.phone_no3,
    cd.psid,
    cd.role_id,
    cd.take_email_at,
    cd.take_phone_at,
FROM
    contacts C
    LEFT JOIN contact_details cd
    ON C.id = cd.id