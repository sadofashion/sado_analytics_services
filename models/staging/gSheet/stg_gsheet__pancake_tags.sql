{{ config(
    materialized = 'table',
    tags = ['gsheet','dimension','table', 'pancake']
) }}

SELECT
    tag_value, category
FROM
    {{ source(
        'gSheet',
        "_ext_pancake_tags"
    ) }}
