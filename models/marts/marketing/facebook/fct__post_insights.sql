{{ config(
    materialized = 'view',
    tags = ['fact', 'view','facebook']
) }}

SELECT
    *, 
    split(post_id,"_")[1] as page_id,
FROM
    {{ ref("fct_fb__post_insights") }}