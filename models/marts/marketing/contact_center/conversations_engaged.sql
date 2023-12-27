{{ config(
    materialized = 'incremental',
    partition_by ={ 
        'field': 'inserted_at',
        'data_type': 'datetime',
        'granularity': 'day',
        },
    unique_key = ['conversation_id'],
    incremental_strategy = 'merge',
    on_schema_change = 'sync_all_columns',
    tags = ['pancake','fact','incremental']
) }}

{% set tag_fields ={ 
    "agent" :"Nhân sự",
    "conversation_type" :"Phân loại",
    "promotion_type" :"Phân loại chương trình",
    "customer_type" :"Phân loại KH",
    "stage" :"Tiến trình chăm sóc",
    "claim_status" :"Trạng thái xử lý" 
} %}
WITH tags AS (

    SELECT
        DISTINCT *
    FROM
        {{ ref("stg_pancake__tags") }}
),
raw_ AS (
    SELECT
        conversations.inserted_at,
        conversations.conversation_id,
        conversations.customer_id,
        conversations.user_id,
        conversations.page_id,
        conversations.post_id,
        conversations.message_count,
        conversations.snippet,
        conversations.type,
        CASE
            WHEN COUNT(ad_id) over(
                PARTITION BY conversations.ad_id
            ) > 0 THEN 'Ads'
            ELSE 'Non-Ads'
        END AS ads_assisted,
        tags.tag_value,
        tags.category,
    FROM
        {{ ref("stg_pancake__conversations") }}
        conversations
        LEFT JOIN tags
        ON conversations.tag_id = tags.tag_id
    
{% if is_incremental() %}
WHERE (
    DATE(conversations.updated_at) > date_sub(DATE(_dbt_max_partition), interval 3 day) 
)
{% endif %}
)
SELECT
    DISTINCT *
FROM
    raw_ 
    pivot (string_agg(distinct tag_value,',') for category IN ({% for key, value in tag_fields.items() %}
        "{{value}}" AS {{ key }}
        {{ "," if not loop.last }}
    {% endfor %}))
