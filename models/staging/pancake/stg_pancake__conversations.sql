SELECT
    conversations.inserted_at,
    {{dbt_utils.generate_surrogate_key(['conversation_id','customer_id'])}} as conversation_id,
    conversations.customer_id,
    conversations.user_id,
    conversations.page_id,
    conversations.post_id,
    conversations.tag_id,
    conversations.message_count,
    conversations.snippet,
    conversations.type,
    conversations.updated_at,
    conversations.ad_id,
    conversations.tag_histories
FROM
    {{ ref("base_pancake__conversations") }}
    conversations
