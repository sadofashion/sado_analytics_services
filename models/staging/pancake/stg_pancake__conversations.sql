SELECT
    conversations.inserted_at,
    conversations.conversation_id,
    conversations.customer_id,
    conversations.user_id,
    conversations.page_id,
    conversations.post_id,
    conversations.tag_id,
    conversations.message_count,
    conversations.snippet,
    conversations.type,
    conversations.updated_at,
FROM
    {{ ref("base_pancake__conversations") }}
    conversations
