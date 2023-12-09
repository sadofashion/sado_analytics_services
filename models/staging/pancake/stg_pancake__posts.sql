SELECT
    regexp_extract(source.message,r'^(.*\n.*)') post_title,
    source.admin_creator.id AS user_id,
    source.admin_creator.name AS user_name,
    source.updated_at,
    source.inserted_at,
    source.page_id,
    source.post_id,
    source.like_count,
    source.reaction_count,
    source.comment_count,
    source.phone_number_count,
FROM
    {{ ref("base_pancake__posts") }}
