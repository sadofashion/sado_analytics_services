SELECT
    regexp_extract(message,r'^(.*\n.*)') post_title,
    admin_creator.id AS user_id,
    admin_creator.name AS user_name,
    updated_at,
    inserted_at,
    page_id,
    post_id,
    like_count,
    reaction_count,
    comment_count,
    phone_number_count,
FROM
    {{ ref("base_pancake__posts") }}
