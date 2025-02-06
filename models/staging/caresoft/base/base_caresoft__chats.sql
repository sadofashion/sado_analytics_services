{# SELECT
    *
EXCEPT(rn_)
FROM
    (
        SELECT
            *,
            ROW_NUMBER() over (
                PARTITION BY conversation_id
                ORDER BY
                    _batched_at DESC
            ) rn_
        FROM
            {{ source(
                'caresoft',
                'chats'
            ) }}
    )
WHERE
    rn_ = 1 #}


with union_ as (
    {{union_relations(
    relations=[
        source('caresoft', 'chats_fb'),
        source('caresoft', 'chats_live'),
        source('caresoft', 'chats_zalo')
        ],
) }}
), 
{{deduplicate_cte(
    cte = 'union_',
    partition_fields=['conversation_id'],
    last_updated_fields=['_batched_at']
    )}}