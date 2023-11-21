SELECT
    *
EXCEPT(rn_)
FROM
    (
        SELECT
            *,
            ROW_NUMBER() over (
                PARTITION BY custom_field_id
                ORDER BY
                    _batched_at DESC
                
            ) rn_
        FROM
            {{ source(
                'caresoft',
                'ticket_custom_fields'
            ) }}
    )
WHERE
    rn_ = 1
