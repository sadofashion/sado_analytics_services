{{
    config(
        tags=['caresoft','dimension','view']
    )
}}

SELECT
    id,
    username,
    email,
    phone_no,
    safe_cast(agent_id as int64) agent_id,
    created_at,
    updated_at,
    group_id,
    group_name,
    role_id,
    login_status,
    call_status,
FROM
    {{ ref('base_caresoft__agents') }}
