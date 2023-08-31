SELECT
    id as userId,
    userName,
    givenName,
    birthDate,
FROM
    {{ ref('base_kiotViet__users') }}
