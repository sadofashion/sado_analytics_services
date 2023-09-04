SELECT
    NAME,
    normalName,
    regexp_extract(
        NAME,
        r'^(.*)\s-\s(?:.*)'
    ) AS province,
    regexp_extract(
        NAME,
        r'^(?:.*)\s-\s(.*)'
    ) AS district,
FROM
    {{ ref('base_kiotViet__locations') }}
