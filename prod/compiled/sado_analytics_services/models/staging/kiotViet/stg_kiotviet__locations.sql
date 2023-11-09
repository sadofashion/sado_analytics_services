

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
    `agile-scheme-394814`.`dbt_dev`.`base_kiotViet__locations`