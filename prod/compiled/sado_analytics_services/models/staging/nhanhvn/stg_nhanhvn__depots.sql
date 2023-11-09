

SELECT
    safe_cast(id as int64) AS depot_id,
    NAME AS depot_name,
    mobile AS contact_number,
    cityName AS city,
    districtName AS district,
    address
FROM
    `agile-scheme-394814`.`dbt_dev`.`base_nhanhvn__stores`