CREATE OR REPLACE TABLE `serious-music-295616.ev.stg_charge_stations` AS
SELECT
  JSON_VALUE(item, '$.id') AS id,
  JSON_VALUE(item, '$.commonName') AS common_name,
  JSON_VALUE(item, '$.placeType') AS place_type,
  CAST(JSON_VALUE(item, '$.lat') AS FLOAT64) AS lat,
  CAST(JSON_VALUE(item, '$.lon') AS FLOAT64) AS lon,
  item AS raw_json,
  CURRENT_TIMESTAMP() AS snapshot_ts
FROM UNNEST(@json_data) AS item;
