CREATE OR REPLACE TABLE `serious-music-295616.ev.stg_charge_connectors` AS
SELECT
  JSON_VALUE(item, '$.id') AS id,
  (SELECT JSON_VALUE(p, '$.value')
   FROM UNNEST(JSON_QUERY_ARRAY(item, '$.additionalProperties')) AS p
   WHERE JSON_VALUE(p, '$.key') = 'ParentStation') AS station_ref,
  (SELECT JSON_VALUE(p, '$.value')
   FROM UNNEST(JSON_QUERY_ARRAY(item, '$.additionalProperties')) AS p
   WHERE JSON_VALUE(p, '$.key') = 'ConnectorType') AS connector_type,
  SAFE_CAST(
    REGEXP_REPLACE(
      (SELECT JSON_VALUE(p, '$.value')
       FROM UNNEST(JSON_QUERY_ARRAY(item, '$.additionalProperties')) AS p
       WHERE JSON_VALUE(p, '$.key') = 'Power'),
     r'[^0-9.]',''
    ) AS FLOAT64
  ) AS power_kw,
  (SELECT JSON_VALUE(p, '$.value')
   FROM UNNEST(JSON_QUERY_ARRAY(item, '$.additionalProperties')) AS p
   WHERE JSON_VALUE(p, '$.key') = 'Status') AS status,
  CAST(JSON_VALUE(item, '$.lat') AS FLOAT64) AS lat,
  CAST(JSON_VALUE(item, '$.lon') AS FLOAT64) AS lon,
  item AS raw_json,
  CURRENT_TIMESTAMP() AS snapshot_ts
FROM UNNEST(@json_data) AS item;
