CREATE OR REPLACE TABLE `serious-music-295616.ev.mart_ev_chargers` AS

WITH station_keys AS (
  SELECT
    s.id AS station_id,
    s.lat,
    s.lon,
    JSON_VALUE(ap, '$.key') AS key,
    JSON_VALUE(ap, '$.value') AS value,
    s.raw_json AS station_raw
  FROM `serious-music-295616.ev.staging_charge_stations` s,
    UNNEST(JSON_QUERY_ARRAY(s.raw_json, '$.additionalProperties')) AS ap
),

station_enriched AS (
  SELECT
    station_id,
    lat,
    lon,
    INITCAP(MAX(IF(key = 'Operator', value, NULL))) AS operator,
    ANY_VALUE(station_raw) AS station_raw
  FROM station_keys
  GROUP BY station_id, lat, lon
),

base AS (
  SELECT
    s.station_id,
    c.id AS connector_id,
    s.lat,
    s.lon,
    IF(
      s.lon BETWEEN -0.23 AND 0.10
      AND s.lat BETWEEN 51.45 AND 51.60,
      TRUE,
      FALSE
    ) AS is_central_london,
    s.operator,
    CASE LOWER(c.status)
      WHEN 'available' THEN 'operational'
      WHEN 'charging' THEN 'in_use'
      WHEN 'occupied' THEN 'in_use'
      WHEN 'unavailable' THEN 'out_of_service'
      ELSE 'unknown'
    END AS status,
    c.power_kw,
    CASE
      WHEN c.power_kw >= 50 THEN 'rapid'
      WHEN c.power_kw >= 22 THEN 'fast'
      WHEN c.power_kw >= 7 THEN 'slow'
      ELSE 'unknown'
    END AS speed_category,
    CURRENT_TIMESTAMP() AS snapshot_ts,
    s.station_raw,
    c.raw_json AS connector_raw
  FROM station_enriched s
  LEFT JOIN `serious-music-295616.ev.staging_charge_connectors` c
    ON REPLACE(s.station_id, 'EsbChargePoint_', '') =
       REPLACE(c.station_ref, 'EsbChargePoint_', '')
)

SELECT *
FROM base
WHERE is_central_london = TRUE;