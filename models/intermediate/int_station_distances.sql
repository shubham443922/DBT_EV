SELECT
  a.OBJECTID AS station_a_id,
  b.OBJECTID AS station_b_id,
  ROUND(
  ST_DISTANCE(ST_POINT(a.LONGITUDE, a.LATITUDE), ST_POINT(b.LONGITUDE, b.LATITUDE)) / 1000,  2) AS distance_km
FROM {{ ref('stg_charging_stations_v1') }} a
JOIN {{ ref('stg_charging_stations_v1') }} b
  ON a.OBJECTID != b.OBJECTID
WHERE ST_DISTANCE(ST_POINT(a.LONGITUDE, a.LATITUDE), ST_POINT(b.LONGITUDE, b.LATITUDE)) < 100000
