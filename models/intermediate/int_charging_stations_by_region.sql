SELECT
  name as region,
  COUNT(*) AS charger_count
FROM {{ ref('stg_charging_stations_v1') }}
GROUP BY region

