
{{ config(materialized='view') }}

SELECT
  EXTRACT(YEAR FROM operational_date) AS year,
  COUNT(*) AS station_count
FROM {{ ref('stg_charging_stations_v1') }}
GROUP BY year
