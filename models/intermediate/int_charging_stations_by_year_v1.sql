SELECT
  OBJECTID,
  CONNECTOR_STATUS,
  EXTRACT(YEAR FROM operational_date) AS year,
  FROM {{ ref('stg_charging_stations_v1') }}

