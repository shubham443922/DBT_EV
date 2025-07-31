{{ config(materialized='view') }}

SELECT
  OBJECTID,
  TLA AS region,
  LOWER(MOTIVE_POWER) AS motive_power,
  MAKE,
  MODEL,
  VEHICLE_TYPE,
  VEHICLE_USAGE,
  TO_DATE(
  CAST(FIRST_NZ_REGISTRATION_YEAR AS INT) || '-' ||
  LPAD(CAST(FIRST_NZ_REGISTRATION_MONTH AS INT), 2, '0') || '-01',
  'YYYY-MM-DD'
) AS registration_date
FROM {{ source('staging', 'RAW_EV_REGISTER') }}
WHERE LOWER(MOTIVE_POWER) LIKE '%electric%'
