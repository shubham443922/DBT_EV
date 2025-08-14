SELECT
  OBJECTID,
  registration_year,
  motive_power,
  motive_power_category,
FROM {{ ref('stg_all_vehicle_type') }}

