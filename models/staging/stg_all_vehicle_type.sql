
SELECT
  OBJECTID,
  MAKE,
  MODEL,
  SUBMODEL,
  motive_power,
  CASE
    WHEN motive_power ILIKE '%ELECTRIC%' THEN 'Electric'
    WHEN motive_power ILIKE '%HYBRID%' THEN 'Hybrid'
    WHEN motive_power ILIKE '%PETROL%' THEN 'Petrol'
    WHEN motive_power ILIKE '%DIESEL%' THEN 'Diesel'
    WHEN motive_power ILIKE '%LPG%' OR motive_power ILIKE '%CNG%' THEN 'Other'
    ELSE 'Other'
  END AS motive_power_category,
  ALTERNATIVE_MOTIVE_POWER,
  TRY_CAST(FIRST_NZ_REGISTRATION_YEAR AS INT) AS registration_year,
  TRY_CAST(FIRST_NZ_REGISTRATION_MONTH AS INT) AS registration_month,
  TRY_CAST(VEHICLE_YEAR AS INT) AS vehicle_year,
  DATE_FROM_PARTS(
    TRY_CAST(FIRST_NZ_REGISTRATION_YEAR AS INT),
    TRY_CAST(FIRST_NZ_REGISTRATION_MONTH AS INT),
    1
  ) AS registration_date,
  CLASS,
  VEHICLE_TYPE,
  VEHICLE_USAGE,
  BODY_TYPE,
  TRANSMISSION_TYPE,
  WIDTH,
  HEIGHT,
  VDAM_WEIGHT,
  GROSS_VEHICLE_MASS,
  POWER_RATING,
  CC_RATING,
  FC_COMBINED,
  FC_URBAN,
  FC_EXTRA_URBAN,
  ORIGINAL_COUNTRY,
  PREVIOUS_COUNTRY,
  IMPORT_STATUS,
  NZ_ASSEMBLED
FROM {{ source('staging', 'RAW_EV_REGISTER') }}

--MY change on 14-08-2025




