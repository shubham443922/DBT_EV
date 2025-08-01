/*{{ config(materialized='view') }}

SELECT
  TRY_TO_NUMBER(objectid) AS objectid,
  INITCAP(TRIM(tla)) AS region,
  INITCAP(TRIM(make)) AS make,
  INITCAP(TRIM(model)) AS model,
  UPPER(TRIM(motive_power)) AS motive_power,
  TRY_TO_NUMBER(first_nz_registration_year) AS registration_year,
  TRY_TO_NUMBER(first_nz_registration_month) AS registration_month,
  TO_DATE(
    CONCAT(
      CAST(CAST(first_nz_registration_year AS INTEGER) AS STRING), '-',
      LPAD(CAST(CAST(first_nz_registration_month AS INTEGER) AS STRING), 2, '0'),
      '-01'
    )
  ) AS registration_date
FROM {{ source('staging', 'RAW_EV_REGISTER') }}
WHERE motive_power ILIKE '%ELECTRIC%'
*/

{{ config(materialized='view') }}

SELECT
  OBJECTID,
  MAKE,
  MODEL,
  SUBMODEL,
  MOTIVE_POWER,
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
WHERE MOTIVE_POWER ILIKE '%electric%' OR ALTERNATIVE_MOTIVE_POWER ILIKE '%electric%'
