{{ config(materialized='view') }}

/*
SELECT
  TRY_TO_NUMBER(objectid) AS objectid,
  INITCAP(TRIM(address)) AS address,
  INITCAP(TRIM(name)) AS station_name,
  latitude,  -- no casting needed
  longitude, -- no casting needed
  TRY_TO_NUMBER(numberofconnectors) AS number_of_connectors,
  operator,
  owner
FROM {{ source('staging', 'RAW_EV_CHARGING_STATION') }}
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
*/



SELECT
  OBJECTID,
  NAME,
  LATITUDE,
  LONGITUDE,
  --ST_POINT(LONGITUDE, LATITUDE) AS location,
  TO_DATE(DATEFIRSTOPERATIONAL, 'DD/MM/YYYY') AS operational_date,
  EXTRACT(YEAR FROM TO_DATE(DATEFIRSTOPERATIONAL, 'DD/MM/YYYY')) AS operational_year,
  
  CONNECTOR_COUNT,
  NUMBEROFCONNECTORS,
  CONNECTOR_POWER_RATING,
  CONNECTOR_POWER_TYPE,
  CONNECTOR_TYPE,
  CONNECTOR_STATUS,
  CURRENTTYPE,

  CARPARKCOUNT,
  HASCARPARKCOST,
  HASCHARGINGCOST,
  IS24HOURS,
  HASTOURISTATTRACTION,
  MAXTIMELIMIT,

  OPERATOR,
  OWNER,
  TYPE
FROM {{ source('staging', 'RAW_EV_CHARGING_STATION') }}
WHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
