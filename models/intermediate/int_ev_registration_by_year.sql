{{ config(materialized='view') }}

SELECT
  registration_year,
  COUNT(*) AS ev_count
FROM {{ ref('stg_ev_registrations_v1') }}
GROUP BY registration_year
