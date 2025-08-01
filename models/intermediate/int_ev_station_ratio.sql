SELECT
  (SELECT COUNT(*) FROM {{ ref('stg_ev_registrations_v1') }}) AS total_ev,
  (SELECT COUNT(*) FROM {{ ref('stg_charging_stations_v1') }}) AS total_stations,
  ROUND(total_ev::FLOAT / NULLIF(total_stations, 0), 2) AS ev_per_station
