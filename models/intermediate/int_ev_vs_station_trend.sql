SELECT
  ev.registration_year AS year,
  ev.ev_count,
  cs.station_count,
  ROUND(ev.ev_count::FLOAT / NULLIF(cs.station_count, 0), 2) AS ev_per_station
FROM {{ ref('int_ev_registration_by_year') }} ev
LEFT JOIN {{ ref('int_charging_stations_by_year') }} cs
  ON ev.registration_year = cs.year
