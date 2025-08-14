SELECT
  (select count(*) from  (select distinct OBJECTID from {{ ref('int_ev_registration_by_year_v1')}} where motive_power_category = 'Electric')) AS total_ev, 
  (SELECT COUNT(*) FROM (select distinct OBJECTID from {{ ref('int_charging_stations_by_year_v1') }})) AS total_stations, 
  ROUND(total_ev::FLOAT / NULLIF(total_stations, 0), 2) AS ev_per_station

