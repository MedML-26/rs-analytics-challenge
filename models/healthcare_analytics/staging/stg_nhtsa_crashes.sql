{{
    config(
        materialized='view',
        schema='staging'
    )
}}

SELECT
    -- Crash identifiers
    year AS crash_year,
    month AS crash_month,
    monthname AS crash_month_name,
    day AS crash_day,
    dayname AS crash_day_name,
    hour AS crash_hour,
    hourname AS crash_hour_name,
    minute AS crash_minute,
    day_week AS day_of_week,
    day_weekname AS day_of_week_name,
    
    -- Location
    state AS state_fips,
    statename AS state_name,
    county AS county_fips,
    countyname AS county_name,
    city AS city_fips,
    cityname AS city_name,
    rur_urb AS rural_urban_code,
    rur_urbname AS rural_urban_name,
    latitude,
    longitud AS longitude,
    
    -- Crash details
    st_case AS case_number,
    fatals AS fatalities,
    drunk_dr AS drunk_drivers,
    persons AS total_persons,
    ve_total AS vehicles_involved,
    ve_forms AS vehicle_forms,
    peds AS pedestrians,
    
    -- Crash characteristics
    man_coll AS manner_of_collision,
    man_collname AS manner_of_collision_name,
    lgt_cond AS light_condition,
    lgt_condname AS light_condition_name,
    weather AS weather_condition,
    weathername AS weather_condition_name,
    harm_ev AS first_harmful_event,
    harm_evname AS first_harmful_event_name,
    
    -- Additional factors
    rel_road AS relation_to_road,
    rel_roadname AS relation_to_road_name,
    wrk_zone AS work_zone,
    wrk_zonename AS work_zone_name,
    sch_bus AS school_bus_related,
    sch_busname AS school_bus_related_name

FROM {{ source('raw_data', 'manual') }}
WHERE year IS NOT NULL
  AND year BETWEEN 2019 AND 2023