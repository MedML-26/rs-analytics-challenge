{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH crashes AS (
    SELECT * FROM {{ ref('stg_nhtsa_crashes') }}
),

geography AS (
    SELECT * FROM {{ ref('dim_geography') }}
),

date_dim AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY c.crash_year, c.state_name, c.case_number) AS crash_key,
    d.date_key,
    g.geography_key,
    
    -- Date/Time
    c.crash_month,
    c.crash_month_name,
    c.crash_day,
    c.crash_hour,
    c.day_of_week,
    c.day_of_week_name,
    
    -- Location
    c.state_name,
    c.county_name,
    c.city_name,
    c.rural_urban_code,
    c.rural_urban_name,
    c.latitude,
    c.longitude,
    
    -- Crash metrics
    c.fatalities,
    c.drunk_drivers,
    c.total_persons,
    c.vehicles_involved,
    c.pedestrians,
    
    -- Crash characteristics
    c.manner_of_collision,
    c.manner_of_collision_name,
    c.light_condition,
    c.light_condition_name,
    c.weather_condition,
    c.weather_condition_name,
    c.first_harmful_event,
    c.first_harmful_event_name,
    c.work_zone,
    c.work_zone_name,
    c.school_bus_related,
    c.school_bus_related_name
    
FROM crashes c
LEFT JOIN geography g ON c.state_name = g.state_code
LEFT JOIN date_dim d ON c.crash_year = d.year