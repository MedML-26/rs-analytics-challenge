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
    ROW_NUMBER() OVER (ORDER BY c.crash_year, c.state_name) AS crash_key,
    d.date_key,
    g.geography_key,
    c.crash_month,
    c.crash_day,
    c.crash_hour,
    c.day_of_week,
    c.fatalities,
    c.drunk_drivers,
    c.total_persons,
    c.vehicles_involved,
    c.manner_of_collision,
    c.light_condition,
    c.weather_condition,
    c.rural_urban_code
FROM crashes c
LEFT JOIN geography g ON c.state_name = g.state_code
LEFT JOIN date_dim d ON c.crash_year = d.year