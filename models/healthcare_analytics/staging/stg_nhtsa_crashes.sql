{{
    config(
        materialized='view',
        schema='staging'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw_data', 'manual') }}
)

SELECT
    CAST(year AS INTEGER) AS crash_year,
    CAST(month AS INTEGER) AS crash_month,
    CAST(day AS INTEGER) AS crash_day,
    CAST(hour AS INTEGER) AS crash_hour,
    CAST(minute AS INTEGER) AS crash_minute,
    CAST(day_week AS INTEGER) AS day_of_week,
    
    CAST(state AS INTEGER) AS state_fips,
    statename AS state_name,
    CAST(county AS INTEGER) AS county_fips,
    CAST(rur_urb AS INTEGER) AS rural_urban_code,
    
    st_case AS case_number,
    CAST(fatals AS INTEGER) AS fatalities,
    CAST(drunk_dr AS INTEGER) AS drunk_drivers,
    CAST(persons AS INTEGER) AS total_persons,
    CAST(ve_total AS INTEGER) AS vehicles_involved,
    
    CAST(man_coll AS INTEGER) AS manner_of_collision,
    CAST(lgt_cond AS INTEGER) AS light_condition,
    CAST(weather AS INTEGER) AS weather_condition,
    CAST(harm_ev AS INTEGER) AS first_harmful_event,
    
    timestamp AS crash_timestamp

FROM source
WHERE year IS NOT NULL
  AND state IS NOT NULL
  AND year BETWEEN 2019 AND 2023