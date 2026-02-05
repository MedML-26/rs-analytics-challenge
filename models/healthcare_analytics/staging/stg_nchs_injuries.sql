{{
    config(
        materialized='view',
        schema='staging'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw_data', 'nchs_raw') }}
),

unnested AS (
    SELECT
        metadata.timestamp AS ingestion_timestamp,
        injury.year,
        injury.state,
        injury.sex,
        injury.age_group,
        injury.race_ethnicity,
        injury.intent,
        injury.mechanism,
        CAST(injury.deaths AS INTEGER) AS deaths,
        CAST(injury.population AS BIGINT) AS population,
        CAST(injury.crude_rate AS DOUBLE) AS crude_rate
    FROM source
    CROSS JOIN UNNEST(data) AS t(injury)
)

SELECT
    year,
    state,
    CASE 
        WHEN sex = 'M' THEN 'Male'
        WHEN sex = 'F' THEN 'Female'
        ELSE sex
    END AS sex,
    age_group,
    race_ethnicity,
    intent AS injury_intent,
    mechanism AS injury_mechanism,
    deaths,
    population,
    crude_rate,
    ingestion_timestamp
FROM unnested
WHERE year IS NOT NULL
  AND state IS NOT NULL