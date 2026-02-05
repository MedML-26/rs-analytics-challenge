{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH injuries AS (
    SELECT * FROM {{ ref('stg_nchs_injuries') }}
),

geography AS (
    SELECT * FROM {{ ref('dim_geography') }}
),

date_dim AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY i.year, i.state) AS injury_key,
    d.date_key,
    g.geography_key,
    i.sex,
    i.age_group,
    i.race_ethnicity,
    i.injury_intent,
    i.injury_mechanism,
    i.deaths,
    i.population,
    i.crude_rate
FROM injuries i
LEFT JOIN geography g ON i.state = g.state_code
LEFT JOIN date_dim d ON CAST(i.year AS INTEGER) = d.year