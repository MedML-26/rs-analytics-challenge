{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH injury_years AS (
    SELECT DISTINCT CAST(year AS INTEGER) AS year
    FROM {{ ref('stg_nchs_injuries') }}
),

crash_years AS (
    SELECT DISTINCT crash_year AS year
    FROM {{ ref('stg_nhtsa_crashes') }}
),

all_years AS (
    SELECT year FROM injury_years
    UNION
    SELECT year FROM crash_years
)

SELECT
    year AS date_key,
    year,
    CASE 
        WHEN year >= 2020 THEN 'Recent (2020+)'
        ELSE 'Historical (Pre-2020)'
    END AS period
FROM all_years
WHERE year IS NOT NULL
ORDER BY year