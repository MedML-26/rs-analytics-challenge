{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH nchs_states AS (
    SELECT DISTINCT state AS state_code
    FROM {{ ref('stg_nchs_injuries') }}
),

nhtsa_states AS (
    SELECT DISTINCT state_name
    FROM {{ ref('stg_nhtsa_crashes') }}
),

combined AS (
    SELECT state_code FROM nchs_states
    UNION
    SELECT state_name AS state_code FROM nhtsa_states
)

SELECT
    ROW_NUMBER() OVER (ORDER BY state_code) AS geography_key,
    state_code,
    state_code AS state_name
FROM combined
WHERE state_code IS NOT NULL
ORDER BY state_code