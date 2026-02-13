-- stg_grid_perpetrators.sql
-- ──────────────────────────────────────────────────────────────────
-- Standardize GRID perpetrator data. Extracts primary ideology from
-- ICat_* binary flag columns. One row per perpetrator per incident.
-- ──────────────────────────────────────────────────────────────────

with source as (

    select * from {{ source('raw_grid', 'grid_perps') }}

)

select
    cast(perp_incident_id as varchar)       as perp_incident_id,
    cast(unique_incident_id as varchar)     as event_id,
    cast(perp_cd as varchar)                as perpetrator_code,
    perp_name                               as perpetrator_group,

    -- Extract primary ideology from ICat binary flags
    -- A perpetrator can have multiple; take the first match by priority.
    case
        when try(cast(pcat_rjih as integer)) = 1
            then 'Religious Jihadist'
        when try(cast(pcat_ethn as integer)) = 1
            then 'Ethnonationalist'
        when try(cast(pcat_iran as integer)) = 1
            then 'Iranian-Backed'
        when try(cast(pcat_left as integer)) = 1
            then 'Left-Wing'
        when try(cast(pcat_right as integer)) = 1
            then 'Right-Wing'
        when try(cast(pcat_ana as integer)) = 1
            then 'Anarchist'
        when try(cast(pcat_single as integer)) = 1
            then 'Single Issue'
        when try(cast(pcat_vig as integer)) = 1
            then 'Vigilante'
        when try(cast(pcat_gangs as integer)) = 1
            then 'Gangs'
        when try(cast(pcat_cart as integer)) = 1
            then 'Cartel'
        when try(cast(pcat_rhbjc as integer)) = 1
            then 'Right-Wing BJC'
        else 'Unknown/Other'
    end as primary_ideology,

    'GRID'                                  as source_dataset,
    cast(now() as timestamp)                       as _dbt_loaded_at

from source
