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
        when try(cast("icat_religious_jihadist" as integer)) = 1
            then 'Religious Jihadist'
        when try(cast("icat_ethnonationalist" as integer)) = 1
            then 'Ethnonationalist'
        when try(cast("icat_iranian-backed" as integer)) = 1
            then 'Iranian-Backed'
        when try(cast("icat_left-wing" as integer)) = 1
            then 'Left-Wing'
        when try(cast("icat_right-wing" as integer)) = 1
            then 'Right-Wing'
        when try(cast("icat_anarchist" as integer)) = 1
            then 'Anarchist'
        when try(cast("icat_single_issue" as integer)) = 1
            then 'Single Issue'
        else 'Unknown/Other'
    end as primary_ideology,

    'GRID'                                  as source_dataset,
    current_timestamp                       as _dbt_loaded_at

from source
