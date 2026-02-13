-- stg_grid_sources.sql
-- ──────────────────────────────────────────────────────────────────
-- Standardize GRID source/citation data for provenance tracking.
-- ──────────────────────────────────────────────────────────────────

select
    cast(unique_incident_id as varchar)     as event_id,
    headline,
    try(date_parse(publication_date, '%Y-%m-%d')) as publication_date,
    publication                      as media_outlet,
    'GRID'                                  as source_dataset,
    current_timestamp                       as _dbt_loaded_at

from {{ source('raw_grid', 'grid_sources') }}
