-- trn_unified_events.sql
-- ──────────────────────────────────────────────────────────────────
-- Core analytical table: every deduplicated terrorism incident in
-- the Middle East. Combines three sets:
--   1. MATCHED   — in both GTD and GRID (from trn_meltt_matches)
--   2. GTD_ONLY  — in GTD but not matched to any GRID event
--   3. GRID_ONLY — in GRID but not matched to any GTD event
-- ──────────────────────────────────────────────────────────────────

{{ config(
    partition_by=['event_year']
) }}

with matches as (

    select * from {{ ref('trn_meltt_matches') }}

),

gtd as (

    select * from {{ ref('stg_gtd_incidents') }}

),

grid as (

    select * from {{ ref('stg_grid_incidents') }}

),

grid_perps_ranked as (

    select
        event_id,
        perpetrator_group   as grid_perpetrator_group,
        primary_ideology    as grid_perpetrator_ideology,
        row_number() over (partition by event_id order by perpetrator_code) as rn
    from {{ ref('stg_grid_perpetrators') }}

),

grid_perps_primary as (

    select * from grid_perps_ranked where rn = 1

),

-- IDs already claimed by matches → exclude from single-source sets
matched_gtd_ids as (
    select distinct gtd_event_id from matches where gtd_event_id is not null
),
matched_grid_ids as (
    select distinct grid_event_id from matches where grid_event_id is not null
),

-- ── GTD-only events ──
gtd_only as (

    select
        cast(null as varchar)           as match_id,
        cast(null as double)            as spatial_distance_km,
        cast(null as integer)           as temporal_distance_days,
        cast(null as double)            as taxonomy_score,
        g.event_date,
        'GTD'                           as date_provenance,
        g.latitude,
        g.longitude,
        'GTD'                           as coords_provenance,
        g.country,
        g.attack_type_primary           as attack_type_gtd,
        cast(null as varchar)           as attack_type_grid,
        g.attack_type_primary           as attack_type,
        'GTD'                           as attack_type_provenance,
        g.weapon_type_primary           as weapon_type_gtd,
        cast(null as varchar)           as weapon_type_grid,
        g.weapon_type_primary           as weapon_type,
        'GTD'                           as weapon_type_provenance,
        g.target_type_primary           as target_type,
        g.target_nationality,
        g.perpetrator_group,
        cast(null as varchar)           as perpetrator_ideology,
        g.total_killed,
        g.us_killed,
        g.total_wounded,
        g.us_wounded,
        g.total_hostages,
        g.us_hostages,
        g.is_suicide,
        g.is_successful,
        cast(null as integer)           as is_ied,
        cast(null as integer)           as is_assassination,
        g.criteria_1_political,
        g.criteria_2_intimidation,
        g.criteria_3_outside_ihl,
        g.is_doubt_terrorism,
        cast(null as varchar)           as incident_summary,
        g.event_id                      as gtd_event_id,
        cast(null as varchar)           as grid_event_id,
        'GTD_ONLY'                      as record_type,
        1                               as n_sources
    from gtd g
    left join matched_gtd_ids m on g.event_id = m.gtd_event_id
    where m.gtd_event_id is null

),

-- ── GRID-only events ──
grid_only as (

    select
        cast(null as varchar)           as match_id,
        cast(null as double)            as spatial_distance_km,
        cast(null as integer)           as temporal_distance_days,
        cast(null as double)            as taxonomy_score,
        g.event_date,
        'GRID'                          as date_provenance,
        g.latitude,
        g.longitude,
        'GRID'                          as coords_provenance,
        g.country,
        cast(null as varchar)           as attack_type_gtd,
        g.attack_type_primary           as attack_type_grid,
        g.attack_type_primary           as attack_type,
        'GRID'                          as attack_type_provenance,
        cast(null as varchar)           as weapon_type_gtd,
        g.weapon_type_primary           as weapon_type_grid,
        g.weapon_type_primary           as weapon_type,
        'GRID'                          as weapon_type_provenance,
        cast(null as varchar)           as target_type,
        cast(null as varchar)           as target_nationality,
        coalesce(gp.grid_perpetrator_group, 'Unknown') as perpetrator_group,
        gp.grid_perpetrator_ideology    as perpetrator_ideology,
        g.total_killed,
        g.us_killed,
        g.total_wounded,
        g.us_wounded,
        g.total_hostages,
        g.us_hostages,
        g.is_suicide,
        cast(null as integer)           as is_successful,
        g.is_ied,
        g.is_assassination,
        cast(null as integer)           as criteria_1_political,
        cast(null as integer)           as criteria_2_intimidation,
        cast(null as integer)           as criteria_3_outside_ihl,
        cast(null as integer)           as is_doubt_terrorism,
        g.incident_summary,
        cast(null as varchar)           as gtd_event_id,
        g.event_id                      as grid_event_id,
        'GRID_ONLY'                     as record_type,
        1                               as n_sources
    from grid g
    left join matched_grid_ids m on g.event_id = m.grid_event_id
    left join grid_perps_primary gp on g.event_id = gp.event_id
    where m.grid_event_id is null

),

-- ── Combine all three sets ──
unified as (

    select * from matches
    union all
    select * from gtd_only
    union all
    select * from grid_only

)

select
    *,
    year(event_date)  as event_year,
    month(event_date) as event_month
from unified
