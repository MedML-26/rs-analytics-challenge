-- trn_meltt_matches.sql
-- ──────────────────────────────────────────────────────────────────
-- Enrich raw MELTT match output by joining back to both source
-- datasets. Each row = one real-world incident confirmed in both
-- GTD and GRID, with fields selected from the higher-priority source.
--
-- Priority rules (from the MELTT reconciliation procedure):
--   Dates       → GRID (daily collection) over GTD (annual)
--   Coordinates → whichever source has higher precision
--   Attack type → GTD (8 specific categories)
--   Casualties  → GTD (has US-specific breakdowns)
--   Perpetrator → GTD name + GRID ideology
-- ──────────────────────────────────────────────────────────────────

with matches_raw as (

    select * from {{ source('meltt_outputs', 'meltt_matched_pairs') }}

),

gtd as (

    select * from {{ ref('stg_gtd_incidents') }}

),

grid as (

    select * from {{ ref('stg_grid_incidents') }}

),

-- Get the primary perpetrator per GRID incident (lowest perp_cd)
grid_perps_ranked as (

    select
        event_id,
        perpetrator_group   as grid_perpetrator_group,
        primary_ideology    as grid_perpetrator_ideology,
        row_number() over (
            partition by event_id
            order by perpetrator_code
        ) as rn
    from {{ ref('stg_grid_perpetrators') }}

),

grid_perps_primary as (

    select * from grid_perps_ranked where rn = 1

),

enriched as (

    select
        -- ── Match metadata ──
        m.match_id,
        m.spatial_distance_km,
        m.temporal_distance_days,
        m.taxonomy_score,

        -- ── Priority-based field selection ──

        -- DATE: GRID > GTD
        coalesce(grid.event_date, gtd.event_date)               as event_date,
        case when grid.event_date is not null then 'GRID' else 'GTD'
        end as date_provenance,

        -- COORDINATES: GRID if available, else GTD
        coalesce(grid.latitude, gtd.latitude)                    as latitude,
        coalesce(grid.longitude, gtd.longitude)                  as longitude,
        case when grid.latitude is not null then 'GRID' else 'GTD'
        end as coords_provenance,

        -- COUNTRY: GTD naming convention
        coalesce(gtd.country, grid.country)                      as country,

        -- ATTACK TYPE: GTD > GRID
        gtd.attack_type_primary                                  as attack_type_gtd,
        grid.attack_type_primary                                 as attack_type_grid,
        coalesce(gtd.attack_type_primary, grid.attack_type_primary) as attack_type,
        'GTD'                                                    as attack_type_provenance,

        -- WEAPON TYPE: GTD > GRID
        gtd.weapon_type_primary                                  as weapon_type_gtd,
        grid.weapon_type_primary                                 as weapon_type_grid,
        coalesce(gtd.weapon_type_primary, grid.weapon_type_primary) as weapon_type,
        'GTD'                                                    as weapon_type_provenance,

        -- TARGET: GTD only
        gtd.target_type_primary                                  as target_type,
        gtd.target_nationality,

        -- PERPETRATOR: GTD name + GRID ideology
        coalesce(gtd.perpetrator_group, gp.grid_perpetrator_group) as perpetrator_group,
        gp.grid_perpetrator_ideology                             as perpetrator_ideology,

        -- CASUALTIES: GTD > GRID (GTD has US-specific fields)
        coalesce(gtd.total_killed, grid.total_killed)            as total_killed,
        coalesce(gtd.us_killed, grid.us_killed)                  as us_killed,
        coalesce(gtd.total_wounded, grid.total_wounded)          as total_wounded,
        coalesce(gtd.us_wounded, grid.us_wounded)                as us_wounded,
        coalesce(gtd.total_hostages, grid.total_hostages)        as total_hostages,
        coalesce(gtd.us_hostages, grid.us_hostages)              as us_hostages,

        -- INCIDENT FLAGS
        coalesce(gtd.is_suicide, grid.is_suicide)                as is_suicide,
        coalesce(gtd.is_successful, 1)                           as is_successful,
        grid.is_ied,
        grid.is_assassination,

        -- GTD terrorism criteria
        gtd.criteria_1_political,
        gtd.criteria_2_intimidation,
        gtd.criteria_3_outside_ihl,
        gtd.is_doubt_terrorism,

        -- Narrative
        grid.incident_summary,

        -- Traceability IDs
        m.gtd_eventid                                            as gtd_event_id,
        m.grid_unique_incident_id                                as grid_event_id,
        'MATCHED'                                                as record_type,
        2                                                        as n_sources

    from matches_raw m
    left join gtd  on cast(m.gtd_eventid as varchar) = gtd.event_id
    left join grid on cast(m.grid_unique_incident_id as varchar) = grid.event_id
    left join grid_perps_primary gp
        on cast(m.grid_unique_incident_id as varchar) = gp.event_id

)

select * from enriched
