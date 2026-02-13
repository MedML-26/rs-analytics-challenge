-- stg_grid_incidents.sql
-- ──────────────────────────────────────────────────────────────────
-- Standardize GRID incident data: parse dates, normalize GENC country
-- names to GTD convention via seed mapping, extract primary tactic
-- and weapon from binary flag columns.
-- ──────────────────────────────────────────────────────────────────

with source as (

    select * from {{ source('raw_gtd', 'gtd_events') }}

),

country_mapping as (

    select * from {{ ref('seed_country_name_mapping') }}

),

dates_parsed as (

    select
        s.*,
        -- GRID date format varies between exports. Try all known formats.
        coalesce(
            try(date_parse(s.incident_date, '%d/%m/%Y')),
            try(date_parse(s.incident_date, '%Y-%m-%d')),
            try(date_parse(s.incident_date, '%m/%d/%Y')),
            try(cast(s.incident_date as date))
        ) as event_date
    from source s

),

with_country_norm as (

    select
        d.*,
        -- Map GENC formal names → GTD informal names for consistent joins
        coalesce(cm.gtd_country_name, d.country_genc_txt) as country_normalized
    from dates_parsed d
    left join country_mapping cm
        on d.country_genc_txt = cm.grid_country_name

),

middle_east_filtered as (

    select *
    from with_country_norm
    where country_normalized in ({{ var_list('me_countries') }})

),

taxonomy_extracted as (

    -- GRID encodes tactics/weapons as binary flag columns (0/1).
    -- Extract the PRIMARY tactic and weapon into single labels.
    select
        *,
        -- Tactic: priority order EX > AS > TR > CO > CV > UN
        case
            when cast(tactic_lvl1_ex as integer) = 1 then 'Explosion'
            when cast(tactic_lvl1_as as integer) = 1 then 'Assault'
            when cast(tactic_lvl1_tr as integer) = 1 then 'Threat/Hoax'
            when cast(tactic_lvl1_co as integer) = 1 then 'Concentration'
            when cast(tactic_lvl1_cv as integer) = 1 then 'Unconventional'
            when cast(tactic_lvl1_un as integer) = 1 then 'Unknown'
            else 'Unknown'
        end as tactic_primary,

        -- Weapon: priority order EX > FI > IN > IM > ML > UA > OT > UN
        case
            when cast(weapon_lvl1_ex as integer) = 1 then 'Explosives'
            when cast(weapon_lvl1_fi as integer) = 1 then 'Firearms'
            when cast(weapon_lvl1_in as integer) = 1 then 'Incendiary'
            when cast(weapon_lvl1_im as integer) = 1 then 'Impact'
            when cast(weapon_lvl1_ml as integer) = 1 then 'Missile/Launcher'
            when cast(weapon_lvl1_ua as integer) = 1 then 'Unmanned Aerial'
            when cast(weapon_lvl1_ot as integer) = 1 then 'Other'
            when cast(weapon_lvl1_un as integer) = 1 then 'Unknown'
            else 'Unknown'
        end as weapon_primary
    from middle_east_filtered

)

select
    -- Primary key
    cast(unique_incident_id as varchar)     as event_id,
    cast(incident_id as varchar)            as incident_group_id,
    cast(incident_seq as varchar)           as incident_sequence,

    -- Date
    event_date,
    cast(incident_year as integer)          as event_year,
    cast(incident_month as integer)         as event_month,
    cast(incident_day as integer)           as event_day,

    -- Geography
    country_normalized                      as country,
    country_genc_txt                        as country_genc,
    country_genc                            as country_genc_code,
    region_txt                              as region,
    stateprovince_genc_txt                  as state_province,
    city,
    cast(latitude as double)               as latitude,
    cast(longitude as double)              as longitude,

    -- Attack/weapon (extracted from binary flags)
    tactic_primary                          as attack_type_primary,
    weapon_primary                          as weapon_type_primary,

    -- Incident flags
    cast(is_suicide as integer)             as is_suicide,
    cast(is_assassination as integer)       as is_assassination,
    cast(is_ied as integer)                 as is_ied,
    cast(is_multi_day as integer)           as is_multi_day,
    cast(is_multi_location as integer)      as is_multi_location,
    cast(claimed as integer)                as is_claimed,

    -- Casualties
    cast(num_killed as double)              as total_killed,
    cast(num_killed_us as double)           as us_killed,
    cast(num_killed_perp as double)         as perpetrators_killed,
    cast(num_wounded as double)             as total_wounded,
    cast(num_wounded_us as double)          as us_wounded,
    cast(num_hostkid as double)             as total_hostages,
    cast(num_hostkid_us as double)          as us_hostages,

    -- Narrative
    summary                                 as incident_summary,

    -- Provenance
    'GRID'                                  as source_dataset,
    current_timestamp                       as _dbt_loaded_at

from taxonomy_extracted
