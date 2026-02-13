-- anl_us_attack_analysis.sql
-- ──────────────────────────────────────────────────────────────────
-- US-targeted attack summary by country and year. Includes detection
-- pathway breakdown, perpetrator analysis, and data source provenance.
-- ──────────────────────────────────────────────────────────────────

with us_events as (

    select * from {{ ref('trn_us_targeted_events') }}

)

select
    country,
    event_year,

    -- Event counts
    count(*)                                                    as us_targeted_events,

    -- US casualties
    sum(coalesce(us_killed, 0))                                 as us_killed,
    sum(coalesce(us_wounded, 0))                                as us_wounded,
    sum(coalesce(us_hostages, 0))                               as us_hostages,

    -- Total casualties (all nationalities)
    sum(coalesce(total_killed, 0))                              as total_killed,
    sum(coalesce(total_wounded, 0))                             as total_wounded,

    -- Attack characteristics
    sum(case when is_suicide = 1 then 1 else 0 end)            as suicide_attacks,
    count(distinct perpetrator_group)                            as unique_perpetrators,

    -- Detection pathway transparency
    sum(case when is_us_by_nationality then 1 else 0 end)      as detected_by_nationality,
    sum(case when is_us_by_casualties then 1 else 0 end)       as detected_by_casualties,
    sum(case when is_us_military_target then 1 else 0 end)     as detected_military_target,
    sum(case when is_us_by_narrative then 1 else 0 end)        as detected_by_narrative,

    -- Data source breakdown
    sum(case when record_type = 'MATCHED' then 1 else 0 end)   as from_both_sources,
    sum(case when record_type = 'GTD_ONLY' then 1 else 0 end)  as from_gtd_only,
    sum(case when record_type = 'GRID_ONLY' then 1 else 0 end) as from_grid_only,

    -- Derived
    case
        when count(*) > 0
        then round(
            cast(sum(coalesce(us_killed, 0)) + sum(coalesce(us_wounded, 0)) as double)
            / count(*), 2)
        else 0.0
    end as avg_us_casualties_per_event,

    current_timestamp as _built_at

from us_events
where event_year is not null
group by country, event_year
