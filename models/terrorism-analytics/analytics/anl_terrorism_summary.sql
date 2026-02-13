-- anl_terrorism_summary.sql
-- ──────────────────────────────────────────────────────────────────
-- Primary analytical table: terrorism events aggregated by country
-- and year with capture-recapture estimates joined. This is the
-- main table for dashboards and reporting.
-- ──────────────────────────────────────────────────────────────────

with unified as (

    select * from {{ ref('trn_unified_events') }}

),

by_country_year as (

    select
        country,
        event_year,
        count(*)                                                        as total_events,
        sum(case when record_type = 'MATCHED' then 1 else 0 end)       as matched_events,
        sum(case when record_type = 'GTD_ONLY' then 1 else 0 end)      as gtd_only_events,
        sum(case when record_type = 'GRID_ONLY' then 1 else 0 end)     as grid_only_events,

        -- Casualties
        sum(coalesce(total_killed, 0))                                  as total_killed,
        sum(coalesce(total_wounded, 0))                                 as total_wounded,
        sum(coalesce(us_killed, 0))                                     as us_killed,
        sum(coalesce(us_wounded, 0))                                    as us_wounded,

        -- Characteristics
        sum(case when is_suicide = 1 then 1 else 0 end)                as suicide_attacks,
        count(distinct perpetrator_group)                                as unique_perpetrators,
        avg(case when n_sources = 2 then 1.0 else 0.0 end)             as pct_cross_validated

    from unified
    where event_year is not null
    group by country, event_year

),

with_capture_recapture as (

    select
        cy.*,
        cr.n_hat                as cr_estimated_total,
        cr.ci_lower             as cr_ci_lower,
        cr.ci_upper             as cr_ci_upper,
        cr.n_missed             as cr_estimated_missed,
        cr.coverage_combined    as cr_coverage,
        cr.dependence_ratio     as cr_dependence_ratio,
        cr.interpretation       as cr_interpretation
    from by_country_year cy
    left join {{ ref('trn_cr_chapman_estimates') }} cr
        on cy.country = cr.country
       and cy.event_year = cr.event_year
       and cr.stratum_type = 'COUNTRY_YEAR'

)

select
    *,
    -- Derived metrics
    case
        when total_events > 0
        then round(cast(total_killed as double) / total_events, 2)
        else 0.0
    end as avg_fatalities_per_event,

    case
        when total_events > 0
        then round(cast(matched_events as double) / total_events, 3)
        else 0.0
    end as cross_validation_rate,

    current_timestamp as _built_at

from with_capture_recapture
