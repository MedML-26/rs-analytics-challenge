-- anl_capture_recapture_summary.sql
-- ──────────────────────────────────────────────────────────────────
-- Executive summary of capture-recapture analysis. Combines the
-- global estimate with country rankings and sensitivity analysis
-- to identify where data gaps are largest.
-- ──────────────────────────────────────────────────────────────────

with estimates as (

    select * from {{ ref('trn_cr_chapman_estimates') }}

),

sensitivity as (

    select * from {{ source('meltt_outputs', 'meltt_sensitivity_grid') }}

),

-- ── Global estimate (single row) ──
global_estimate as (

    select
        n1_gtd,
        n2_grid,
        m_matched,
        n_hat,
        se_hat,
        ci_lower,
        ci_upper,
        n_observed,
        n_missed,
        coverage_gtd,
        coverage_grid,
        coverage_combined,
        dependence_ratio,
        interpretation
    from estimates
    where stratum_type = 'GLOBAL'

),

-- ── Country rankings by data gap size ──
country_rankings as (

    select
        country,
        n1_gtd,
        n2_grid,
        m_matched,
        n_hat,
        ci_lower,
        ci_upper,
        n_missed,
        coverage_combined,
        dependence_ratio,
        row_number() over (order by n_missed desc) as gap_rank
    from estimates
    where stratum_type = 'COUNTRY'
      and n_hat is not null

),

-- ── Sensitivity analysis: how does N̂ change with MELTT window? ──
sensitivity_impact as (

    select
        s.temporal_window_days,
        s.spatial_window_km,
        s.n_matches,
        s.precision,
        s.recall,
        -- Recompute Chapman with this window's match count
        case
            when s.n_matches = 0 then null
            else (
                (cast(ge.n1_gtd + 1 as double) * cast(ge.n2_grid + 1 as double))
                / cast(s.n_matches + 1 as double) - 1.0
            )
        end as n_hat_at_window,
        case
            when s.n_matches = 0 or ge.n_hat is null or ge.n_hat = 0 then null
            else ((
                (cast(ge.n1_gtd + 1 as double) * cast(ge.n2_grid + 1 as double))
                / cast(s.n_matches + 1 as double) - 1.0
            ) - ge.n_hat) / ge.n_hat * 100
        end as pct_change_from_default
    from sensitivity s
    cross join global_estimate ge

)

-- ── Output: global summary row ──
select
    'GLOBAL_SUMMARY'                as report_section,
    ge.n1_gtd,
    ge.n2_grid,
    ge.m_matched,
    ge.n_hat                        as estimated_total_events,
    ge.ci_lower,
    ge.ci_upper,
    ge.n_observed,
    ge.n_missed                     as estimated_unreported,
    ge.coverage_combined,
    ge.dependence_ratio,
    ge.interpretation,

    -- Country with largest data gap
    (select count(*) from country_rankings)          as countries_analyzed,
    (select max(country) from country_rankings
     where gap_rank = 1)                             as largest_gap_country,
    (select max(n_missed) from country_rankings
     where gap_rank = 1)                             as largest_gap_missed,

    -- Sensitivity range
    (select min(n_hat_at_window) from sensitivity_impact
     where n_hat_at_window is not null)              as sensitivity_n_hat_min,
    (select max(n_hat_at_window) from sensitivity_impact
     where n_hat_at_window is not null)              as sensitivity_n_hat_max,

    current_timestamp                                as _built_at

from global_estimate ge
