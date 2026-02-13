-- overlap_period_calibration.sql
-- ──────────────────────────────────────────────────────────────────
-- Ad-hoc analysis: compare GTD vs GRID monthly event counts during
-- the 2018–2020 overlap period. The gtd_to_grid_ratio can serve as
-- a correction factor when creating GTD-equivalent time series for
-- the GRID-only period (2021+).
--
-- This is an ANALYSIS file — run via `dbt compile` or directly in
-- Athena. It does not create a table.
-- ──────────────────────────────────────────────────────────────────

with gtd_monthly as (

    select
        country,
        year(event_date)    as event_year,
        month(event_date)   as event_month,
        count(*)            as gtd_events
    from {{ ref('stg_gtd_incidents') }}
    where event_date >= date '{{ var("terrorism_overlap_start") }}'
      and event_date <= date '{{ var("terrorism_overlap_end") }}'
    group by country, year(event_date), month(event_date)

),

grid_monthly as (

    select
        country,
        year(event_date)    as event_year,
        month(event_date)   as event_month,
        count(*)            as grid_events
    from {{ ref('stg_grid_incidents') }}
    where event_date >= date '{{ var("terrorism_overlap_start") }}'
      and event_date <= date '{{ var("terrorism_overlap_end") }}'
    group by country, year(event_date), month(event_date)

)

select
    coalesce(g.country, r.country)          as country,
    coalesce(g.event_year, r.event_year)    as event_year,
    coalesce(g.event_month, r.event_month)  as event_month,
    coalesce(g.gtd_events, 0)               as gtd_events,
    coalesce(r.grid_events, 0)              as grid_events,
    coalesce(g.gtd_events, 0)
        - coalesce(r.grid_events, 0)        as difference,
    case
        when coalesce(r.grid_events, 0) > 0
        then round(
            cast(coalesce(g.gtd_events, 0) as double)
            / r.grid_events, 3)
        else null
    end as gtd_to_grid_ratio

from gtd_monthly g
full outer join grid_monthly r
    on g.country = r.country
   and g.event_year = r.event_year
   and g.event_month = r.event_month

order by country, event_year, event_month
