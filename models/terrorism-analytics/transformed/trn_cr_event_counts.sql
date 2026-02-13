-- trn_cr_event_counts.sql
-- ──────────────────────────────────────────────────────────────────
-- Compute raw counts for capture-recapture estimation:
--   n1 = GTD events in stratum
--   n2 = GRID events in stratum
--   m  = MELTT-matched events in stratum
--
-- Four granularity levels: GLOBAL, COUNTRY, YEAR, COUNTRY_YEAR.
-- All restricted to the 2018–2020 overlap period.
-- ──────────────────────────────────────────────────────────────────

with gtd as (

    select event_id, country, year(event_date) as event_year
    from {{ ref('stg_gtd_incidents') }}
    where event_date >= date '{{ var("terrorism_overlap_start") }}'
      and event_date <= date '{{ var("terrorism_overlap_end") }}'

),

grid as (

    select event_id, country, year(event_date) as event_year
    from {{ ref('stg_grid_incidents') }}
    where event_date >= date '{{ var("terrorism_overlap_start") }}'
      and event_date <= date '{{ var("terrorism_overlap_end") }}'

),

matches as (

    select gtd_event_id, grid_event_id, country, year(event_date) as event_year
    from {{ ref('trn_meltt_matches') }}
    where event_date >= date '{{ var("terrorism_overlap_start") }}'
      and event_date <= date '{{ var("terrorism_overlap_end") }}'

),

-- ── Global counts ──
global_counts as (
    select
        'GLOBAL'                            as stratum_type,
        'All Middle East'                   as stratum_key,
        cast(null as varchar)               as country,
        cast(null as integer)               as event_year,
        (select count(*) from gtd)          as n1_gtd,
        (select count(*) from grid)         as n2_grid,
        (select count(*) from matches)      as m_matched
),

-- ── Country-level counts ──
country_counts as (
    select
        'COUNTRY'                           as stratum_type,
        g.country                           as stratum_key,
        g.country,
        cast(null as integer)               as event_year,
        g.n1_gtd,
        coalesce(gr.n2_grid, 0)             as n2_grid,
        coalesce(m.m_matched, 0)            as m_matched
    from (select country, count(*) as n1_gtd from gtd group by country) g
    left join (select country, count(*) as n2_grid from grid group by country) gr
        on g.country = gr.country
    left join (select country, count(*) as m_matched from matches group by country) m
        on g.country = m.country
),

-- ── Year-level counts ──
year_counts as (
    select
        'YEAR'                              as stratum_type,
        cast(g.event_year as varchar)       as stratum_key,
        cast(null as varchar)               as country,
        g.event_year,
        g.n1_gtd,
        coalesce(gr.n2_grid, 0)             as n2_grid,
        coalesce(m.m_matched, 0)            as m_matched
    from (select event_year, count(*) as n1_gtd from gtd group by event_year) g
    left join (select event_year, count(*) as n2_grid from grid group by event_year) gr
        on g.event_year = gr.event_year
    left join (select event_year, count(*) as m_matched from matches group by event_year) m
        on g.event_year = m.event_year
),

-- ── Country × Year counts (most granular) ──
country_year_counts as (
    select
        'COUNTRY_YEAR'                      as stratum_type,
        g.country || '_' || cast(g.event_year as varchar) as stratum_key,
        g.country,
        g.event_year,
        g.n1_gtd,
        coalesce(gr.n2_grid, 0)             as n2_grid,
        coalesce(m.m_matched, 0)            as m_matched
    from (
        select country, event_year, count(*) as n1_gtd
        from gtd group by country, event_year
    ) g
    left join (
        select country, event_year, count(*) as n2_grid
        from grid group by country, event_year
    ) gr on g.country = gr.country and g.event_year = gr.event_year
    left join (
        select country, event_year, count(*) as m_matched
        from matches group by country, event_year
    ) m on g.country = m.country and g.event_year = m.event_year
)

select * from global_counts
union all
select * from country_counts
union all
select * from year_counts
union all
select * from country_year_counts
