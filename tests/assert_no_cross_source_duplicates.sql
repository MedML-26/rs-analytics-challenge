-- assert_no_cross_source_duplicates.sql
-- ──────────────────────────────────────────────────────────────────
-- Verify that no event appears in both MATCHED and single-source
-- sets in the unified events table. If a GTD event is claimed by
-- a match, it should NOT also appear as GTD_ONLY (and vice versa
-- for GRID). Returns FAILING rows.
-- ──────────────────────────────────────────────────────────────────

with matched_gtd_ids as (
    select distinct gtd_event_id
    from {{ ref('trn_meltt_matches') }}
    where gtd_event_id is not null
),

matched_grid_ids as (
    select distinct grid_event_id
    from {{ ref('trn_meltt_matches') }}
    where grid_event_id is not null
),

unified as (
    select * from {{ ref('trn_unified_events') }}
),

-- GTD events marked as GTD_ONLY but their ID appears in matched set
gtd_duplicates as (
    select
        u.gtd_event_id,
        u.record_type,
        'GTD_ONLY_BUT_IN_MATCHES' as failure_reason
    from unified u
    inner join matched_gtd_ids m on u.gtd_event_id = m.gtd_event_id
    where u.record_type = 'GTD_ONLY'
),

-- GRID events marked as GRID_ONLY but their ID appears in matched set
grid_duplicates as (
    select
        u.grid_event_id       as gtd_event_id,  -- reuse column for union
        u.record_type,
        'GRID_ONLY_BUT_IN_MATCHES' as failure_reason
    from unified u
    inner join matched_grid_ids m on u.grid_event_id = m.grid_event_id
    where u.record_type = 'GRID_ONLY'
)

select * from gtd_duplicates
union all
select * from grid_duplicates
