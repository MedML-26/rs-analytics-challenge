-- trn_us_targeted_events.sql
-- ──────────────────────────────────────────────────────────────────
-- Filter unified events to attacks targeting US soldiers, personnel,
-- or entities. Four detection pathways:
--
--   1. GTD target nationality = 'United States'
--   2. US-specific casualty fields (nkillus/nwoundus/nhostkidus) > 0
--   3. Military target + US nationality context
--   4. Narrative keyword search in GRID incident_summary
--
-- Each pathway is tracked for transparency and validation.
-- ──────────────────────────────────────────────────────────────────

with unified as (

    select * from {{ ref('trn_unified_events') }}

),

us_detection as (

    select
        *,
        -- Pathway 1: explicit US target nationality in GTD
        case
            when target_nationality = 'United States'
            then true else false
        end as is_us_by_nationality,

        -- Pathway 2: US-specific casualty fields nonzero
        case
            when coalesce(us_killed, 0) > 0
              or coalesce(us_wounded, 0) > 0
              or coalesce(us_hostages, 0) > 0
            then true else false
        end as is_us_by_casualties,

        -- Pathway 3: military target + US context
        case
            when target_type = 'Military'
             and target_nationality = 'United States'
            then true else false
        end as is_us_military_target,

        -- Pathway 4: keyword search in GRID narrative
        case
            when incident_summary is not null
             and (
                 regexp_like(lower(incident_summary),
                    'u\.?s\.?\s+(military|soldier|troop|force|convoy|base|embassy)')
                 or regexp_like(lower(incident_summary),
                    'american\s+(soldier|troop|force|military|personnel)')
                 or regexp_like(lower(incident_summary),
                    '(target|attack|ambush|bomb)\w*\s+(an?\s+)?(u\.?s\.?|american)')
                 or regexp_like(lower(incident_summary),
                    'coalition\s+force')
             )
            then true else false
        end as is_us_by_narrative

    from unified

),

flagged as (

    select
        *,
        -- Combined flag: ANY pathway triggers inclusion
        case
            when is_us_by_nationality
              or is_us_by_casualties
              or is_us_military_target
              or is_us_by_narrative
            then true else false
        end as is_us_targeted,

        -- Count pathways that detected this event
        (case when is_us_by_nationality then 1 else 0 end
         + case when is_us_by_casualties then 1 else 0 end
         + case when is_us_military_target then 1 else 0 end
         + case when is_us_by_narrative then 1 else 0 end
        ) as us_detection_pathways

    from us_detection

)

select * from flagged
where is_us_targeted = true
