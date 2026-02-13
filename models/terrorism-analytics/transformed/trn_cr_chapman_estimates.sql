-- trn_cr_chapman_estimates.sql
-- ──────────────────────────────────────────────────────────────────
-- Apply the Chapman bias-corrected Lincoln-Petersen capture-recapture
-- estimator to event counts at every stratification level.
--
-- Chapman: N̂ = (n1+1)(n2+1)/(m+1) − 1
-- Var:     (n1+1)(n2+1)(n1−m)(n2−m) / ((m+1)²(m+2))
-- CI:      Log-normal [N̂/C, N̂·C]
-- ──────────────────────────────────────────────────────────────────

with counts as (

    select * from {{ ref('trn_cr_event_counts') }}

)

select
    stratum_type,
    stratum_key,
    country,
    event_year,
    n1_gtd,
    n2_grid,
    m_matched,

    -- ── Chapman estimator (via macro) ──
    {{ chapman_estimator('n1_gtd', 'n2_grid', 'm_matched') }},

    -- ── Interpretation flag ──
    case
        when m_matched = 0 then 'NO_MATCHES'
        when (cast(m_matched as double) / nullif(cast(n2_grid as double), 0))
             / nullif(
                 cast(n1_gtd as double) / nullif(
                     (cast(n1_gtd + 1 as double) * cast(n2_grid + 1 as double))
                     / cast(m_matched + 1 as double) - 1.0
                 , 0)
             , 0) > 1.0
        then 'POSITIVE_DEPENDENCE_LOWER_BOUND'
        else 'APPROXIMATELY_INDEPENDENT'
    end as interpretation,

    '{{ var("terrorism_overlap_start") }}'   as overlap_start,
    '{{ var("terrorism_overlap_end") }}'     as overlap_end,
    current_timestamp                        as _estimated_at

from counts
