-- assert_cr_estimates_valid.sql
-- ──────────────────────────────────────────────────────────────────
-- Validate that Chapman capture-recapture estimates satisfy all
-- logical constraints. Returns FAILING rows (dbt test passes if
-- this query returns zero rows).
--
-- Constraints:
--   1. N̂ ≥ n_observed  (can't estimate fewer than we actually saw)
--   2. CI lower > 0     (can't have negative events)
--   3. CI upper ≥ N̂    (upper CI must be above point estimate)
--   4. 0 ≤ coverage ≤ 1 (proportions must be valid)
-- ──────────────────────────────────────────────────────────────────

with estimates as (

    select * from {{ ref('trn_cr_chapman_estimates') }}
    where n_hat is not null   -- exclude m=0 strata (legitimately null)

)

select
    stratum_type,
    stratum_key,
    n_hat,
    n_observed,
    ci_lower,
    ci_upper,
    coverage_combined,
    case
        when n_hat < n_observed             then 'N_HAT_BELOW_OBSERVED'
        when ci_lower <= 0                  then 'CI_LOWER_NOT_POSITIVE'
        when ci_upper < n_hat               then 'CI_UPPER_BELOW_ESTIMATE'
        when coverage_combined < 0
          or coverage_combined > 1          then 'COVERAGE_OUT_OF_RANGE'
    end as failure_reason

from estimates
where n_hat < n_observed
   or ci_lower <= 0
   or ci_upper < n_hat
   or coverage_combined < 0
   or coverage_combined > 1
