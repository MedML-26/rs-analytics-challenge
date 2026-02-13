{# ── chapman_estimator.sql ─────────────────────────────────────── #}
{# Reusable SQL macro implementing the Chapman bias-corrected      #}
{# Lincoln-Petersen capture-recapture estimator.                   #}
{#                                                                  #}
{# Chapman: N̂ = (n1+1)(n2+1)/(m+1) − 1                           #}
{# Variance: (n1+1)(n2+1)(n1−m)(n2−m) / ((m+1)²(m+2))            #}
{# CI:       Log-normal [N̂/C, N̂·C]                               #}
{#           where C = exp(1.96 · √(ln(1 + Var/N̂²)))              #}
{#                                                                  #}
{# Parameters:                                                      #}
{#   n1_col — column name for dataset 1 count (GTD)                #}
{#   n2_col — column name for dataset 2 count (GRID)               #}
{#   m_col  — column name for matched pair count                   #}
{#                                                                  #}
{# Usage in a SELECT:                                               #}
{#   {{ chapman_estimator('n1_gtd', 'n2_grid', 'm_matched') }}    #}
{#                                                                  #}
{# Returns columns:                                                 #}
{#   n_hat, se_hat, ci_lower, ci_upper,                            #}
{#   n_observed, n_missed,                                          #}
{#   coverage_gtd, coverage_grid, coverage_combined,                #}
{#   dependence_ratio                                               #}
{# ─────────────────────────────────────────────────────────────── #}

{% macro chapman_estimator(n1_col, n2_col, m_col) %}

    -- ── Chapman point estimate ──
    case
        when {{ m_col }} = 0 then null
        else (
            (cast({{ n1_col }} + 1 as double) * cast({{ n2_col }} + 1 as double))
            / cast({{ m_col }} + 1 as double) - 1.0
        )
    end as n_hat,

    -- ── Variance ──
    case
        when {{ m_col }} = 0 then null
        else (
            (cast({{ n1_col }} + 1 as double) * cast({{ n2_col }} + 1 as double)
             * cast({{ n1_col }} - {{ m_col }} as double)
             * cast({{ n2_col }} - {{ m_col }} as double))
            / (power(cast({{ m_col }} + 1 as double), 2)
               * cast({{ m_col }} + 2 as double))
        )
    end as var_hat,

    -- ── Standard error ──
    case
        when {{ m_col }} = 0 then null
        else sqrt(
            (cast({{ n1_col }} + 1 as double) * cast({{ n2_col }} + 1 as double)
             * cast({{ n1_col }} - {{ m_col }} as double)
             * cast({{ n2_col }} - {{ m_col }} as double))
            / (power(cast({{ m_col }} + 1 as double), 2)
               * cast({{ m_col }} + 2 as double))
        )
    end as se_hat,

    -- ── Log-normal 95% CI: lower bound ──
    case
        when {{ m_col }} = 0 then null
        else (
            -- N̂ / C  where C = exp(1.96 * sqrt(ln(1 + Var/N̂²)))
            (
                (cast({{ n1_col }} + 1 as double) * cast({{ n2_col }} + 1 as double))
                / cast({{ m_col }} + 1 as double) - 1.0
            )
            / exp(1.96 * sqrt(ln(
                1.0 + (
                    (cast({{ n1_col }} + 1 as double) * cast({{ n2_col }} + 1 as double)
                     * cast({{ n1_col }} - {{ m_col }} as double)
                     * cast({{ n2_col }} - {{ m_col }} as double))
                    / (power(cast({{ m_col }} + 1 as double), 2)
                       * cast({{ m_col }} + 2 as double))
                )
                / power(
                    (cast({{ n1_col }} + 1 as double) * cast({{ n2_col }} + 1 as double))
                    / cast({{ m_col }} + 1 as double) - 1.0
                , 2)
            )))
        )
    end as ci_lower,

    -- ── Log-normal 95% CI: upper bound ──
    case
        when {{ m_col }} = 0 then null
        else (
            -- N̂ * C
            (
                (cast({{ n1_col }} + 1 as double) * cast({{ n2_col }} + 1 as double))
                / cast({{ m_col }} + 1 as double) - 1.0
            )
            * exp(1.96 * sqrt(ln(
                1.0 + (
                    (cast({{ n1_col }} + 1 as double) * cast({{ n2_col }} + 1 as double)
                     * cast({{ n1_col }} - {{ m_col }} as double)
                     * cast({{ n2_col }} - {{ m_col }} as double))
                    / (power(cast({{ m_col }} + 1 as double), 2)
                       * cast({{ m_col }} + 2 as double))
                )
                / power(
                    (cast({{ n1_col }} + 1 as double) * cast({{ n2_col }} + 1 as double))
                    / cast({{ m_col }} + 1 as double) - 1.0
                , 2)
            )))
        )
    end as ci_upper,

    -- ── Observed total (union count, no duplicates) ──
    ({{ n1_col }} + {{ n2_col }} - {{ m_col }}) as n_observed,

    -- ── Estimated missed events ──
    case
        when {{ m_col }} = 0 then null
        else (
            (cast({{ n1_col }} + 1 as double) * cast({{ n2_col }} + 1 as double))
            / cast({{ m_col }} + 1 as double) - 1.0
        ) - ({{ n1_col }} + {{ n2_col }} - {{ m_col }})
    end as n_missed,

    -- ── Dataset-specific coverage rates ──
    case
        when {{ m_col }} = 0 then null
        else cast({{ n1_col }} as double) / (
            (cast({{ n1_col }} + 1 as double) * cast({{ n2_col }} + 1 as double))
            / cast({{ m_col }} + 1 as double) - 1.0
        )
    end as coverage_gtd,

    case
        when {{ m_col }} = 0 then null
        else cast({{ n2_col }} as double) / (
            (cast({{ n1_col }} + 1 as double) * cast({{ n2_col }} + 1 as double))
            / cast({{ m_col }} + 1 as double) - 1.0
        )
    end as coverage_grid,

    case
        when {{ m_col }} = 0 then null
        else cast({{ n1_col }} + {{ n2_col }} - {{ m_col }} as double) / (
            (cast({{ n1_col }} + 1 as double) * cast({{ n2_col }} + 1 as double))
            / cast({{ m_col }} + 1 as double) - 1.0
        )
    end as coverage_combined,

    -- ── Dependence ratio: P(GTD∩GRID|GRID) / P(GTD) ──
    -- If > 1 → positive dependence (shared media sources) → N̂ is lower bound
    case
        when {{ m_col }} = 0 or {{ n2_col }} = 0 then null
        else (cast({{ m_col }} as double) / cast({{ n2_col }} as double))
             / (
                 cast({{ n1_col }} as double)
                 / nullif(
                     (cast({{ n1_col }} + 1 as double) * cast({{ n2_col }} + 1 as double))
                     / cast({{ m_col }} + 1 as double) - 1.0
                 , 0)
             )
    end as dependence_ratio

{% endmacro %}
