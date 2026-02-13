{# ── var_list.sql ──────────────────────────────────────────────── #}
{# Expand a dbt variable that holds a YAML list into a SQL        #}
{# comma-separated string for use in WHERE ... IN (...) clauses.  #}
{#                                                                 #}
{# Usage:                                                          #}
{#   WHERE country IN ({{ var_list('me_countries') }})             #}
{#                                                                 #}
{# Produces:                                                       #}
{#   WHERE country IN ('Iraq', 'Syria', 'Yemen', ...)             #}
{# ─────────────────────────────────────────────────────────────── #}

{% macro var_list(var_name) %}
    {% set items = var(var_name) %}
    {% for item in items %}
        '{{ item }}'{% if not loop.last %}, {% endif %}
    {% endfor %}
{% endmacro %}
