-- Analytics: Daily incident aggregates for trend analysis

with incidents as (
    select * from {{ ref('fct_incidents') }}
),

daily_aggregates as (
    select
        cast(incident_occurred_at as date) as incident_date,
        occurred_year,
        occurred_month,
        occurred_day_of_week,
        occurred_day_name,
        
        -- Overall counts
        count(*) as total_incidents,
        count(distinct incident_number) as unique_incident_numbers,
        
        -- By offense category
        count(case when offense_category = 'Property Crime - Theft' then 1 end) as theft_incidents,
        count(case when offense_category = 'Property Crime - Burglary' then 1 end) as burglary_incidents,
        count(case when offense_category like 'Violent Crime%' then 1 end) as violent_crime_incidents,
        count(case when offense_category = 'Drug-Related' then 1 end) as drug_incidents,
        
        -- Investigation status
        sum(closed_case_count) as closed_cases,
        sum(open_case_count) as open_cases,
        round(cast(sum(closed_case_count) as decimal) / count(*) * 100, 2) as closure_rate_pct,
        
        -- Weapon involvement
        sum(weapon_involved_count) as incidents_with_weapons,
        round(cast(sum(weapon_involved_count) as decimal) / count(*) * 100, 2) as weapon_rate_pct,
        
        -- Domestic violence
        sum(domestic_incident_count) as domestic_incidents,
        round(cast(sum(domestic_incident_count) as decimal) / count(*) * 100, 2) as domestic_rate_pct,
        
        -- Response time metrics
        avg(response_time_hours) as avg_response_hours,
        min(response_time_hours) as min_response_hours,
        max(response_time_hours) as max_response_hours,
        
        -- Data quality
        sum(case when has_coordinates then 1 else 0 end) as incidents_with_coordinates,
        round(cast(sum(case when has_coordinates then 1 else 0 end) as decimal) / count(*) * 100, 2) as coordinate_coverage_pct
        
    from incidents
    where incident_occurred_at is not null
    group by 1, 2, 3, 4, 5
)

select * from daily_aggregates
order by incident_date desc