-- Analytics: Geographic hotspot analysis by zone and location type

with incidents as (
    select * from {{ ref('fct_incidents') }}
),

geographic_aggregates as (
    select
        police_zone,
        reporting_patrol_area,
        location_category,
        zip_code,
        
        -- Overall metrics
        count(*) as total_incidents,
        count(distinct incident_number) as unique_incidents,
        
        -- Top offense types
        count(case when offense_category = 'Property Crime - Theft' then 1 end) as theft_count,
        count(case when offense_category = 'Property Crime - Burglary' then 1 end) as burglary_count,
        count(case when offense_category like 'Violent Crime%' then 1 end) as violent_crime_count,
        count(case when offense_category = 'Drug-Related' then 1 end) as drug_crime_count,
        
        -- Investigation metrics
        sum(closed_case_count) as closed_cases,
        sum(open_case_count) as open_cases,
        round(cast(sum(closed_case_count) as decimal) / count(*) * 100, 2) as closure_rate_pct,
        
        -- Weapon and domestic violence
        sum(weapon_involved_count) as weapon_involved_incidents,
        sum(domestic_incident_count) as domestic_incidents,
        
        -- Geographic data
        avg(latitude) as avg_latitude,
        avg(longitude) as avg_longitude,
        count(case when has_coordinates then 1 end) as incidents_with_coords,
        
        -- Time patterns
        count(case when time_of_day = 'Late Night (12am-6am)' then 1 end) as late_night_incidents,
        count(case when time_of_day = 'Evening (6pm-12am)' then 1 end) as evening_incidents,
        
        -- Most recent incident
        max(incident_occurred_at) as most_recent_incident
        
    from incidents
    where has_coordinates = true
    group by 1, 2, 3, 4
)

select 
    *,
    round(cast(total_incidents as decimal) / nullif(
        (select count(*) from {{ ref('fct_incidents') }} where has_coordinates = true), 0
    ) * 100, 2) as pct_of_total_incidents
    
from geographic_aggregates
where total_incidents >= 10
order by total_incidents desc