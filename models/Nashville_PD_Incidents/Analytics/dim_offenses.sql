-- Analytics: Offense dimension table with statistics
-- Athena-compatible syntax (mode() removed)

with incidents as (
    select * from {{ ref('fct_incidents') }}
),

-- Get most common values using window functions
location_rankings as (
    select 
        offense_description,
        location_category,
        row_number() over (partition by offense_description order by count(*) desc) as rn
    from incidents
    where offense_description is not null
    group by 1, 2
),

time_rankings as (
    select 
        offense_description,
        time_of_day,
        row_number() over (partition by offense_description order by count(*) desc) as rn
    from incidents
    where offense_description is not null and time_of_day is not null
    group by 1, 2
),

day_rankings as (
    select 
        offense_description,
        occurred_day_name,
        row_number() over (partition by offense_description order by count(*) desc) as rn
    from incidents
    where offense_description is not null and occurred_day_name is not null
    group by 1, 2
),

offense_stats as (
    select
        offense_description,
        offense_category,
        nibrs_code,
        
        -- Volume metrics
        count(*) as total_incidents,
        count(distinct incident_number) as unique_incident_numbers,
        
        -- Investigation outcomes
        sum(closed_case_count) as closed_cases,
        sum(open_case_count) as open_cases,
        round(cast(sum(closed_case_count) as decimal) / count(*) * 100, 2) as closure_rate_pct,
        
        -- Associated characteristics
        sum(weapon_involved_count) as incidents_with_weapons,
        round(cast(sum(weapon_involved_count) as decimal) / count(*) * 100, 2) as weapon_involvement_rate,
        
        sum(domestic_incident_count) as domestic_incidents,
        round(cast(sum(domestic_incident_count) as decimal) / count(*) * 100, 2) as domestic_rate,
        
        -- Response time
        avg(response_time_hours) as avg_response_hours,
        
        -- Geographic spread
        count(distinct police_zone) as zones_affected,
        count(distinct reporting_patrol_area) as patrol_areas_affected,
        
        -- Date range
        min(incident_occurred_at) as first_recorded,
        max(incident_occurred_at) as most_recent
        
    from incidents
    where offense_description is not null
    group by 1, 2, 3
)

select 
    os.*,
    lr.location_category as most_common_location,
    tr.time_of_day as most_common_time_of_day,
    dr.occurred_day_name as most_common_day,
    round(cast(os.total_incidents as decimal) / 
        (select count(*) from {{ ref('fct_incidents') }}) * 100, 2) as pct_of_all_incidents
    
from offense_stats os
left join location_rankings lr 
    on os.offense_description = lr.offense_description 
    and lr.rn = 1
left join time_rankings tr 
    on os.offense_description = tr.offense_description 
    and tr.rn = 1
left join day_rankings dr 
    on os.offense_description = dr.offense_description 
    and dr.rn = 1
order by os.total_incidents desc