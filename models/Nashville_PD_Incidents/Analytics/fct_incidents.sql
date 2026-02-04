-- Analytics: Core fact table for incident analysis
-- Optimized for querying with pre-calculated metrics

with transformed as (
    select * from {{ ref('transformed_incidents') }}
),

fact_incidents as (
    select
        -- Identifiers
        incident_id,
        incident_number,
        offense_number,
        
        -- Temporal dimensions
        incident_occurred_at,
        incident_reported_at,
        occurred_year,
        occurred_month,
        occurred_day,
        occurred_day_of_week,
        occurred_day_name,
        occurred_hour,
        time_of_day,
        
        -- Geographic dimensions
        latitude,
        longitude,
        reporting_patrol_area,
        police_zone,
        zip_code,
        location_code,
        location_description,
        location_category,
        
        -- Offense dimensions
        offense_description,
        offense_category,
        nibrs_code,
        
        -- Weapon dimensions
        weapon_description,
        weapon_category,
        
        -- Victim dimensions
        victim_type,
        victim_type_code,
        victim_gender,
        victim_race,
        victim_ethnicity,
        
        -- Investigation dimensions
        investigation_status,
        incident_status_description,
        report_type_description,
        
        -- Pre-calculated metrics
        response_time_hours,
        case when is_domestic_related = true then 1 else 0 end as domestic_incident_count,
        case when investigation_status = 'Closed' then 1 else 0 end as closed_case_count,
        case when investigation_status = 'Open' then 1 else 0 end as open_case_count,
        case when weapon_category != 'No Weapon' then 1 else 0 end as weapon_involved_count,
        1 as incident_count,
        
        -- Data quality metrics
        has_coordinates,
        has_zone,
        has_valid_occurred_date,
        has_valid_reported_date
        
    from transformed
    -- Keep all records, even those with invalid dates
-- This preserves 669K records with data quality issues for analysis
)

select * from fact_incidents