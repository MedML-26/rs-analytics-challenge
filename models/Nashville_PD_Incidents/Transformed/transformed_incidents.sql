-- Transformed: Feature engineering and categorization
-- Athena-compatible syntax

with staged as (
    select * from {{ ref('stg_incidents') }}
),

enriched as (
    select
        -- Core fields
        incident_id,
        primary_key,
        incident_number,
        offense_number,
        report_type,
        report_type_description,
        incident_status_code,
        incident_status_description,
        investigation_status,
        
        -- Location
        incident_location,
        latitude,
        longitude,
        x_coordinate,
        y_coordinate,
        reporting_patrol_area,
        police_zone,
        location_code,
        location_description,
        zip_code,
        
        -- Categorize locations
        case
            when location_description like '%RESIDENCE%' or location_description = 'APARTMENT' then 'Residential'
            when location_description like '%PARKING%' then 'Parking Area'
            when location_description like '%HIGHWAY%' or location_description like '%ROAD%' then 'Street/Highway'
            when location_description like '%HOTEL%' or location_description like '%MOTEL%' then 'Lodging'
            when location_description in ('SPECIALTY STORE', 'CONVENIENCE STORE', '"DEPARTMENT', '"GROCERY', '"SERVICE') then 'Retail/Commercial'
            when location_description like '%RESTAURANT%' or location_description like '%BAR%' then 'Food/Drink'
            when location_description like '%HOSPITAL%' then 'Healthcare'
            when location_description like '%SCHOOL%' then 'Education'
            when location_description like '%GOVERNMENT%' then 'Government'
            else 'Other'
        end as location_category,
        
        -- Offense details
        nibrs_code,
        offense_description,
        
        -- Categorize offenses
        case
            when offense_description like '%BURGLARY%' then 'Property Crime - Burglary'
            when offense_description like '%THEFT%' or offense_description like '%SHOPLIFTING%' or offense_description like '%LARC%' then 'Property Crime - Theft'
            when offense_description like '%VEHICLE THEFT%' then 'Property Crime - Vehicle Theft'
            when offense_description like '%DAMAGE%' or offense_description like '%VANDAL%' then 'Property Crime - Vandalism'
            when offense_description like '%ASSAULT%' or offense_description like '%BATTERY%' then 'Violent Crime - Assault'
            when offense_description like '%ROBBERY%' then 'Violent Crime - Robbery'
            when offense_description like '%HOMICIDE%' or offense_description like '%MURDER%' then 'Violent Crime - Homicide'
            when offense_description like '%SEXUAL%' or offense_description like '%RAPE%' then 'Violent Crime - Sexual Offense'
            when offense_description like '%FRAUD%' or offense_description like '%COUNTERFEIT%' then 'Financial Crime'
            when offense_description like '%DRUG%' or offense_description like '%CONTROLLED SUBSTANCE%' or offense_description like '%NARCOTIC%' then 'Drug-Related'
            when offense_description like '%WEAPON%' then 'Weapons Offense'
            when offense_description in ('POLICE INQUIRY', '740', '810', 'LOST PROPERTY', 'FOUND PROPERTY') then 'Administrative/Service'
            else 'Other'
        end as offense_category,
        
        -- Weapon details
        weapon_description,
        weapon_primary_code,
        
        case
            when weapon_description in ('NONE', 'None') then 'No Weapon'
            when weapon_description like '%HANDGUN%' or weapon_description like '%REVOLVER%' then 'Firearm - Handgun'
            when weapon_description like '%RIFLE%' or weapon_description like '%SHOTGUN%' then 'Firearm - Long Gun'
            when weapon_description like '%FIREARM%' then 'Firearm - Other'
            when weapon_description like '%KNIFE%' or weapon_description like '%CUTTING%' or weapon_description like '%LETHAL%' then 'Edged Weapon'
            when weapon_description like '%CLUB%' or weapon_description like '%BLUNT%' then 'Blunt Weapon'
            when weapon_description like '%PERSONAL%' or weapon_description like '%HANDS%' then 'Personal Weapons'
            when weapon_description like '%MOTOR VEHICLE%' then 'Vehicle'
            when weapon_description like '%DRUG%' or weapon_description like '%COCAINE%' then 'Drugs'
            else 'Other/Unknown'
        end as weapon_category,
        
        -- Victim info
        victim_number,
        is_domestic_related,
        victim_type_code,
        victim_description,
        victim_gender,
        victim_race,
        victim_ethnicity,
        victim_county_resident,
        
        case
            when victim_type_code = 'I' then 'Individual'
            when victim_type_code = 'B' then 'Business'
            when victim_type_code = 'S' then 'Society'
            when victim_type_code = 'G' then 'Government'
            when victim_type_code = 'F' then 'Financial Institution'
            when victim_type_code = 'R' then 'Religious Organization'
            when victim_type_code = 'U' then 'Unknown'
            else 'Other'
        end as victim_type,
        
        -- Temporal data
        incident_occurred_at,
        incident_reported_at,
        
        -- Time features (null-safe)
        case when incident_occurred_at is not null then year(incident_occurred_at) end as occurred_year,
        case when incident_occurred_at is not null then month(incident_occurred_at) end as occurred_month,
        case when incident_occurred_at is not null then day_of_month(incident_occurred_at) end as occurred_day,
        case when incident_occurred_at is not null then day_of_week(incident_occurred_at) end as occurred_day_of_week,
        case when incident_occurred_at is not null then hour(incident_occurred_at) end as occurred_hour,
        
        case 
            when day_of_week(incident_occurred_at) = 1 then 'Monday'
            when day_of_week(incident_occurred_at) = 2 then 'Tuesday'
            when day_of_week(incident_occurred_at) = 3 then 'Wednesday'
            when day_of_week(incident_occurred_at) = 4 then 'Thursday'
            when day_of_week(incident_occurred_at) = 5 then 'Friday'
            when day_of_week(incident_occurred_at) = 6 then 'Saturday'
            when day_of_week(incident_occurred_at) = 7 then 'Sunday'
        end as occurred_day_name,
        
        case
            when hour(incident_occurred_at) between 0 and 5 then 'Late Night (12am-6am)'
            when hour(incident_occurred_at) between 6 and 11 then 'Morning (6am-12pm)'
            when hour(incident_occurred_at) between 12 and 17 then 'Afternoon (12pm-6pm)'
            when hour(incident_occurred_at) between 18 and 23 then 'Evening (6pm-12am)'
        end as time_of_day,
        
        -- Response time using date_diff (Athena compatible)
        case 
            when incident_occurred_at is not null and incident_reported_at is not null
            then cast(date_diff('second', incident_occurred_at, incident_reported_at) as double) / 3600.0
        end as response_time_hours,
        
        -- Quality flags
        has_coordinates,
        has_zone,
        has_valid_occurred_date,
        has_valid_reported_date
        
    from staged
)

select * from enriched