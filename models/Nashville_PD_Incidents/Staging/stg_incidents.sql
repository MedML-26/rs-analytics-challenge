-- Staging: Load raw incidents with data cleaning and quality flags
-- Athena-compatible version using REGEXP_LIKE instead of ~ operator

with source_data as (
    select * from {{ source('nashville_incidents', 'incidents_raw') }}
),

cleaned as (
    select
        -- Core identifiers
        objectid as incident_id,
        primary_key,
        incident_number,
        offense_number,
        
        -- Report metadata
        report_type,
        report_type_description,
        incident_status_code,
        incident_status_description,
        investigation_status,
        
        -- Location data
        incident_location,
        latitude,
        longitude,
        x as x_coordinate,
        y as y_coordinate,
        rpa as reporting_patrol_area,
        zone as police_zone,
        location_code,
        location_description,
        zip_code,
        
        -- Offense details
        offense_nibrs as nibrs_code,
        offense_description,
        
        -- Weapon info - clean mixed types
        case 
            when weapon_description is null then null
            when weapon_description in ('NONE', 'None', '') then 'NONE'
            when REGEXP_LIKE(weapon_description, '^[0-9]+$') then null
            when weapon_description in ('I', 'B', 'S', 'G', 'U', 'F', 'P', 'O') then null
            else upper(trim(weapon_description))
        end as weapon_description,
        
        case
            when weapon_primary in ('17', '16', '07', '09', '13', '01') then null
            else weapon_primary
        end as weapon_primary_code,
        
        -- Victim info - clean mixed types
        victim_number,
        
        case 
            when lower(trim(domestic_related)) in ('yes', 'y', '1') then true
            when lower(trim(domestic_related)) in ('no', 'n', '0') then false
            else null
        end as is_domestic_related,
        
        case
            when victim_type in ('I', 'B', 'S', 'G', 'U', 'F', 'P', 'O', 'R') then victim_type
            when victim_type in ('Yes', 'No', 'NONE') then null
            when REGEXP_LIKE(victim_type, '^[0-9]+$') then null
            else null
        end as victim_type_code,
        
        victim_description,
        
        case
            when victim_gender in ('M', 'F', 'U') then victim_gender
            else null
        end as victim_gender,
        
        case
            when victim_race in ('W', 'B', 'A', 'I', 'U') then victim_race
            else null
        end as victim_race,
        
        victim_ethnicity,
        victim_county_resident,
        
        -- Parse dates - handle mixed formats
        case
            when REGEXP_LIKE(incident_occurred, '^\d{4}/\d{2}/\d{2}') then 
                cast(
                    replace(substr(incident_occurred, 1, 19), '/', '-')
                    as timestamp
                )
            else null
        end as incident_occurred_at,
        
        case
            when REGEXP_LIKE(incident_reported, '^\d{4}/\d{2}/\d{2}') then 
                cast(
                    replace(substr(incident_reported, 1, 19), '/', '-')
                    as timestamp
                )
            else null
        end as incident_reported_at,
        
        -- Quality flags
        case when latitude is not null and longitude is not null then true else false end as has_coordinates,
        case when zone is not null then true else false end as has_zone,
        case when REGEXP_LIKE(incident_occurred, '^\d{4}/\d{2}/\d{2}') then true else false end as has_valid_occurred_date,
        case when REGEXP_LIKE(incident_reported, '^\d{4}/\d{2}/\d{2}') then true else false end as has_valid_reported_date
        
    from source_data
)

select * from cleaned