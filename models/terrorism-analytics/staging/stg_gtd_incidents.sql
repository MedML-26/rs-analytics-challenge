-- stg_gtd_incidents.sql
-- ──────────────────────────────────────────────────────────────────
-- Standardize raw GTD data: construct dates from iyear/imonth/iday,
-- clean coordinates, filter to Middle East, select analysis columns.
--
-- Athena/Presto SQL via dbt-athena-community adapter.
-- ──────────────────────────────────────────────────────────────────

with source as (

    select * from {{ source('raw_gtd', 'gtd_incidents') }}

),

date_constructed as (

    select
        *,
        -- GTD stores date as 3 integer columns. 0 = unknown → impute midpoints.
        case when cast(imonth as integer) = 0 then 6
             else cast(imonth as integer) end                    as imonth_clean,
        case when cast(iday as integer) = 0 then 15
             else cast(iday as integer) end                      as iday_clean,
        case when cast(imonth as integer) = 0
             then true else false end                            as is_month_imputed,
        case when cast(iday as integer) = 0
             then true else false end                            as is_day_imputed
    from source

),

dates_parsed as (

    select
        *,
        -- Build a proper DATE from cleaned year/month/day.
        -- try() returns NULL instead of erroring on impossible combos (e.g. Feb 31).
        try(date_parse(
            cast(iyear as varchar) || '-'
            || lpad(cast(imonth_clean as varchar), 2, '0') || '-'
            || lpad(cast(iday_clean as varchar), 2, '0'),
            '%Y-%m-%d'
        )) as event_date
    from date_constructed

),

coords_cleaned as (

    select
        *,
        -- Null-island (0, 0) is missing data, not Gulf of Guinea events
        case
            when cast(latitude as double) = 0.0
             and cast(longitude as double) = 0.0 then true
            else false
        end as is_null_island,
        -- Out-of-range coordinates
        case
            when abs(cast(latitude as double)) > 90
              or abs(cast(longitude as double)) > 180 then true
            else false
        end as is_coords_invalid,
        -- Clean: null out bad values
        case
            when (cast(latitude as double) = 0.0 and cast(longitude as double) = 0.0)
              or abs(cast(latitude as double)) > 90 then null
            else cast(latitude as double)
        end as latitude_clean,
        case
            when (cast(latitude as double) = 0.0 and cast(longitude as double) = 0.0)
              or abs(cast(longitude as double)) > 180 then null
            else cast(longitude as double)
        end as longitude_clean
    from dates_parsed

),

middle_east_filtered as (

    select *
    from coords_cleaned
    where country_txt in ({{ var_list('me_countries') }})

)

select
    -- Primary key
    cast(eventid as varchar)                as event_id,

    -- Date
    event_date,
    cast(iyear as integer)                  as event_year,
    cast(imonth_clean as integer)           as event_month,
    cast(iday_clean as integer)             as event_day,
    is_month_imputed,
    is_day_imputed,

    -- Geography
    country_txt                             as country,
    region_txt                              as region,
    provstate                               as state_province,
    city,
    latitude_clean                          as latitude,
    longitude_clean                         as longitude,
    is_null_island,
    is_coords_invalid,

    -- Attack classification
    attacktype1_txt                         as attack_type_primary,
    cast(attacktype1 as integer)            as attack_type_code,
    attacktype2_txt                         as attack_type_secondary,
    attacktype3_txt                         as attack_type_tertiary,

    -- Weapon classification
    weaptype1_txt                           as weapon_type_primary,
    cast(weaptype1 as integer)              as weapon_type_code,
    weapsubtype1_txt                        as weapon_subtype_primary,

    -- Target classification
    targtype1_txt                           as target_type_primary,
    cast(targtype1 as integer)              as target_type_code,
    targsubtype1_txt                        as target_subtype_primary,
    natlty1_txt                             as target_nationality,

    -- Perpetrator
    gname                                   as perpetrator_group,
    gsubname                                as perpetrator_subgroup,

    -- Incident characteristics
    cast(success as integer)                as is_successful,
    cast(suicide as integer)                as is_suicide,
    cast(crit1 as integer)                  as criteria_1_political,
    cast(crit2 as integer)                  as criteria_2_intimidation,
    cast(crit3 as integer)                  as criteria_3_outside_ihl,
    cast(doubtterr as integer)              as is_doubt_terrorism,

    -- Casualties
    cast(nkill as double)                   as total_killed,
    cast(nkillus as double)                 as us_killed,
    cast(nwound as double)                  as total_wounded,
    cast(nwoundus as double)                as us_wounded,
    cast(nkillter as double)                as perpetrators_killed,

    -- Hostage/kidnapping
    cast(nhostkid as double)                as total_hostages,
    cast(nhostkidus as double)              as us_hostages,

    -- Data quality
    cast(specificity as integer)            as location_specificity,

    -- Provenance
    'GTD'                                   as source_dataset,
    current_timestamp                       as _dbt_loaded_at

from middle_east_filtered
