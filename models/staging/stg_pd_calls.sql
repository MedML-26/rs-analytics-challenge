with source as (

    select * from {{ source('pd_calls_db', 'pd_calls_raw') }}

),

cleaned as (

    select
        cast(objectid as int)                       as call_id,
        event_number,
        cast(complaint_number as bigint)            as complaint_number,
        cast(tencode as int)                        as tencode,
        cast(tencode_description as int)            as tencode_code,
        tencode_suffix,
        tencode_suffix_description,
        disposition_code,
        disposition_description,
        cast(block as int)                          as block,
        street_name,
        unit_dispatched,
        shift,
        sector,
        zone,
        cast(latitude as double)                    as latitude,
        cast(longitude as double)                   as longitude,
        cast(rpa as int)                            as rpa,
        cast(
            date_parse(call_received, '%m/%d/%Y %h:%i:%s %p') 
            as timestamp
        )                                           as call_received_at,
        cast(x as double)                           as x_coord,
        cast(y as double)                           as y_coord,

        case 
            when latitude is not null and longitude is not null 
            then true else false 
        end                                         as has_geolocation,

        case 
            when disposition_description is not null 
            then true else false 
        end                                         as has_disposition

    from source

)

select * from cleaned