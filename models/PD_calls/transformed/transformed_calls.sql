with staged as (

    select * from {{ ref('stg_pd_calls') }}

),

enriched as (

    select
        *,

        year(call_received_at)                      as call_year,
        month(call_received_at)                     as call_month,
        day(call_received_at)                       as call_day,
        hour(call_received_at)                      as call_hour,
        day_of_week(call_received_at)               as call_day_of_week,

        case
            when day_of_week(call_received_at) in (6, 7) then 'Weekend'
            else 'Weekday'
        end                                         as weekend_flag,

        case
            when hour(call_received_at) between 0 and 5   then 'Night (12am-6am)'
            when hour(call_received_at) between 6 and 11  then 'Morning (6am-12pm)'
            when hour(call_received_at) between 12 and 17 then 'Afternoon (12pm-6pm)'
            else 'Evening (6pm-12am)'
        end                                         as time_period,

        case
            when hour(call_received_at) in (7,8,9)     then 'Morning Rush'
            when hour(call_received_at) in (16,17,18)  then 'Evening Rush'
            else 'Non-Rush'
        end                                         as rush_hour_flag,

        case
            when disposition_description in (
                'ASSISTED CITIZEN', 'ASSISTED OTHER UNIT', 
                'REPORT TAKEN', 'ARREST MADE', 'CITATION ISSUED'
            ) then 'Productive'
            when disposition_description in (
                'DISREGARD / SIGNAL 9', 'UNABLE TO LOCATE', 
                'GONE ON ARRIVAL'
            ) then 'Non-Productive'
            else 'Other'
        end                                         as disposition_category

    from staged

)

select * from enriched