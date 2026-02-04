with disposition_stats as (

    select
        disposition_code,
        disposition_description,
        disposition_category,
        count(*)                                            as total_calls,
        sum(case when has_geolocation then 1 else 0 end)   as geocoded_calls,
        count(distinct tencode)                             as unique_tencodes,
        count(distinct shift)                               as active_shifts

    from {{ ref('transformed_calls') }}
    group by 1, 2, 3

)

select * from disposition_stats