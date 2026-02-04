with tencode_stats as (

    select
        tencode,
        tencode_code,
        tencode_suffix,
        tencode_suffix_description,
        count(*)                                            as total_calls,
        sum(case when has_geolocation then 1 else 0 end)   as geocoded_calls,
        round(
            sum(case when has_geolocation then 1 else 0 end) 
            * 100.0 / count(*), 2
        )                                                   as geocoding_rate,
        count(distinct disposition_description)             as unique_dispositions

    from {{ ref('transformed_calls') }}
    group by 1, 2, 3, 4

)

select * from tencode_stats