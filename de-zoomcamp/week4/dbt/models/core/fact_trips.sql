{{
    config(
        materialized='table'
    )
}}

with green_trips as (
    select *,
    'Green' as taxi_type
    from {{ ref('stg_green_trips') }}
),

yellow_trips as (
    select *,
    'Yellow' as taxi_type
    from {{ ref('stg_yellow_trips') }}
),

trips_unioned as (
    select * from green_trips
    union all
    select * from yellow_trips
),
dim_zones as (
    select *
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    trips.trip_id, 
    trips.vendor_id, 
    trips.taxi_type,
    trips.ratecode_id, 
    trips.pickup_location_id, 
    pickup.borough as pickup_borough, 
    pickup.zone as pickup_zone, 
    trips.dropoff_location_id,
    dropoff.borough as dropoff_borough, 
    dropoff.zone as dropoff_zone,  
    trips.pickup_datetime, 
    trips.dropoff_datetime, 
    trips.store_and_fwd_flag, 
    trips.passenger_count, 
    trips.trip_distance, 
    trips.trip_type, 
    trips.fare_amount, 
    trips.extra, 
    trips.mta_tax, 
    trips.tip_amount, 
    trips.tolls_amount, 
    trips.ehail_fee, 
    trips.improvement_surcharge, 
    trips.total_amount, 
    trips.payment_type, 
    trips.payment_type_description
from trips_unioned as trips
inner join dim_zones as pickup
on trips.pickup_location_id = pickup.location_id
inner join dim_zones as dropoff
on trips.dropoff_location_id = dropoff.location_id 