{{ config(materialized='table') }}

select 
    locationid AS location_id, 
    borough, 
    zone, 
    replace(service_zone,'Boro','Green') as service_zone 
from {{ ref('zone_lookup') }}