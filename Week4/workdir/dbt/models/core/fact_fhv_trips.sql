{{ config(materialized="table") }}

with fhv_data as (
    SELECT *,
        'fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
),

dim_zones as (
    SELECT * FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)

SELECT
    fhv_data.dispatching_base_num,
    fhv_data.pickup_datetime,
    fhv_data.dropoff_datetime,
    fhv_data.PULocationID,
    fhv_data.DOLocationID,
    fhv_data.SR_Flag,
    fhv_data.Affiliated_base_number
FROM fhv_data

INNER JOIN dim_zones as pickup_zones
ON fhv_data.PULocationID = pickup_zones.locationid