{{ config(materialized='view') }}

SELECT * FROM {{ source('staging', 'fhv_trips_nonpartitioned') }}

-- dbt build -m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}