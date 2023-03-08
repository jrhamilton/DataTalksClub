# Parameterizing Flow and Deployments with ETL into GCS
_video_: https://www.youtube.com/watch?v=QrDxPjX10iw&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=23
_follow_: https://github.com/boisalai/de-zoomcamp-2023/blob/main/week2.md#setup-environment

# Run
I moved to the 03_deployments directory instead of running from the base directory because I was getting a PostgreSql permissions error with the ny_taxi_data directory for some odd reason. Moving up to the 03_deployments directory fixed the issue.
`cd 03_deployments`
`conda activate start`
`prefect orion start`
`prefect deployment build parameterized_flow.py:etl_parent_flow -n "Running Parameterized ETL"`
