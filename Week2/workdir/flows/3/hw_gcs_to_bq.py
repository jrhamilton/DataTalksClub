from pathlib import Path
from datetime import timedelta
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash

@task(retries=3,cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""

    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"

    # - Setting path. Was having issues with working_dir paraemter in yaml
    # - Create a base data directory which will result in data/data/{color}
    # - First resolve to working directory (may be unecessary but for clarity)
    working_path = Path().resolve()
    # - Next append the base data data directory
    base_data_path = f"{working_path}/data"

    # - Create the base data directory
    Path(f"{base_data_path}/{color}").mkdir(parents=True, exist_ok=True)

    gcs_block = GcsBucket.load("datatalks-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"{base_data_path}/")

    return Path(f"{base_data_path}/{gcs_path}")


@task()
def to_df(path: Path) -> pd.DataFrame:
    """To DataFrame"""
    df = pd.read_parquet(path)
    return df


@task(log_prints=True)
def write_bq(df: pd.DataFrame, month: int) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("path-cred-1") 

    rc = len(df)

    df.to_gbq(
            destination_table="dtczoomcamp.y-2019-23",
            project_id="datatalksclub-375802",
            credentials=gcp_credentials_block.get_credentials_from_service_account(),
            chunksize=500_000,
            if_exists="append",
    )

    return rc


@task(log_prints=True)
def print_row_count(rows):
    print(f"Total Row Count: {rows}")


@flow()
def etl_gcs_to_bq(color: str, year: int, month: int):
    """Main ETL flow to load multiple dataframes into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = to_df(path)
    rows = write_bq(df, month)
    return rows


@flow()
def rowcount_many_gcs_to_bq(
        color: str = "yellow", year: int = 2019, months: list[int] = [2,3]
    ):
    # etl_gcs_to_pq.map(...) will not work
    # because function is a @flow not a @task
    rows = 0
    for month in months:
        rows += etl_gcs_to_bq(color, year, month)

    print_row_count(rows)


if __name__ == "__main__":
    color = "yellow"
    year = 2019
    months = [2]

    rowcount_many_gcs_to_bq(color, year, months)
