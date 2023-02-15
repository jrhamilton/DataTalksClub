from pathlib import Path
import pandas as pd
from google.cloud import storage
import os
from prefect import flow, task
from prefect.tasks import task_input_hash


@task()
def format_file_paths(datasets_page: str, tag: str, file_prefix: str,
                      year: str, month: str, ext: str) -> (str, str):
    """Format for fetch dataset_url and path for local and GCS"""
    f = f"{file_prefix}_{year}-{month:02}.{ext}"
    dataset_url = f"{datasets_page}/{f}"
    path = f"data/tag/{tag}/{f}"
    return (dataset_url, path)


@task()
def fetch(url: str, path: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    print(url)

    path_array = path.split("/")
    path_array.pop()
    path_dir = '/'.join(path_array)
    Path(f"{path_dir}").mkdir(parents=True, exist_ok=True)

    os.system(f"wget {url} -O {path}")
    
    return


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""

    gcs = storage.Client()
    bucket = gcs.get_bucket("prefect-datatalks-taxi")
    blob = bucket.blob(path)
    blob.upload_from_filename(path, timeout=120)

    return


@flow()
def web_to_gcs(datasets_page: str, tag: str, file_prefix: str,
               year: str, months: list[int], ext: str) -> None:
    """The Main ETL/ELT function"""

    for month in months:
        (dataset_url, path) = format_file_paths(datasets_page, tag, file_prefix, year, month, ext)
        df = fetch(dataset_url, path)
        write_gcs(path)


if __name__ == '__main__':
    tag = "fhv"
    datasets_page = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{tag}"
    file_prefix = "fhv_tripdata"
    year = 2019
    months = [1,2,3,4,5,6,7,8,10,11,12]
    #months = [9]
    ext = "csv.gz"

    web_to_gcs(datasets_page, tag, file_prefix, year, months, ext)
