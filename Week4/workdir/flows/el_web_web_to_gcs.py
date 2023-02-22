from pathlib import Path
import io
import os
import requests
import time
import pandas as pd
import pyarrow
from google.cloud import storage

BUCKET = os.environ.get("GCP_GCS_BUCKET", "taxi-rides-ny-dtc-110100100")


def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url, low_memory=False)
    return df


def write_local(df: pd.DataFrame,
                service: str,
                dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    Path(f"data/{service}").mkdir(parents=True, exist_ok=True)
    path = Path(f"data/{service}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


def write_gcs(bucket, object_name, local_file) -> None:
    """Upload local parquet file to GCS"""
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def etl_web_to_gcs(service: str, year: int, month: int) -> None:
    """The Main ETL function"""
    dataset_file = f"{service}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    print(f"FETCHED: {dataset_url}")
    path = write_local(df, service, dataset_file)
    print(f"DATASET FILE WRITTEN AS: {dataset_file}")
    write_gcs(BUCKET, f"data/{service}/{dataset_file}.parquet", path)
    print(f"{service} {year} {month} written to {BUCKET}")


if __name__ == '__main__':
    services = ['green', 'yellow']
    years = [2019,2020]
    months = [1,2,3,4,5,6,7,8,9,10,11,12]

    # For Testing
    #services = ['yellow']
    #years = [2020]
    #months = [9]

    for service in services:
        for year in years:
            for month in months:
                time.sleep(0.5)
                etl_web_to_gcs(service, year, month)
                time.sleep(1)
