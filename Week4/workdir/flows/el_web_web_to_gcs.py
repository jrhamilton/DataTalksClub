from pathlib import Path
import io
import os
import requests
import time
import pandas as pd
import pyarrow.csv as pycsv
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

BUCKET = os.environ.get("GCP_GCS_BUCKET", "taxi-rides-ny-dtc-110100100")

FCSVGZ = 'data/tempfile.csv.gz'
FCSV = 'data/tempfile.csv'
FPARQ = 'data/tempfile.parquet'

SCHEMA_GREEN = pa.schema(
    [
        ('VendorID',pa.string()),
        ('lpep_pickup_datetime',pa.timestamp('s')),
        ('lpep_dropoff_datetime',pa.timestamp('s')),
        ('store_and_fwd_flag',pa.string()),
        ('RatecodeID',pa.int64()),
        ('PULocationID',pa.int64()),
        ('DOLocationID',pa.int64()),
        ('passenger_count',pa.int64()),
        ('trip_distance',pa.float64()),
        ('fare_amount',pa.float64()),
        ('extra',pa.float64()),
        ('mta_tax',pa.float64()),
        ('tip_amount',pa.float64()),
        ('tolls_amount',pa.float64()),
        ('ehail_fee',pa.float64()),
        ('improvement_surcharge',pa.float64()),
        ('total_amount',pa.float64()),
        ('payment_type',pa.int64()),
        ('trip_type',pa.int64()),
        ('congestion_surcharge',pa.float64()),
    ]
)

SCHEMA_YELLOW = pa.schema(
   [
        ('VendorID', pa.string()), 
        ('tpep_pickup_datetime', pa.timestamp('s')), 
        ('tpep_dropoff_datetime', pa.timestamp('s')), 
        ('passenger_count', pa.int64()), 
        ('trip_distance', pa.float64()), 
        ('RatecodeID', pa.string()), 
        ('store_and_fwd_flag', pa.string()), 
        ('PULocationID', pa.int64()), 
        ('DOLocationID', pa.int64()), 
        ('payment_type', pa.int64()), 
        ('fare_amount',pa.float64()), 
        ('extra',pa.float64()), 
        ('mta_tax', pa.float64()), 
        ('tip_amount', pa.float64()), 
        ('tolls_amount', pa.float64()), 
        ('improvement_surcharge', pa.float64()), 
        ('total_amount', pa.float64()), 
        ('congestion_surcharge', pa.float64())
    ]
)



def fetch(dataset_url: str) -> None:
    """Fetch dataset and unzip"""
    os.system(f"wget {dataset_url} -O {FCSVGZ}")
    os.system(f"gunzip -f {FCSVGZ}")

    print(f"Fetched {dataset_url}")


def read_and_fix_schema(service: str) -> None:
    """Read and fix schema"""

    table = pycsv.read_csv(FCSV)

    print("read_csv has been completed")

    if service =='yellow':
        table = table.cast(SCHEMA_YELLOW)

    elif service == 'green':
        table = table.cast(SCHEMA_GREEN)

    pq.write_table(table,
                   FCSV.replace('.csv.gz',
                                '.parquet'))

    os.system(f"mv {FCSV} {FPARQ}")

    print("Schema written and completed")


def write_gcs(bucket, object_name, local_file) -> None:
    """Upload local parquet file to GCS"""
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
    print(f"{object_name} written to {BUCKET}")


def el_web_to_gcs(service: str, year: int, month: int) -> None:
    """The Main ETL function"""
    dataset_file = f"{service}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{dataset_file}.csv.gz"

    fetch(dataset_url)
    read_and_fix_schema(service)
    write_gcs(BUCKET, f"data/{service}/{dataset_file}.parquet", FPARQ)


def main_flow():
    """Main function of el_to_gcs"""

    services = ['green', 'yellow']
    years = [2019,2020]
    months = [1,2,3,4,5,6,7,8,9,10,11,12]

    # For Testing
    #services = ['green']
    #years = [2020]
    #months = [4,5]

    t_start = time.time()

    for service in services:
        for year in years:
            for month in months:
                time.sleep(0.5)
                m_start = time.time()

                el_web_to_gcs(service, year, month)

                m_end = time.time()
                print(' -- -- -- TIME FOR THIS MONTH = %.3f seconds -- -- --' % (m_end - m_start))
                time.sleep(1)

    t_end = time.time()
    print('TOTAL TIME: %.3f seconds' % (t_end - t_start))


if __name__ == '__main__':
    main_flow()
