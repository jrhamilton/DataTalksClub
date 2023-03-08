# From Google Cloud Storage to Big Query
_video_: https://www.youtube.com/watch?v=Cx5jt-V5sgE&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=21

_file_: etl_gcs_to_bq.py

We need to extract data from the Google Cloud Storage using GcsBucket.load()
We use the path (gcs_path) to get the path on the Storage Bucket.
GcsBucket has a method get_directory that get works on GCS and delivers to a local path.
We then return the path to our main flow.
```python
@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("datatalks-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path="../data/")
    return Path(f"../data/{gcs_path}")
```

We then proceed to tranform the data.
We add 0 passengers to missing passenger counts data.
```python
@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing messenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df
```

Now write this data into BigQuery with next task.
We get out GCP Credentials loaded that we created from the Prefect Orion block.
Then run pandas dataframe method to Google's BigQuery

```python
@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("path-cred-1") 

    df.to_gbq(
            destination_table="dtczoomcamp.taxi-rides",
            project_id="datatalksclub-375802",
            credentials=gcp_credentials_block.get_credentials_from_service_account(),
            chunksize=500_000,
            if_exists="append",
    )
```

Now we need to go to the Google Cloud account and create a table.
Follow from 14:05 of the video is easier than me typing it out.

run:
`python etl_gcs_to_bq.py`

Then check in BigQuery by running a query select to see results.
