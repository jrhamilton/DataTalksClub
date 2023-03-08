# Connecting Spark to Big Query
- Video: https://www.youtube.com/watch?v=HIm2BOj8C0Q&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=58

- Tutorial Reference: https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example#pyspark

### Copy the original file to modify for BigQuery
`cp 06_spark_sql.py spark_sql_bq.py`


### Modify the file

In the file, on the last command, remove the coalesce command then change parquet to table and make it a write to bigquery.

change:

```python
df_result.coalesce(10) \
    .write \
    .parquet(output, mode='overwrite')
```

to:

```python
df_result.write.format('bigquery') \
    .option('table', output) \
    .save()
```

Under the `SparkSession.builder` command, add the following:

```
spark.conf.set('temporaryGcsBucket', '<TEMPBUCKET-LLL-NNN>')
```

You can find the temporaryGcsBucket by searching in GCS Dashboard search 'Cloud Storage', clicking 'Cloud Storage', then finding the storage bucket that has 'temp' in the name. Use that name in the command above.


### Upload to GCS Bucket
`gsutil cp spark_sql_bq.py gs://<GCS_BUCKET>/code/gcs_spark_sql_bq.py`


### The Schema
Find a schema to use from your BigQuery page or create a new one.
I made mine trips_data_all like in the video.
When creating schema, make sure it is in the same region as your bucket to make life easier.
No need to create a new table. BigQuery will create the table for us.


### The Execution

Two new/different things here:
1. A jars flag was added. Find that in the tutorial link above.
2. The output variable was changed to represent the BigQuery Schema/table


Execute:

```
gcloud dataproc jobs submit pyspark \
    --cluster=CLUSTER \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    --project=PROJECT_NAME \
    gs://GCS_BUCKET/code/gcs_spark_sql_bq.py \
    -- \
        --input_green=gs://GCS_BUCKET/pq/green/2020/*/ \
        --input_yellow=gs://GCS_BUCKET/pq/yellow/2020/*/ \
        --output=trips_data_all.reports-2020
```
