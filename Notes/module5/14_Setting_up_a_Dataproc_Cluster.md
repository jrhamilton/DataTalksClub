# Setting Up a Dataproc Cluster
Video: https://www.youtube.com/watch?v=osAiAYahvh8&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=57

# DataProc setup

1. Log in to Google Cloud.
2. Search DataProc.
3. Enable API
4. 'CREATE' cluster on Comput Engine
5. Fill out form
    - Region should be same as Bucket
    - Zone does not matter.
    - Cluster - Single Mode
    - Optional Components, check:
        * Jyputer Notebook
        * Docker
6. Click CREATE (Or ADD), I forget now.


## Submit a Job

1. Now click on the cluster on the list page once the status of the cluster says 'Running'.

2. Click 'Submit a Job'

3. Job Type: PySpark

4. Main Python file: Upload the 06_spark_sql.py file from last session.
    - cd to code directory.
    - `gsutil cp 06_spark_sql.py gs://<GCS_BUCKET>/code/06_spark_sql.py`
    - Paste the full `gs://<GCS_BUCKET>/code/06_spark_sql.py` into Main Python file field.
    - NOTE: May be a good idea to remove the last `df_result.show()` near the end of script before sending to bucket.
        - I did the following: `cp 06_spark_sql.py gcs_spark_sql.py` then sent the latter file to my bucket after removing the show command.

5. Go to Arguments:
    - Take the spark-submit arguments from last session.
    - replace the input files with the ones on the bucket.
        A. First argument:
            - `--input_green=gs://<GCS_BUCKET>/pq/green/2021/*/`
        B. Second argument:
            - `--input_yellow=gs://<GCS_BUCKET>/pq/yellow/2021/*/`
        C. Third argument:
            - `--output=gs://<GCS_BUCKET>/report-2021

6. Click 'Submit'


## Alternate Submission Through CLI / GCLOUD SDK

1. Click 'Configuration' Tab
2. Scroll down and click 'EQUIVALENT REST'
3. Copy. (Or just copy the commands below)
    - cluster is the cluster you just created
    - region. Check your Bucket region.
        - It will be just under your Bucket name
    - Next set your Project ID.
        - This can be found in your dashboard.
        - This is not shown in the instructions,
          but I kept getting a flag error that my project wasn't set.
          You may not need this set.
    - Next is the location of the file
    - Next, paste the arguments copied from the 'EQUIVALENT REST'

```
gcloud dataproc jobs submit pyspark \
    --cluster=<CLUSTER_NAME> \
    --region=us-central1 \
    --project=<PROJECT_ID> \
    gs://<GCS_BUCKET>/code/gcs_spark_sql.py \
    -- \
        --input_green=gs://<GCS_BUCKET>/pq/green/2020/*/ \
        --input_yellow=gs://<GCS_BUCKET>/pq/yellow/2020/*/ \
        --output=gs://<GCS_BUCKET>/report-2020/


Whoops! We get an error.

## Add Permission

1. In GCS, run a search for 'IAM'
2. Click IAM & Admin
3. Click on the edit icon on the right of your *best* service account. I chose the one with already the most permissions. Which was the one I used for DBT.
Add 
4. Add role 'DataProc Administrator'
5. Re-run the `gcloud dataproc jobs submit pyspark` command again.
6. Success!

---

* The following is the error when running the gcloud dataproc job without the project flag:

```
ERROR: (gcloud.dataproc.jobs.submit.pyspark) The required property [project] is not currently set.
It can be set on a per-command basis by re-running your command with the [--project] flag.

You may set it for your current workspace by running:

  $ gcloud config set project VALUE

or it can be set temporarily by the environment variable [CLOUDSDK_CORE_PROJECT]
-bash: --input_green=gs://<GCS_BUCKET>/pq/green/2020/*/: No such file or directory
```
