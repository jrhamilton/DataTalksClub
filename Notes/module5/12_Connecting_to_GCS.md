# Connecting to Google Cloud Storage
https://www.youtube.com/watch?v=Yyz293hBVcQ&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=54

We have our pq data in the code file and we first need to send it to the Google Cloud Storage.

```
$ gsutil -m cp -r data/pq/ gs://GCS_BUCKET/pq
```

The '-m' flag tells the cpu to use all threads

Once we confirm that our files are completely uploaded to our GCS Bucket,
we are advised to follow Alvin Do's instructions to connect pyspark to GCS by.


1. Be in the code directory
2. `mkdir lib`
3. First, we download a jar shown in: https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage#clusters
4. We will download the connector from the example which is: gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar
    - This is the one that Alexey uses. Not actually the one from the web page.
5. `gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar gcs-connector-hadoop3-2.2.5.jar`

## In Jupyter
```
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import os
-
credentials_location = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
GCS_BUCKET = os.environ['GCS_BUCKET']
-
```

For the usual `spark = sparkSession.builder ...`
Instead of sparkSession, we will be using sparkConf() before sparkSession.
We will modify it a bit this time.
We will be making the `.set` method a few times for spark.hadoop configurations:

```
conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName('test') \
        .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)
```

Now we create sparkContext which was before created with sessionBuilder.

In this case sparkContext will be telling GCS to use the jar implementation when command prefixed with `gs:` and use `credentials_location`.

```
sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
```

Now create good ole' faithful sparkSession.builder.
Except this time a bit modified.
`.setMaster` and `.setAppName` were already set in `SparkConf()`

```
spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()
```

Now execute read of parquet file

```
df_green = spark.read.parquet(f'gs://{GCS_BUCKET}/pq/green/*/*')
df_green.show()
df_green.count()
```

Result of count shoud be 2304517
