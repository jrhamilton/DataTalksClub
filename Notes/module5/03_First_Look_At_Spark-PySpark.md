# First Look at Spark / PySpark

``$ jupyter notebook``
---
```
import pyspark
from pyspark.sql import SparkSession
import pandas as pd
import sys
-
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
-
wget https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-02.parquet
-
```

We are using parquet files instead of plain csv files like in the video so we need to do a little transformation before moving on.

```
pf = 'fhvhv_tripdata_2021-01.parquet'
df = pd.read_parquet(pf)
df.to_csv('tripdata.csv', index=False, sep=',')
-
f = pd.read_csv('tripdata.csv', usecols=[0,1,5,6,7,8,19])
f.to_csv("tripdata.csv", index=False)
-
```

As you can see, I also removed extra columns by just keeping the required columns in this lesson.
For me, the parquet had many more columns than Alexey showed in his wget of the csv file.
I will also be working with one less column as mine didn't have the last column which I believe is unnecessary anyways.

Now we can continue on..

```
!wc -l 'tripdata.csv'
-
df = spark.read \
    .option("header", "true") \
    .csv('tripdata.csv')
-
df.head(5)
-
df.schema
-
!head -n 101 tripdata.csv > head.csv
-
!head -n 10 head.csv
-
!wc -l head.csv
-
df_pandas = pd.read_csv('head.csv')
-
df_pandas.dtypes
-
spark.createDataFrame(df_pandas).show()
-
spark.createDataFrame(df_pandas).schema
-
```

The schema shown above is for Scala types.
We need to transform to Python types and change longs to integers for better efficiency.
Example: Longs are 8 bytes. Integers are 4 bytes.

```
from pyspark.sql import types
-
schema = types.StructType([
        types.StructField('hvfhs_license_num', types.StringType(), True),
        types.StructField('dispatching_base_num', types.StringType(), True),
        types.StructField('pickup_datetime', types.TimestampType(), True),
        types.StructField('dropoff_datetime', types.TimestampType(), True),
        types.StructField('PULocationID', types.IntegerType(), True),
        types.StructField('DOLocationID', types.IntegerType(), True)
    ])
-
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('tripdata.csv')
-
df.show()
-
df.head(10)
```
---

## Spark Cluster
- GCS
    - Contains files
- Spark Cluster
    - contains a bunch of computational workers.
- Each Spark Cluster executor gets a GCS file to work on.
- Once an executor it may move on to another file.

## Repartition
Can take one large file and split into many partitions

```
-
df = df.repartition(24)
-
df.write.parquet('fhvhv/2021/01/')
-
```

Can take as many partitions as there are ~files in a folder~

Check progress from above in http://localhost:4040/jobs

The last execution will create fhvhv/2021/01 directories.

`ls -lh fhvhv/2021/01/` shows 24 parts

To run the last write command without it throwing AnalysisException, run with overwrite:
`df.write.parquet('fhvhv/2021/01/', mode='overwrite')`
