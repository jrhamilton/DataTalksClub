# Preparing Yellow and Green Taxi Data
>> OPTIONAL
VIDEO: https://www.youtube.com/watch?v=CI3P4tAtru4&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=47

GitHub Script: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_5_batch_processing/code/download_data.sh

## Just Watch the Video
The video is a pleasant viewing and refresher on some Bash scripting. Please watch.

`set -e` commands the script to interupt execution when a non-zero code returns. This can happen with 404 return code.

`zcat file.csv.gz` is cat for compressed files.

## Jupyter Notebook
Open new file with Jupyter Notebook.
Use Jupyter file from previous video as base.
Skip over wget method because we will be working on files downlaoded from the bash script.

`$ jupyter notebook`
```
import pyspark
from pyspark.sql import SparkSession
import pandas as pd
-
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
-
```

Now we change it up a bit and work on our local data/raw directory and files we just downloaded.

Also note that we've downloaded \*.csv.gz files and not \.parquet files.

```
df_green = spark.read \
    .option("header", "true") \
    .csv("data/raw/green/2021/01/")
-
df_green_pd = pd.read_csv('data/raw/green/2021/01/green_tripata_2021_01.csv.gz', nrows=1000)
-
spark.createDataFrame(df_green_pd).schema
-
```

Now use vim to transform the schema and past back:

```
from pyspark.sql import types
-
green_schema = types.StructType([
    types.StructField('VendorID', types.IntegerType(), True),
    types.StructField('lpep_pickup_datetime', types.TimestampType(), True),
    types.StructField('lpep_dropoff_datetime', types.TimestampType(), True),
    types.StructField('store_and_fwd_flag', types.StringType(), True),
    types.StructField('RatecodeID', types.IntegerType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('passenger_count', types.LongType(), True),
    types.StructField('trip_distance', types.DoubleType(), True),
    types.StructField('fare_amount', types.DoubleType(), True),
    types.StructField('extra', types.DoubleType(), True),
    types.StructField('mta_tax', types.DoubleType(), True),
    types.StructField('tip_amount', types.DoubleType(), True),
    types.StructField('tolls_amount', types.DoubleType(), True),
    types.StructField('ehail_fee', types.DoubleType(), True),
    types.StructField('improvement_surcharge', types.DoubleType(), True),
    types.StructField('total_amount', types.DoubleType(), True),
    types.StructField('payment_type', types.IntegerType(), True),
    types.StructField('trip_type', types.IntegerType(), True),
    types.StructField('congestion_surcharge', types.DoubleType(), True)])
-
```

Now use Spark to read, repartition and write to parquet.
Do a for loop to iterate through each month of the year.

```
year = 2021

for month in range(1, 13):
    print(f'processing data for {year}/{month}')

    input_path = f'data/raw/green/{year}/{month:02d}/'
    output_path = f'data/pq/green/{year}/{month:02d}/'

    df_green = spark.read \
        .option("header", "true") \
        .schema(green_schema) \
        .csv(input_path)

    df_green \
        .repartition(4) \
        .write.parquet(output_path)
-
```

Repeat for `year = 2020`

Now for the yellow datasets.

Now do same thing with yellow dataset:

```
df_yellow_pd = pd.read_csv('data/raw/yellow/2021/01/yellow_tripdata_2021_01.csv.gz', nrows=1000)
-
spark.createDataFrame(df_yellow_pd).schema
-
types.StructType([
    types.StructField('VendorID', types.IntegerType(), True),
    types.StructField('tpep_pickup_datetime', types.TimestampType(), True),
    types.StructField('tpep_dropoff_datetime', types.TimestampType(), True),
    types.StructField('passenger_count', types.IntegerType(), True),
    types.StructField('trip_distance', types.DoubleType(), True),
    types.StructField('RatecodeID', types.IntegerType(), True),
    types.StructField('store_and_fwd_flag', types.StringType(), True),
    types.StructField('PULocationID', types.IntegerType(), True),
    types.StructField('DOLocationID', types.IntegerType(), True),
    types.StructField('payment_type', types.IntegerType(), True),
    types.StructField('fare_amount', types.DoubleType(), True),
    types.StructField('extra', types.DoubleType(), True),
    types.StructField('mta_tax', types.DoubleType(), True),
    types.StructField('tip_amount', types.DoubleType(), True),
    types.StructField('tolls_amount', types.DoubleType(), True),
    types.StructField('improvement_surcharge', types.DoubleType(), True),
    types.StructField('total_amount', types.DoubleType(), True),
    types.StructField('congestion_surcharge', types.DoubleType(), True)])
-
year = 2021

for month in range(1, 13):
    print(f'processing data for {year}/{month}')

    input_path = f'data/raw/yellow/{year}/{month:02d}/'
    output_path = f'data/pq/yellow/{year}/{month:02d}/'

    df_yellow = spark.read \
        .option("header", "true") \
        .schema(yellow_schema) \
        .csv(input_path)

    df_yellow \
        .repartition(4) \
        .write.parquet(output_path)
-
```

Now Then do the same last jupyter command but for 2020 like done with green above.

Can check http://localhost:4040/jobs to see progress.
