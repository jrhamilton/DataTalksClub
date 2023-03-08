# SQL with Spark
VIDEO: https://www.youtube.com/watch?v=uAlp2VuZZPY&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=48

wget https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/main/week_5_batch_processing/code/05_taxi_schema.ipynb

Let's dig into the data we downloaded in the past sessions.

Open Juyper Notebook:

```
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
-
df_green = spark.read.parquet('data/pq/green/*/*')
-
df_yellow = spark.read.parquet('data/pq/yellow/*/*')
-
df_green.printSchema()
-
df_green.columns
-
df_yellow.columns
-
```

We have mis-match columns in the each columns.

Use set & set to find unions:


```
set(df_green.columns) & set(df_yellow.columns)
-
```

The pickup and dropoff datetime fields are diffrently named so the union method does not work here. Let's change the field names to a more general name.

```
df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')
-
```

This displays the columns in sorted order. So let's render it in another manner:

```
common_columns = []

yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_columns.append(col)
-
common_columns
-
```

Now they are in a normal order unionized.

```
from pyspark.sql import functions as F
-
df_green_sel = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))
-
df_yellow_sel = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))
-
```

Now get service type and count of each:

```
df_trips_data = df_green_sel.unionAll(df_yellow_sel)
-
```

Before running SQL commands on trips_data, we need to let Spark know what it is working with. So we will create a temporary view. Alexey uses createTempTable, but this is deprecated at the moment in favor of createOrReplaceTempView().

```
df_tripsdata.createOrReplaceTempView('tripsdata')
-
spark.sql("""
    SELECT * FROM tripsdata LIMIT 10;
""").show()
-
```

Let's do something more interesting:

```
spark.sql("""
    SELECT
        service_type,
        count(1)
    FROM tripsdata
    GROUP BY service_type;
""").show()
```

Now take the sql statement from the taxi_schema.ipynb file and transform it to our liking as shown in taxi_schema.sql

```
df_result = spark.sql("""
SELECT
    -- Reveneue grouping
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month,

    service_type,

    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance

FROM
    trips_data
GROUP BY
    1,2,3
""")
-
df_result.show()
-
```

Now we see the result with show() and see it complete, we can now write it to parquet file as report:

```
df_result.write.parquet('data/report/revenue/')
-
```

This returned inconsistent results from what was shown in the video. Another user (@JamieRV) on Slack mentioned that they were able to partition and get the correct or similar results so tried that with success:

```
df_result.repartition(24).write.parquet('data/report/revenue/', mode='overwrite')
-
```

This returned many files.

We can also use coalesce:

```
df_result.coalesce(10).write.parquet('data/report/revenue/', mode='overwrite')
-
```

This returns one file at 520K
