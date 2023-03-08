# GroupBy in Spark
Video: https://www.youtube.com/watch?v=9qrDsY_2COo&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=50

## Group By

Paritions [3 partitions]
    - executors filter group partition

```
df_green_revenue = spark.sql("""
SELECT
    date_trunc('hour', lpep_pickup_datetime) AS hour,
    PULocationID AS zone,

    SUM(total_amount) AS amount,
    COUNT(1) AS number_records
FROM
    green
WHERE
    lpep_pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY
    1, 2
ORDER BY
    1, 2
""")
```

The GROUPYB is y hour and zone.

The example with 3 partitions would be like:
    1. Executor1 = h1, z1, 100, 5
        - Grouped by Hour 1 and Zone 1 would be 100 amount and 5 records
    2. Executor1 = h2, z2, 200, 10
        - Grouped by Hour 1, Zone 2 would be 200 amount and 10 records

### Group By Stage 2: Reshuffling
- External Merge Sort


## Jypter Notebook
Create new notebook


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
df_green.createOrReplaceTempView('green')
-
df_green_revenue = spark.sql("""
SELECT
    date_trunc('hour', lpep_pickup_datetime) AS hour,
    PULocationID AS zone,

    SUM(total_amount) AS amount,
    COUNT(1) AS number_records
FROM
    green
WHERE
    lpep_pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY
    1, 2
ORDER BY
    1, 2
""")
-
df_green_revenue.show()
-
df_green_revenue.write.parquet('data/report/revenue/green')
-


## Review, a Second Time
Group By
Partitions
The Executors actualle execute the following query:
```SQL
    date_trunc('hour', lpep_pickup_datetime) AS hour,
    PULocationID AS zone,
```
- Executor executes filtering like removing dates before 2020.
- Executor executes one partition at a time.


### Stage 2
The Executor takes each iteration an iteration of each partition it executes and reshuffles.
- The key for each record is the group by directive (h1, z1) etc, hten reshuffles to another distrubution (or new partition) which results in less records or parititons.
The keys make sure the same row are in each parititon.
I think the idea is that the finishing number of paritions matches the number of executors

1000 initial partitions -> 10 executors -> 10 partitions
This is what I gather at the moment.

## Back to Jupter
Now do the same for yellow.

Now repartition to combine in fewer files:

```SQL
df_green_revenue \
    .repartition(20) \
    .write.parquet('data/report/revenue/green', mode='overwrite')
```

And do the same for yellow:

```SQL
df_yellow_revenue \
    .repartition(20) \
    .write.parquet('data/report/revenue/yellow', mode='overwrite')
```


### Check files
`ls data/reports`
`ls lhR data/reports` to check size of each partition.
