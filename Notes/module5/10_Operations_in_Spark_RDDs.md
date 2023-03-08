# Operations in Spark RDDs
VIDEO: https://www.youtube.com/watch?v=Bdu-xIrF3OM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=52

RDD: Resillient Distributed Datasets

Row is a special object for building dataframes

```python
import pyspark
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql import types

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

start = datetime(year=2020, month=1, day=1)

revenueRow = namedtuple('RevenueRow', ['hour', 'zone', 'revenue', 'count'])

def filter_outliers(row):
    return row.lpep_pickup_datetime >= start

def prepare_for_grouping(row):
    hour = row.lpep_pickup_datetime.replace(minute=0, second=0, microsecond=0)
    zone = row.PULocationID
    key = (hour, zone)

    amount = row.total_amount
    count = 1
    value = (amount, count)

    return (key, value)

def calculate_revenue_reducer(left_value, right_value):
    left_amount, left_count = left_value
    right_amount, right_count = right_value

    output_amount = left_amount + right_amount
    output_count = left_count + right_count

    return (output_amount, output_count)

def unwrap(row):
    return revenueRow(
        hour = row[0][0],
        zone = row[0][1],
        revenue = row[1][0],
        count = row[1][1]
    )

df_green = spark.read.parquet('data/pq/green/*/*')

df_result = rdd \
    .filter(filter_outliers) \
    .map(prepare_for_grouping) \
    .reduceByKey(calculate_revenue_reducer) \
    .map(unwrap) \
    .toDF()
```

run `df_result.schema` to show and clipboar the schema an d reconfig them in vim like done before. Changing longs to ints and such.

transform:
```
StructType([StructField('hour', TimestampType(), True), StructField('zone', LongType(), True), StructField('revenue', DoubleType(), True), StructField('count', LongType(), True)])
```
to:
```
types.StructType([
    types.StructField('hour', types.TimestampType(), True),
    types.StructField('zone', types.IntegerType(), True),
    types.StructField('revenue', types.DoubleType(), True),
    types.StructField('count', types.IntegerType(), True)
    ])
```

before `df_result = rdd...`

```
result_schema = types.StructType([
    types.StructField('hour', types.TimestampType(), True),
    types.StructField('zone', types.IntegerType(), True),
    types.StructField('revenue', types.DoubleType(), True),
    types.StructField('count', types.IntegerType(), True)
    ])
```

Now change the `tdDF(...)` in `df_result = rdd...`

```
df_result = rdd \
    .filter(filter_outliers) \
    .map(prepare_for_grouping) \
    .reduceByKey(calculate_revenue_reducer) \
    .map(unwrap) \
    .toDF(result_schema)

df_result.schema

df_result.show()

df_result.write.parquet('tmp/green-revenue')
```

This transformation reshuffles all same keys (H, Z) to same partition.

In the RDD we:
  1. first filtered out filter_outliers (the WHERE statement in SQL)
  2. second was map
  3. reduce
  4. map the unwrap
  5. to dataframe

This shows how things are done many years ago but not necessary now that we have dataframes but can be useful.
