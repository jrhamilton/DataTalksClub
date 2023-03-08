# Spark DataFrames

We continue from last session with Jupyter:

```
-
df = spark.read.parquet('fhvhv/2021/01/')
-
df
-
df.printSchema()
-
```

Now we can select specific columns from out DataFrame. And then maybe filter on License Number

```
df.select('pickup_datetime', 'dropoff_datetime', 'PuLocationId', 'DOLocationID') \
    .filter(df.hvfhs_license_num == 'HV0003') \
    .show()

## Actions vs Transformations
### Transformations
- Selecting columns
- Filtering
- joins, group by, etc.
- Applying transformation to each columns
- For example, we have original dataframe with select. Then with filter. Then show does the trnaformation.
    - Select -> Lazy
    - Filter -> Lazy
    - show() -> Action

### Action
- show(), take(), head()
- Write


## Side Note
`from pyspark.sql import functions as F`
Now write `F.` then tab and can seek functions scroll

```
from pyspark.sql import functions as F
-
df \
    .withColumn('pickup_datetime', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_datetime', F.to_date(df.dropoff_datetime)) \
    .show()
-
```

The previous command overwrites pickup_datetime and dropoff_datetime. The next command will not:

```
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .show()
-
```

Now there are 2 new columns: pickup_date and dropoff_date.

Now use a select then Action with show():

```
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .select('pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
-
```


## Define Function
Let's say that have a job that is going crazy:

```
def crazy_stuff(base_num):
    num = int(base_num[1:])
    if num % 7 == 0:
        return f's/{num:03x}'
    elif num % 3 == 0:
        return f'a/{num:03x}'
    else:
        return f'e{num:03x}'
-
```

This shows how SQL may not be the proper job but rather Spark could be useful.

```
crazy_stuff('B02884')
-
crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())
-
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .withColumn('base_id', crazy_stuff_udf(df.dispatching_base_num)) \
    .select('base_id', 'pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
_
```

As shown, the withColumn methods are Transformation and therefor delayed until show() transformation is rendered.

With Spark we can do Tranformations while querying and also test.

With Spark we can have SQL and UDF
