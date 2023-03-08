# Spark RDD MapPartition

rdd.mapPartitions takes every element of RDD and creates new elements of RDD

RDD Parition -> map Partition -> New RDD Partition

Very convenient because if we have a very large partition maybe 1 tb with partitions with 100 mbs each. Process each partition and so on.

Spark can take big data lake and chunk into smaller pieces for mapParition to work on.

## To Jupyter
From where we left off in last RDD session:

```
columns = ['VenderID', 'lpep_pickup_datetime', 'PULocationID', 'DOLocationID', 'trip_distance']
-
duration_rdd = df_green \
    .select(columns) \
    .rdd
-
def apply_model_in_batch(partition):
    cnt = 0
    for row in partition:
        cnt = cnt + 1

    return [cnt]
-
rdd.mapPartitions(apply_model_in_batch).collect()
-
```

The last command will take some time because it needs to go through each partition.

Executors will complete before others

Now in the `def apply_model_in_batch(rows)` method, add a dataframe application to the method.

```
rows = duration_rdd.take(10)
-
def apply_model_in_batch(rows):
    df = pd.DataFrame(rows, columns=columns)
    cnt = len(df)
    return [cnt]
-
duration.rdd.mapPartition(apply_model_in_batch).collect()
```

Ceate a prediction function

above `def apply_model_in_batch`

```
model = ...

def model_predict(df):
    y_pred = df.trip_distance * 5
    return y_pred

def apply_model_in_batch(rows):
    df = pd.DataFrame(rows, columns=columns)
    predictions = model_predict(df)
    df['predicted_duration'] = predictions

    for row in df.itertuples():
        yield row
-
duration_rdd.mapPartitions(apply_model_in_batch).toDF().show()
-
```
