# Joins in Spark

Yellow and Green may both have 4 columns

Join to table with columns:
h,z,Ry,Hy,Rq,Hq,*|

## Back to Jupyter
Using same file from last session:

Join green and yellow on hour and zone using outer because outer join because we may have record in green but not in yellow and we want to have zeros in yellow and when something is yellow but not green we want to have zeros in green part.

```
df_join = df_green_revenue.join(df_yellow_revenue, on=['hour','zone'], how='outer')
-
```

This returns 2 sets of 'amount' and 'number_records' without knowing which is which.

Add a withColumn method before the join method
ADD THIS BEFORE `df_join = df_green_revenue(..)`

```
df_green_revenue_tmp = df_green_revenue \
    .withColumnRenamed('amount', 'green_amount') \
    .withColumnRenamed('number_records', 'green_number_records')

df_yellow_revenue_tmp = df_yellow_revenue \
    .withColumnRenamed('amount', 'yellow_amount') \
    .withColumnRenamed('number_records', 'yellow_number_records')
-
```

Now change the join statement to use these new variables.

```
df_join = df_green_revenue_tmp.join(df_yellow_revenue_tmp, on=['hour','zone'], how='outer')
-
df_join
-
```

The last `df_join` will show if we have the columns we want.

Now write:

```

## External Merge Sort
* Yellow with 2 partitions
    - Each with a bunch of records
    - But yellow will only have one key 1, and one key 2..
    - But partition executor may have key1..key100
    - Executor 2 may have key101..key200 and so forth
* Green with 2 partitions
    - Each with a bunch of records
    - Green will have only one key 1, and one key 2..
    - But partition executor may have key1..key100
    - Executor 2 may have key101..key200 and so forth
* Reshuffle to new partition:
    - partition 1 has (K1, Y1), (K4, G3) ..
    - partition 2 has (K2, Y2), (K2, G1) ...
    - partition 3 has (K3, Y1), (K3, G5) ...
* JOIN
    - From partition 1:
        * (K1, Y1, X), (K4, X, G3)
    - From Partition 2:
        * (K2, Y2, G1)
    - FROM Partition 3:
        * (K3, Y1, G)


## Back to Jupyter Notebook
We didn't read from the data we saved, we computed data on the fly.

Instead let's load the data and materialize the results because maybe we want a dashboard or something useful.

So add above the `df_<COLOR>revenue_temp = ...` statements:

```
df_green_revenue = spark.read.parquet('data/report/revenue/green')
df_yellow_revenue = spark.read.parquet('data/report/revenue/yellow')
-
```

We can look at another case where one table is large and the other is small.

First read the DataFrame:

```
df_join = spark.read.parquet('data/report/overwrite/total')
-
df_join.show()
-
```

```
df_zones = spark.read.parquet('zones/')
-
```

My setup shows this directory was never made. Looks like it was 03_test.pynb from the video. I must have somehow missed this one or didn't complete it. Need to go back and look.

I just hopped into ipython and created the parquet from the taxi_zone.csv download. I follow along by catching a glimpse of the related Jupyter Notebook quickly seen in the video. Turns out it is just a simple download and write to parquet:

```python
n [1]: import pyspark

In [2]: pyspark.__file__

In [3]: from pyspark.sql import SparkSession

n [5]: spark = SparkSession.builder \
   ...:     .master("local[*]") \
   ...:     .appName('test') \
   ...:     .getOrCreate()

In [7]: !wget https://github.com/DataTalksClub/nyc-tlc-data/r
   ...: eleases/download/misc/taxi_zone_lookup.csv

In [10]: df = spark.read \
    ...:     .option("header", "true") \
    ...:     .csv('taxi_zone_lookup.csv')

In [12]: df.write.parquet('zones')
```


Now back to the Jupyter file:

```
df_zones = spark.read.parquet('zones/')
-
df_zones.show()
-
```

Now let's join the 2 tables, df_join and df_zones.

```
df_result = df_join.join(df_zones, df_join.zone == df_zones.LocationID)
-
df_result.drop('LocationID').show()
-
df_result.drop('LocationID', 'zone').write.parquet('temp/revenue/zones')
-
```

Added `df_join.zone == df_zones.LocationID` because the columns on each table have different names.
Also drop LocationID since we don't need that in view.
Write to parquet temporarily because show() only shows first 20 results and we want to work on all results.
Drop zone as well since it has different types in each the table (int and string).

---

Because the zones is a very small dataframe, the every executor gets a braodcast of the taxi_zone table which is much faster than doing Merge Sort.
