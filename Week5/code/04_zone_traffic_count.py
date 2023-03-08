#!/usr/bin/env python

#import sys
import os
import sys
import pyspark
pyspark.__file__
from pyspark.sql import SparkSession
#import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types


spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()


df_june = spark.read.parquet('fhvhv/2021/06/')


df = spark.read \
    .option("header", "true") \
    .csv('taxi+_zone_lookup.csv')

df.write.parquet('zones', mode='overwrite')

df_zones = spark.read.parquet('zones')

df_zones.show()


#df_join = df_june.join(df_yellow_revenue, on=['hour','zone'], how='outer')

df_join = df_june.join(df_zones, df_june.PULocationID == df_zones.LocationID)

#df_result.drop('LocationID').show()

#df_result.drop('LocationID', 'zone').write.parquet('temp/revenue/zones')


df_zone_counts = df_join.groupBy('zone') \
        .count() \
        .select(F.col("zone").alias("distinct_zone"),F.col("count").alias("zone_count"))


df_zone_counts.show()

df_zone_counts.write.parquet('report/zone_traffic', mode='overwrite')
