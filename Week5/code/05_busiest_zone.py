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


df_zone_counts = spark.read.parquet('report/zone_traffic')

df_zone_counts.show()

df_zone_counts.createOrReplaceTempView('june')

busiest_zone = spark.sql(
"""
SELECT 
    distinct_zone,
    zone_count
FROM
    june
ORDER BY
    zone_count DESC
"""
)

busiest_zone.show()
