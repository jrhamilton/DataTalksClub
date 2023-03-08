#!/usr/bin/env python

import os
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types


spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()


df_june = spark.read.parquet('fhvhv/2021/06/')


timeF = "yyyy-MM-dd'T'HH:mm:ss"


@F.udf(returnType=types.DoubleType())
def toTripDuration(pickup, dropoff):
    diff = dropoff - pickup
    diff_hour = diff / (60 * 60)
    return diff_hour


df_june.withColumn("trip_duration",
                   toTripDuration(F.unix_timestamp("pickup_datetime",
                                                   format=timeF),
                                  F.unix_timestamp("dropoff_datetime",
                                                   format=timeF))) \
    .select(F.max('trip_duration')) \
    .show(truncate=False)
