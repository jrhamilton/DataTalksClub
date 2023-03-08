#!/usr/bin/env python
# coding: utf-8

# In[1]:


#import sys
#import os
import pyspark
from pyspark.sql import SparkSession
#import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types


# In[2]:


spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()


df = spark.read.parquet('fhvhv/2021/06/')

df.createOrReplaceTempView('june')
df.show()

df_june_trips_15 = spark.sql(
"""
SELECT
    COUNT(1)
FROM
    june
WHERE
    pickup_datetime >= '2021-06-15 00:00:00'
AND
    pickup_datetime < '2021-06-16 00:00:00'
""")

df_june_trips_15.show()
