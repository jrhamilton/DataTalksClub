# Creating a Local Spark Cluster
Video: https://www.youtube.com/watch?v=HXBwSlXo5IA&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=55

1. Turn a Jupyter Notebook into a script
2. How to use Spark Submit to submit a Spark Cluster

```
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```

When we run the above, we create a local Spark Cluster then connect to it.

Shut down clusters in Juypter by clicking 'Running' and then shut down terminals and notebooks.
Now we can't connect to it

Instructions for running Spark in Standalone mode:
https://spark.apache.org/docs/latest/spark-standalone.html#installing-spark-standalone-to-a-cluster

```
cd $SPARK_HOME
./sbin/start-master.sh
```

Change SparkSession.builder to use spark://localhost in Juypter Notebook new file:

```
spark = SparkSession.builder \
    .master("spark://<HOSTNAME>:7077") \
    .appName('test') \
    .getOrCreate()
```

Running this but we have zero workers. Only a master. We need an executor. From the web page above follow instructions to start a worker (slave).

`./sbin/start-worker.sh <master-spark-URL>`

cd to the code directory.

`jupyter nbconvert --to=script 06_spark_sql.ipynb`

Then execute:

`python 06_spark_sql.py`


Now the fun part. Let's make this script configurable for the cli.

Add argparse library near the top of script:

`import argparse'

Then after import statments add the following argparsing statements:

```python
parser = argparse.ArgumentParser()

parser.add_argument('--input_green', requred=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output
```

Now change all the 'data/pq/<COLOR>/*/*' references to their respective args variables displayed above.
And replace with the output variable where necessary (the write command at the end of the script).

Now test the script with the following command:

```
python 06_spark_sql.py \
    --input_green=data/pq/green/2020/*/ \
    --input_yellow=data/pq/yellow/2020/*/ \
    --output=data/report-2020
```

Check that data/report-2020 was made with `ls data`

We need to make this more practible for remote work and probably need to use a better naming scheme for URL in setting sparksession.

We will look at spark-submit
DOCS: https://spark.apache.org/docs/latest/submitting-applications.html

```
URL=spark://$HOSTNAME:7077

spark-submit \
    --master=$URL \
    06_spark_sql.py \
         --input_green=data/pq/green/2021/*/ \
         --input_yellow=data/pq/yellow/2021/*/ \
         --output=data/report-2021
```

Now that we are passing the master URL, we can remove it from inside the script:
The SparkSession.builder method not looks like this:

```python
spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()
```

Now run the above `spark-submit` command in its entirity.

Check data for report-2021 file.

When running spark jobs with Prefect (or Airflow) this can provide some flexibilty.

Stop the workers:
```
./sbin/stop-worker.sh
./sbin/stop-master.sh
```
