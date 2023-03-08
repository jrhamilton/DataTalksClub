# Introduction to Spark

## Apacke Spark
- A multi-language engine used to executing data engineering, data science and machine learning on single node machines or clusters.
- An open source analytics engine for large scale data processing
- Data processing engine
- Written in Scala
- PySpark
- Used for executing Batch Jobs
- Also used for streaming

## When to use Spark
- Used when your data is in a Data Lake
- Spark will pull data from data lake. Do some processing. Send back to data lake.
- When using SQL is not easy.
- Big files
- Have a lot of unit tests
- Uses a lot of models

## If you can express with SQL
- Then go with Presto/Athena

## Example Flow
Raw Data -> DL -> SQL Athena -> Spark -> { { Spark (Apply ML) -> Lake } || { Python (Train ML) -> Model -> Smaprk -> Lake } }
