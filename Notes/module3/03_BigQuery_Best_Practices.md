# BigQuery Best Practices

## BQ - Best Practice
* Cost reduction
    - Avoid SELECT *
        * select specific columns instead.
    - Price your queries before running them
    - Use clustered or partitioned tables
    - Use Streaming inserts with caution
    - materialize query results in stages
        * BQ always caches query results

* Query performance
    - Filter on paritioned columns
    - Denomoralizing data
    - Use nested or repeated columns
    - Use external data sources approprately
    - Don't use it, in case u want a high query performance
    - Reduce data before use a JOIN
    - Do not treat WITH clauses as prepared statements
    - Avoid verharding tables

* Query performance
    - Avoid javascript user-defined functions
    - Use approximate aggregation functions (HyperLogLog++)
    - Order Last for query operations to maximize performance
    - Optimize your join patterns
    - AS a best practice, place the table with the largest numbers of rows first, followed by the tabe with the fewst ros, and the palce the remaining tables by decreasing size.
