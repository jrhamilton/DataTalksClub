# Internals of BigQuery
https://www.youtube.com/watch?v=eduHi1inM4s&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=28

BigQuery stores data into separate storage called Collosus.

## Collosus
- Stores cheap storage
- Stores data in columnal format
- Advantage because BQ separated storage from compute
    - has signifcant less cost
    - generally only pay for storage in Collosus.
    - Most cost is running queries or reading data
        - Basically compute
        - Thus has a lot of advantages for BQ
- Because on different hardware how do they communicate?
    - If network is bad can be a disadavantage
    - This is where Jupiter network comes into play
    - Jupiter nw is inside BQ data centers
        - and provides 1 TB network speed
        - Huge advantage separate hardware
- Dremmel
    - Query execution engine.
    - Separates queyr into 3 structure.
    - Separates in a way that each node can execute a subquery
    - BQ uses column oriented structure. Time(2:50)
    - Time(4:00) - SHOWS HOW DREMMEL WORKS.
    - Dremmel Leaf Nodes is what actually talks to Colussus database
    - Dremmel divides queries into smaller chunks and propagates them to the leaf nodes
        - Starts with root server and query statement
        - Then goes to mixiers
        - Then to Leaf nodes.
        - Then Leaf nodes hands off to Collosus
- Borg
    - Orchestration
    - Precurser to Kubernetes

Reference Links:
- https://cloud.google.com/bigquery/docs/how-to
- https://research.google/pubs/pub36632/
- https://panoply.io/data-warehouse-guide/bigquery-architecture/
- https://www.goldsborough.me/distributed-systems/2019/05/18/21-09-00-a_look_at_dremel/
