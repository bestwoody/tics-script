# Connect CH to Spark

## Processes and data relation
```
+-----------------------------------------------+
|                                               |
|  Spark                    +-------------+     |
|                           | Spark rows  |     |
|                           +---------^---+     |
|                                     |         |
|  +----------------------------------|------+  |
|  |                                  |      |  |
|  |  Spark-CH Client:              Decode   |  |
|  |    In process '.jar' package     |      |  |
|  |                                  |      |  |
|  +---+------------------------------|------+  |
|      |                              |         |
+------|------------------------------|---------+
       |                              |
       | Scan (TCP Connection)        |
       |                              |
+------|------------------------------|---------+
|      |                              |         |
|      |         CH Server            |         |
|      |                              |         |
|  +---v------------------------------|------+  |
|  |                                  |      |  |
|  |  CH Connector          +---------+---+  |  |
|  |                        | Arrow data  |  |  |
|  |                        +---------^---+  |  |
|  |                                  |      |  |
|  |                               Encode    |  |
|  |                                  |      |  |
|  +---+------------------------------|------+  |
|      |                              |         |
|  +---v------------------------------|------+  |
|  |                                  |      |  |
|  |  CH Engine:                      |      |  |
|  |    MergeTree                     |      |  |
|  |    MutableMergeTree              |      |  |
|  |                        +---------+---+  |  |
|  |                        | CH Columns  |  |  |
|  |                        +---------^---+  |  |
|  |                                  |      |  |
|  |                                Query    |  |
|  |                                  |      |  |
|  |                        +---------+---+  |  |
|  |                        | Database    |  |  |
|  |                        | (Files)     |  |  |
|  |                        +-------------+  |  |
|  |                                         |  |
|  +---^-------------------------------------+  |
|      |                                        |
+------|----------------------------------------+
       |
       |
+------+--------------------+
|                           |
|  Binlog Syncer:           |
|    One process            |
|                           |
|  +---------------------+  |
|  |                     |  |
|  |  Binlog Writer:     |  |
|  |    In process       |  |
|  |                     |  |
|  +-------------^-------+  |
|                |          |
+----------------|----------+
                 |
                 |
+----------------|----------+
|                |          |
|  TiDB          |          |
|       +--------+-------+  |
|       |  Binlog        |  |
|       +----------------+  |
|                           |
+---------------------------+

-->: Calling direction or data flow
```
