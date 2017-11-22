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
|  |  Spark-Magic Client:           Decode   |  |
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
|      |              +---------------|------+  |
|      |              |               |      |  |
|      |              |  CH Engine:   |      |  |
|  +---v---------+    |    MergeTree  |      |  |
|  |             |    |               |      |  |
|  |  Latch      |    |     +---------+---+  |  |
|  |  Service    +---->     | CH Columns  |  |  |
|  |             |    |     +---------^---+  |  |
|  |             |    |               |      |  |
|  +---^---------+    |             Query    |  |
|      |              |               |      |  |
|      |              |     +---------+---+  |  |
|      |              |     | Database    |  |  |
|      |              |     | (Files)     |  |  |
|      |              |     +-------------+  |  |
|      |              |                      |  |
|      |              +----------------------+  |
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

## CH wire point
* Patched-CH features
    * Input SQL instructions.
    * Output data.
    * Input compaction instructions.
    * Output compaction status.
    * Data visiblity control, hide uncompacted data (for speed).
    * Data consistency control, hide uncompleted writing data.
    * Data updating supported.
* Can be:
    * Storage layer, best choice when we dont't need CH as coprocesser.
    * AST layer, best choice when we need CH as coprocesser.
    * SQL layer, easiest one.
    * We start with SQL layer.
