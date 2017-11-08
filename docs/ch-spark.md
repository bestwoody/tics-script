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
       | Scan (Load ch.so, JNI)       |
       |                              |
+------|------------------------------|---------+
|      |                              |         |
|      |         CH Lib (ch.so)       |         |
|      |                              |         |   +-----------------+
|      |                              |         |   |                 |
|      +-------------------------------------------->  Latch service  |
|      |                              |         |   |                 |
|      |                              |         |   +--------------^--+
|  +---v------------------------------|------+  |                  |
|  |                                  |      |  |                  |
|  |  CH Connector          +---------+---+  |  |                  |
|  |                        | Arrow data  |  |  |                  |
|  |                        +---------^---+  |  |                  |
|  |                                  |      |  |                  |
|  |                               Encode    |  |                  |
|  |                                  |      |  |                  |
|  +---+------------------------------|------+  |                  |
|      |                              |         |                  |
|  +---+------------------------------|------+  |                  |
|  |                                  |      |  |                  |
|  |  CH Engine:            +---------+---+  |  |                  |
|  |    MergeTree           | CH Columns  |  |  |   +-----------+  |
|  |                        +---------^---+  |  |   |           |  |
|  |                                  |      +------>  Database |  |
|  |                                Query    |  |   |  (files)  |  |
|  |                                         |  |   |           |  |
|  +-----------------------------------------+  |   +-----^-----+  |
|                                               |         |        |
+-----------------------------------------------+         |        |
                                                          |        |
                                                          |        |
+---------------------------+                             |        |
|                           |                             |        |
|  Binlog Syncer:           |                             |        |
|    One process            |                             |        |
|                           |                             |        |
|  +---------------------+  |                             |        |
|  |                     |  |                             |        |
|  |  CH Lib (ch.so):    +--------------------------------+--------+
|  |    Store engine     |  |
|  |    Connector        |  |
|  |                     |  |
|  +-------------+-------+  |
|                |          |
|  +-------------+-------+  |
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

## Code bases and modules relation
```
+---------------------------------------------+
|                                             |
|  Community version modules                  |
|                                             |
|  +-----------------+   +-----------------+  |
|  |  CH             |   |  Spark          |  |
|  +---^-------------+   +-----------------+  |
|      |                                      |
+------|--------------------------------------+
       |
+------|--------------------------------------+
|      |                                      |
|      | Keep syncing                         |
|      |                                      |
|  +---v-----------------------------------+  |
|  |  Patched CH:                          |  |
|  |    Update/delete support              |  |
|  |    Wire SQL layer to Connectors       |  |
|  +---------------------------------------+  |
|                                             |
|  +---------------------------------------+  |
|  |  Magic API Connector:                 |  |
|  |    Magic protocol (write/scan)        |  |
|  |    Arrow format encoding              |  |
|  |    Export API to '.so'                |  |
|  +---------------------------------------+  |
|                                             |
|  Repo of CH engine (ch.so)                  |
|                                             |
+---------------------------------------------+

+---------------------------------------------+
|                                             |
|  Common modules for all engines             |
|                                             |
|  +---------------------------------------+  |
|  |  Magic Writer:                        |  |
|  |    Link to <engine>.so                |  |
|  |    Read Binlog                        |  |
|  |    Use Magic API, write to engine     |  |
|  +---------------------------------------+  |
|                                             |
|  +---------------------------------------+  |
|  |  Spark-Magic Connector:               |  |
|  |    Jar package for Spark              |  |
|  |    Link to <engine>.so with JNI       |  |
|  |    Wire Spark to Magic protocol       |  |
|  |    Decode Arrow format to RDD         |  |
|  +---------------------------------------+  |
|                                             |
|  +---------------------------------------+  |
|  |  Latch Service:                       |  |
|  |    Can be called by rpc               |  |
|  |    Acquire/release data latches       |  |
|  |    TBD                                |  |
|  +---------------------------------------+  |
|                                             |
+---------------------------------------------+
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