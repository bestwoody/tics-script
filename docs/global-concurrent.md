# The concurrent concept of query execution

## A single query between the two clusters
```
+---------------------------------------------+  +--------------+  +--------------+
| +-----------+  +-----------+  +-----------+ |  |              |  |              |
| | RDD       |  | RDD       |  | RDD       | |  |              |  |              |
| | Partition |  | Partition |  | Partition | |  |              |  |              |
| +-----+-----+  +-----+-----+  +-----+-----+ |  |              |  |              |
|       |              |              |       |  |              |  |              |
|       +--------------+--------------+       |  |              |  |              |
|       |                                     |  |              |  |              |
|       | RDDs in the same node               |  |              |  |              |
|       |     share a same connection         |  |              |  |              |
|       |                                     |  |              |  |              |
| +-----+-------+                             |  |              |  |              |
| | CH JVM SDK  |                             |  |              |  |              |
| +-----+-------+                             |  |              |  |              |
|       |                                     |  |              |  |              |
|       |                        Spark Worker |  | Spark Worker |  | Spark Worker |
+-------|-------------------------------------+  +-------+------+  +-------+------+
        |                                                |                 |
        | +----------------------------------------------+                 |
        | |                                                                |
        | | +--------------------------------------------------------------+
        | | |
        | | | TCP Connections, (globally) same query, same query id
        | | |
        | | |                                          . . .             . . .
        | | |                                  The same connections from Spark Workers
        | | |                                          | | |             | | |
        | | |                                          | | |             | | |
+-------|-|-|---------------------------------+  +-----|-|-|----+  +-----|-|-|----+
|       | | |                                 |  |              |  |              |
| +-----|-|-|-----+  +---------------+        |  |              |  |              |
| | Session       |  | Session       |  ...   |  |              |  |              |
| +-------+-------+  +-------+-------+        |  |              |  |              |
|         |                  |                |  |              |  |              |
| +-------+-------+  +-------+-------+        |  |              |  |              |
| | CH Execution  |  | CH Execution  |  ...   |  |              |  |              |
| +-------+-------+  +-------+-------+        |  |              |  |              |
|         |                  |                |  |              |  |              |
|     +---+------------------+---+            |  |              |  |              |
|     |       CH Executor        |            |  |              |  |              |
|     +--------------------------+            |  |              |  |              |
|                                   CH Server |  |    CH Server |  |    CH Server |
+---------------------------------------------+  +--------------+  +--------------+
```

## Detail
```
[G]: Global singleton
[N]: Node singleton
[g]: Global singleton response to a query
[n]: Node singleton response to a query
[1]: Single instance in it's owner
[*]: Multi instances in it's owner

GlobalView:
    [g] QueryID
    [g] QueryString
    [G] SparkCluster
    [G] CHCluster

[G] SparkCluster
    [g] SparkDriver
        [g] SparkPlan
            [g] QueryID
    [*] SparkNode
        [*] SparkRDD
            [g] QueryString
            [n] QueryToken
            [n] CHExecutor
                [n] TCPConnection(to CH)
                [n] CodecThreadPool
                    [*] CodecThread

[G] CHCluster
    [*] CHNode
        [N] SessionManager
            [n] Session
                [n] QueryToken
                [*] TCPConnection(from RDD)
                [*] BlockStream
                [n] EncodeThreadPool
                    [*] EncodeThread
        [N] QueryExecutor
```

## Current implement
* Different from above, NOT share connections anymore
    * Eearly implement: partitions in the same node share the same connection.
    * It conflicted with yarn-module.
    * Benchmark shows that it not gain much performance.
    * Extra complicacy.
* In running config, partition number should not be greater than cpu core number
    * Too much partitions will be executed batch by batch, not all in the same time
* The whole system still CPU bond, not IO bond
    * Tunning concurrent args will improve the speed of TCP transfer interface
    * But no effect to the whole query elapsed time.
* It's fast
    * Compare to deterministic partitioning (IE: parquet), this session-module:
        * Eliminate the waiting between faster readers and slower readers.
        * All readers will finished in the same second.
        * Benchmark shows that it's faster (10-50%).
* It's confliced with Spark RDD retry.
    * Spark may only retry part of the partitions, it against the session module.
    * Worth it, so we don't support RDD retry.
