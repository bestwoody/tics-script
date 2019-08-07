# The concurrent concept of query execution

## A single query between the two clusters
```
+---------------------------------------------+  +--------------+  +--------------+
| +-----------+  +-----------+  +-----------+ |  |              |  |              |
| | RDD       |  | RDD       |  | RDD       | |  |              |  |              |
| | Partition |  | Partition |  | Partition | |  |              |  |              |
| +-----+-----+  +-----+-----+  +-----+-----+ |  |              |  |              |
|       |              |              |       |  |              |  |              |
|       |+-------------+              |       |  |              |  |              |
|       ||+---------------------------+       |  |              |  |              |
|       |||                                   |  |              |  |              |
|       |||                                   |  |              |  |              |
|       |||                                   |  |              |  |              |
| +-----+++-----+                             |  |              |  |              |
| | CH JVM SDK  |                             |  |              |  |              |
| +-----+++-----+                             |  |              |  |              |
|       |||                                   |  |              |  |              |
|       |||                      Spark Worker |  | Spark Worker |  | Spark Worker |
+-------|||-----------------------------------+  +-------+------+  +-------+------+
        |||                                              |                 |
        ||| +--------------------------------------------+                 |
        ||| |                                                              |
        ||| | +------------------------------------------------------------+
        ||| | |
        ||| | | TCP Connections, (globally) same query, same query id
        ||| | |
        ||| | |                                        . . .             . . .
        ||| | |                                Other connections from Spark Workers
        ||| | |                                        | | |             | | |
        ||| | |                                        | | |             | | |
+-------|||-|-|-------------------------------+  +-----|-|-|----+  +-----|-|-|----+
|       ||| | |                               |  |              |  |              |
| +-----|||-|-|---+  +---------------+        |  |              |  |              |
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
[g]: Global singleton corresponding to a query
[n]: Node singleton corresponding to a query
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
        [1] SparkRDD
            [*] SparkRDDPartition
                [g] QueryString
                [n] QueryToken
                [1] CHExecutor
                    [1] TCPConnection(to CH)
                    [*] CodecThread

[G] CHCluster
    [*] CHNode
        [N] SessionManager
            [n] Session
                [n] QueryToken
                [*] TCPConnection(from RDDPartition)
                [*] BlockStream
                [n] EncodeThreadPool
                    [*] EncodeThread
        [N] QueryExecutor
```

## Current implement
* In running config, partition number should not be greater than cpu core number
    * Too much partitions will be executed batch by batch, not all in the same time
* The whole system still CPU bound, not IO buond
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
