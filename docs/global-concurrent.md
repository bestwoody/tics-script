# The concurrent concept of query execution

## NOTE: Partitions in the same node NOT share the same connection anymore

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