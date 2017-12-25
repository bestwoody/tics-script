# The concurrent concept of query execution

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
