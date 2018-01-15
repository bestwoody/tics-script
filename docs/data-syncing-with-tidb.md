# Data syncing with TiDB (draft)

## Solution
```
+---------+   +---------+
|         |   |         |
| TiDB 1  |   | TiDB 2  |
|         |   |         |
+----+----+   +----+----+  ...
     |             |
+----v----+   +----v----+
| Binlog  |   | Binlog  |
| Pump 1  |   | Pump 2  |
+----+----+   +----+----+
     |             |
+----v----+   +----v----+
| Local   |   | Local   |
| Binlog 1|   | Binlog 2|
+----+----+   +----+----+
     |             |
     +------+------+
            |
       +----v----+
       | Binlog  | Sort Binlogs from
       | Drainer |  multi TiDBs by TS
       +----+----+
            |
       +----v----+
       | Kafka   | Or local files
       | Cluster |  for now
       +----+----+
            |
       +----v----+
       | CHSql   | Not support tables
       | Writer  |  without primekey
       +----+----+
            |
     +------+------+
     |             |
+----v----+   +----v----+
| CH      |   | CH      |
| Server 1|   | Server 2|
+----+----+   +----+----+  ...
     |             |
     +------+------+
            |
       +----v----+
       | Spark   | CHSpark
       | Cluster |
       +---------+
```

## Data consistency
* Use `at least once` message model
    * With network transportion, hard to keep `exactly once`.
    * Use by modules:
        * Binlog-Drainer
        * Kafka-Cluster
        * CHSql-Writer
    * Tables must have primekey for this model
        * To keep idempotent, so that duplicate data will be overrided and ignored
* Use `deterministic routing by primekey` to partition data to multi nodes
    * In Kafka-Cluster
        * Binlog will be out-of-order when go through cluster.
        * So the binlog of the same primekey should be sent to the same one kafka broker.
        * In global view, binlog are out-of-order.
        * All binlog of one (any) primekey, keep the right order.
    * In CH-Cluster
        * We need the same primekey data in a same node
        * So that aggregation pushdown can be done. IE: distinct
        * Implement in CHSql-Writer

## Data syncing speed
All performance info are based on rough benchmark, need more precise test.
* Binlog-Drainer is a singleton module, may cause performance problem (or not, TBD).
    * 50-500 MB/s
    * 1000000 Rows/s
* In new TiDB cluster
    * 1 TiDB Node: `< 10MB/s`, `< 50000 Rows/s`
    * One drainer can serve 20+ TiDB, should be good.
* Deploy syncing in a TiDB cluster with (lots of old) data
    * Current solution:
        * Generate drainer-save-point
        * Use MyDump to dump the whole data
        * Deploy syncing modules, generate binlog
        * Import dump-result to TheFlash
        * Sync binlog to TheFlash
    * New solution:
        * Mark the current largest committed ts in TiDB.
        * Dump snapshot(current-ts) to binlog
            * Use TiDB, or TiSpark, mydump, etc.
        * Deploy syncing modules, filter binlog before current-ts, sync to TheFlash
        * Faster, and avoid (dump-result) sql parsing
    * Both can be operated online.
