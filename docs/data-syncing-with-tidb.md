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
