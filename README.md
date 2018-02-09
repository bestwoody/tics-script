# The Flash
An OLAP project of TiDB

## Design
* [The big picture](./docs/the-big-picture.md)
* [The CH-Spark solution](./docs/ch-spark-tcp.md)
* [More docs](./docs)

## Progress
Online:
```
[Features]
**--- Fully test (IMPORTANT: types system)
----- Writing transaction(batch level) support
*---- Binlog syncer
*---- MyDump data importer
***-- Data update supporting
```
Further Features:
```
----- External executor
----- Shares storage
```

## Done jobs
* POC:
    * Magic Protocol
    * CH-Magic Connector
    * Spark-Magic Connector
* Optimizations:
    * Aggregation pushdown
    * Spark codegen
    * Small table broadcast
    * Session-module read
* Tests
    * Features test
    * Stability test
