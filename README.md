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
*---- Binlog Writer
***-- Data update supporting
----- Writing transaction(batch level) support
[Tests]
***** Features test
***-- Stability test
**--- Types system test
[Optimization]
***** Aggregation pushdown
***** Spark codegen
*---- Small table broadcast
----- External executor
```
POC:
```
***** Magic Protocol
***** CH-Magic Connector
***** Spark-Magic Connector
```
