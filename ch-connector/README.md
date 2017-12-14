# CH-Connector: Dev on ClickHouse

## Dir notes:
* `clickhouse`
    * The official repository
    * Git submodule of `theflash` repository
* `delta`
    * Patch source code
    * `clickhouse` + `delta` = `CH-Connector`
* `running`
    * Running env
    * Test data set, configs, etc

## Dev on ClickHouse and keep syncing with the official repository
* See [dev-on-ch](./dev-on-ch.md)

## Progress
```
** Codec
** IPC (JNI, TCP Server)
** Types supported: Essential
-- Types supported: Full
     - Bool
     - Decimal
     - Nested types
-- Benchmark
-- Optimization
     - Faster data copy
     - Streaming + parallel + async
```
JNI only (suspended by now)
```
-- Optimization
     - Memory usage control
     - Disable background threads
```
