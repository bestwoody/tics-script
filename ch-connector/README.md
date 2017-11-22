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
** Types supported: Essential
-- Types supported: Full
     - DateTime
     - Bool
     - Decimal
     - Nested types
-- Benchmark
-- Optimization
     - Faster data copy
```
JNI only (suspended by now)
```
** JNI
-- Optimization
     - Memory usage control
     - Disable background threads
```
