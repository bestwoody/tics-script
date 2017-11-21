# CH-Connector: Dev on ClickHouse

## Dir notes:
* `clickhouse`
    * The official repo
    * Git submodule of `theflash` repo
* `delta`
    * Patch source code
    * `clickhouse` + `delta` = `CH-Connector`
* `running`
    * Running env
    * Test data set, configs, etc

## Dev on ClickHouse and keep syncing with the official repo
* See [dev-on-ch](./dev-on-ch.md)

## Progress
```
OK Codec
OK JNI
OK Types supported: Essential
-- Types supported: Full
     - DateTime
     - Bool
     - Decimal
     - Nested types
*- Benchmark
-- Optimization
     - Memory usage control
     - Disable background threads
     - Faster data copy
```
