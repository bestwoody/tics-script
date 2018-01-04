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
** Benchmark
** Streaming + parallel + async
** Types supporting: Essential
-- Types supporting: Full. Not supported yet: Bool, Decimal, Nested types
-- Optimization
```
JNI only (Deprecated)
```
-- Disable background threads
```
