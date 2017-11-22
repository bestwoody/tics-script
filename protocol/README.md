# The Binary Protocol between Calculating and Storage

## Currently we use Arrow as codec, and JNI as IPC data transfer method
* May switch to TCP later.

## Progress
JNI
```
** Codec
** JNI
** Error Handling
** Cancal query
-- Query progress report
** Benchmark, TPS + Throughput
```
TCP
```
** Codec
** TCP
-- Error Handling
-- Benchmark, TPS + Throughput
-- Cancal query
-- Query progress report
```
