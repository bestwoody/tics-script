# The Binary Protocol between Calculating and Storage

## Currently we use Arrow as codec, and TCP as IPC data transfer method
* We used JNI, now we think TCP is a better choice
    * JNI problems
        * Not fast(not slow, although), see: [JNI Benchmarks](./docs/jni-arrow-bench-result.md)
        * Hard to control java native memory usage.
    * When we use JNI, Latch Service must be a standalone service.
        * If use TCP, Latch Service can be integrade to CH process.

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
** Error Handling
-- Parrallel transport
-- Cancal query
-- Benchmark, TPS + Throughput
-- Query progress report
```
