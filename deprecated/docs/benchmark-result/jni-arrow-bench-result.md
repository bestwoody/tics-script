# JNI and Arrow Bench Result
* Hardware: `MacBookPro 15' 2015` `4 Phy Cores, 2.5GHZ` `16G Mem`

## JNI Call
* Test case: `int sum(int, int)`
* 7e7 TPS on 1 core

## JNI Memory IO Thoughput
* Alloc buffer in `.so`, (copy and) return bytes to java
* Test case: `byte[] alloc(int size)`
* 1KB per call, 1 core: 2GB/s
* 1MB per call, 1 core: 4GB/s

## JNI Arrow Simple Array Transfer
* Alloc arrow-array in `.so`, (copy and) return serialized bytes to java, no decoding
* Test case: `byte[] getInt64Array(int rows)`
* 1K rows per call, 1 core: 220MB/s, 28e6 rows/s
* 1M rows per call, 1 core: 520MB/s, 68e6 rows/s
* best perform: 500K row per call.

## JNI Arrow Simple Array Transfer and Decode
* Alloc arrow-array in `.so`, (copy and) return serialized bytes to java, and decode in java
* Test case: `byte[] getInt64Array(int rows)`
* 1K rows per call, 1 core: 130MB/s, 17e6 rows/s
* 1M rows per call, 1 core: 300MB/s, 39e6 rows/s
