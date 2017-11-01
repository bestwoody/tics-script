# JNI and Arrow Bench Result
* Hardware: `MacBookPro 15' 2015` `4 Phy Cores, 2.5GHZ` `16G Mem`

## JNI Call
* Test cast: `int sum(int, int)`
* 70000000 TPS on 1 core

## JNI Memory IO Thoughput
* Alloc buffer in `.so`, (copy and) return bytes to java
* Test cast: `byte[] alloc(int size)`
* 1KB per call, 1 core: 2GB/s
* 1MB per call, 1 core: 4GB/s

## JNI Arrow Simple Array Transfer
* Alloc arrow-arry in `.so`, (copy and) return serialized bytes to java, no decoding
* Test cast: `byte[] getInt64Array(int rows)`
* 1K rows per call, 1 core: 225MB/s, 2e7 rows/s
* 1M rows per call, 1 core: 500MB/s, 1e8 rows/s
* best perform: 500K row per call.
