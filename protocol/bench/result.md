# JNI and Arrow Bench Result
* Hardware: `MacBookPro 15' 2015` `4 Phy Cores` `16G Mem`

## JNI Call
* Test cast: `int sum(int, int)`
* 70000000 TPS on 1 core

## JNI Memory IO Thoughput
* Alloc buffer in `.so`, (copy and) return bytes to java
* Test cast: `byte[] alloc(int size)`
* 1KB per call: 2GB/s on 1 core
* 1MB per call: 4GB/s on 1 core
