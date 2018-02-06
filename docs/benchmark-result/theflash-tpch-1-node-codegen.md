# TheFlash TPCH on 1 node, pushdown + codegen + broadcast

## Result

* Time unit: sec
* Run 20 times of each query
    * Discard a few unstable result (cause by network retry)
    * Calculate avg result
* Comparation
    * Parquet: Spark + Parquet
    * CHSpark: Spark + CH, pushdown, codegen, broadcast

| Query    | Parquet | CHSpark | Comment         | Faster |
| -------- | ------: | ------: | :-------------- | :----- |
| Q01      |  134.4  |   10.4  |                 | ====== |
| Q02      |   48.4  |   44.0  |                 | =      |
| Q03      |  223.0  |   70.1  |                 | =====  |
| Q04      |  335.0  |  299.1  | Spark unstable  | =      |
| Q05      |  233.3  |  122.2  |                 | ===    |
| Q06      |  207.2  |    4.4  |                 | ====== |
| Q07      |  260.8  |   90.4  |                 | =====  |
| Q08      |  162.1  |  132.1  |                 | =      |
| Q09      |  150.3  |  176.1  |                 | SLOWER |
| Q10      |   99.2  |   56.9  |                 | ==     |
| Q11      |   35.0  |   18.2  |                 | ===    |
| Q12      |   45.7  |   36.8  |                 | =      |
| Q13      |   57.0  |   57.1  |                 |        |
| Q14      |  188.0  |   13.0  |                 | ====== |
| Q16      |  152.5  |  166.9  | Spark unstable  | =      |
| Q19      |   29.2  |   53.2  |                 | SLOWER |
| Q20      |  268.4  |         | Spark unstable  |        |
| Q22      |   76.8  |   88.4  |                 | SLOWER |


## Raw result data
* Parquet: default config
```
Q01, avg: 134.4, detail: [117.4, 167.3, 144.2, 116.3, 119.1, 141.9]
Q02, avg:  48.4, detail: [48.0, 50.6, 48.8, 49.5, 45.8, 47.9]
Q03, avg: 223.7, detail: [291.6, 209.5, 192.1, 210.7, 224.6, 213.7]
Q04, avg: 335.0, detail: [293.3, 340.3, 371.4]
Q05, avg: 233.3, detail: [198.3, 229.3, 217.8, 215.6, 209.0, 329.9]
Q06, avg: 207.2, detail: [223.0, 229.4, 191.2, 166.8, 211.5, 221.3]
Q07, avg: 260.8, detail: [254.2, 259.7, 253.3, 276.0]
Q08, avg: 162.1, detail: [185.8, 157.0, 150.7, 165.4, 162.5, 151.4]
Q09, avg: 194.4, detail: [150.6, 151.1, 145.6, 154.0]
Q10, avg:  99.2, detail: [94.2, 101.1, 106.7, 93.9, 95.3, 104.3]
Q11, avg:  35.0, detail: [34.7, 35.2, 34.2, 36.8, 36.9, 32.4]
Q12, avg:  45.7, detail: [42.7, 44.7, 45.4, 43.7, 42.7, 54.9]
Q13, avg:  57.1, detail: [57.2, 54.6, 57.6, 60.7, 55.1, 57.1]
Q14, avg: 188.0, detail: [172.2, 193.8, 194.5, 170.6, 232.2, 165.0]
Q16, avg: 152.3, detail: [139.4, 142.8, 166.2, 122.2, 219.4, 124.0]
Q17, avg: 172.3, detail: [171.4, 168.9, 162.1, 182.4, 166.9, 182.3]
Q18, avg: 197.4, detail: [178.2, 181.2, 293.7, 182.5, 166.0, 182.9]
Q19, avg:  29.2, detail: [27.5, 36.1, 27.4, 29.0, 25.7, 29.5]
Q20, avg: 268.4, detail: [233.2, 234.8, 243.5, 299.3, 256.4, 343.5]
Q22, avg:  76.8, detail: [67.3, 72.9, 77.2, 77.7, 81.4, 84.0]
```
* CHSpark:
```
Q01, avg:  10.4, detail: [9.7, 10.4, 10.7, 10.6, 10.2, 10.7]
Q02, avg:  44.0, detail: [42.7, 44.6, 45.5, 44.8, 43.6, 42.8]
Q03, avg:  70.1, detail: [66.2, 71.6, 73.0, 71.2, 67.0, 71.6]
Q04, avg: 299.1, detail: [295.7, 292.3, 296.3, 296.5, 286.5, 327.2]
Q05, avg: 122.2, detail: [120.8, 121.6, 115.8, 130.0, 117.0, 127.9]
Q06, avg:   4.4, detail: [4.6, 4.5, 4.2, 4.4, 4.4, 4.5]
Q07, avg:  90.4, detail: [84.4, 84.8, 88.1, 97.7, 92.4, 95.0]
Q08, avg: 132.1, detail: [129.4, 143.7, 130.9, 126.9, 125.0, 136.7]
Q09, avg: 176.1, detail: [166.6, 190.3, 166.8, 178.4, 174.4, 179.9]
Q10, avg:  56.9, detail: [60.2, 57.1, 57.4, 52.9, 56.1, 57.7]
Q11, avg:  18.2, detail: [18.9, 18.3, 18.6, 17.4, 18.3, 17.4]
Q12, avg:  36.8, detail: [37.1, 35.9, 35.6, 39.1, 37.6, 35.5]
Q13, avg:  57.0, detail: [54.2, 59.0, 57.6, 57.4, 57.9, 55.7]
Q14, avg:  13.0, detail: [13.5, 13.1, 13.0, 13.3, 12.2, 13.0]
Q16, avg: 192.3, detail: [183.4, 150.5, 243.1]
Q19, avg:  53.2, detail: [51.7, 64.6, 49.9, 50.1, 50.9, 52.3]
Q22, avg:  88.4, detail: [87.8, 90.4, 80.4, 91.3, 100.1, 80.5]
```

## Environment

### Hardware
* Intel Xeon E312xx 16 cores 2.4Ghz.
* 16G Memory 1600MHz .
* SSD 2.0GB+/s.

### Software
* CentOS Linux release 7.2.1511
* Spark version 2.1.1
* ClickHouse server version 1.1.54310

### Spark
* SPARK EXECUTOR MEMORY=12G
* SPARK WORKER MEMORY=12G
* SPARK WORKER CORES=16

## TheFlash
* Partitions=16
* Decoders=4
* Encoders=8
* Pushdown=true
* Codegen=true
* Broadcast: tables bigger than 160k rows

### Data Set
* TPCH-100 100G data scala
