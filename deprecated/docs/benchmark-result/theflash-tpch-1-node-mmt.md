# TheFlash TPCH on 1 node, Parquet vs MergeTree vs MutableMergeTree

## Result

* Time unit: sec

* Comparation columns
    * Parquet:
        * Spark + Parquet
    * Origin:
        * Spark + CH
        * Original MergeTree engine, partitioning by `YYMM(date)`
    * SelRaw:
        * Spark + CH
        * MutableMergeTree engine, partitioning by `hash(primary key) % mod`
        * Use SelRaw instead of Select, allow duplicated primary key
    * Mutable
        * Spark + CH, Support Update/Delete
        * MutableMergeTree engine, partitioning by `hash(primary key) % mod`
        * Dedupcating algorithm: parallel ReplacingDeletingSortedInputStream(it's a simple priority queue)
    * A vs B
        * `+` faster
        * `-` slower

* Comparation result and why
    * `Origin > Parquet`
        * CH is faster
        * Origin is partitioned by date
    * `Origin > SelRaw`
        * Origin is partitioned by date, MutableMergeTree has no date index
    * `SelRaw > Mutable`
        * MutableMergeTree has extra `deduplicating on read` operation
    * `Origin > Mutable`
        * Origin is partitioned by date, MutableMergeTree has no date index
        * MutableMergeTree has extra `deduplicating on read` operation
        * Origin is IO bound (2G/s), MutableMergeTree is CPU bound (700-900M/s. TODO: optimized, faster now)

| Query    | Parquet | Origin  | SelRaw  | Mutable | Origin vs Parquet | Origin vs Mutable | Mutable vs Parquet |
| -------- | ------: | ------: | ------: | ------: | ----------------- | ----------------- | ------------------ |
| Q01      |  134.4  |   10.4  |    14.7 |    22.3 | ++++++++++++++++  | +++               | ++++++++++++++     |
| Q02      |   48.4  |   44.0  |    46.9 |    51.3 | +                 | +                 | -                  |
| Q03      |  223.0  |   70.1  |    76.5 |    86.7 | +++++++++++       | ++                | ++++               |
| Q04      |  335.0  |  299.1  |         |   344.3 | +                 | ++                | -                  |
| Q05      |  233.3  |  122.2  |   154.8 |   140.1 | ++++++++          | ++                | +++++++            |
| Q06      |  207.2  |    4.4  |     7.8 |    12.2 | +++++++++++++++++ | +++++             | +++++++++++++++++  |
| Q07      |  260.8  |   90.4  |   102.8 |   114.3 | +++++++++++       | +++               | +++++++++          |
| Q08      |  162.1  |  132.1  |   177.8 |   146.9 | ++++              | ++                | +                  |
| Q09      |  150.3  |  176.1  |   177.5 |   191.0 | -                 | ++                | --                 |
| Q10      |   99.2  |   56.9  |    63.1 |    79.7 | ++++++++          | ++                | ++                 |
| Q11      |   35.0  |   18.2  |    19.9 |    25.2 | ++++++++          | ++                | +                  |
| Q12      |   45.7  |   36.8  |    46.1 |    64.4 | ++                | +++               | ---                |
| Q13      |   57.0  |   57.1  |    59.8 |    66.0 | EQUAL             | +                 | -                  |
| Q14      |  188.0  |   13.0  |    25.7 |    35.4 | +++++++++++++++++ | +++++             | ++++++++++         |
| Q19      |   29.2  |   53.2  |    56.0 |    66.4 | ----              | +++               | ----------         |
| Q22      |   76.8  |   88.4  |    92.4 |   102.6 | -                 | +                 | -                  |


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
* CHSpark: partitions=16, decoders=1, encoders=16
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
* MutableMergeTree: rdd-partitions=16, decoders=1, encoders=16, storage-partitions=16
```
Q01, avg:  22.3, detail: [23.2, 22.4, 24.2, 20.3, 21.5]
Q02, avg:  51.3, detail: [51.9, 52.3, 49.8, 51.7, 50.9]
Q03, avg:  86.7, detail: [86.0, 85.9, 85.5, 86.6, 89.4]
Q04, avg: 344.3, detail: [348.0, 343.6, 341.8, 341.4, 346.6]
Q05, avg: 140.1, detail: [135.6, 137.8, 150.1, 139.1, 137.7]
Q06, avg:  12.2, detail: [12.3, 12.3, 12.0, 12.0, 12.4]
Q07, avg: 114.3, detail: [117.9, 115.3, 111.6, 114.2, 112.4]
Q08, avg: 146.9, detail: [142.2, 160.6, 148.6, 142.6, 140.4]
Q09, avg: 191.0, detail: [180.7, 186.7, 183.8, 193.1, 210.8]
Q10, avg:  79.7, detail: [75.4, 83.0, 78.8, 84.2, 77.1]
Q11, avg:  25.2, detail: [24.5, 26.1, 25.3, 24.2, 26.1]
Q12, avg:  64.4, detail: [59.3, 70.4, 60.0, 67.6, 64.8]
Q13, avg:  66.0, detail: [68.1, 68.6, 62.2, 64.7, 66.6]
Q14, avg:  35.4, detail: [41.6, 32.6, 32.4, 32.5, 38.0]
Q16, avg: 134.6, detail: [151.3, 132.1, 131.5, 118.3, 140.0]
Q19, avg:  66.4, detail: [67.2, 63.5, 61.7, 63.5, 76.1]
Q22, avg: 102.6, detail: [101.8, 106.9, 106.4, 95.6, 102.5]
```

* SelRaw on MutableMergeTree: rdd-partitions=16, decoders=1, encoders=16, storage-partitions=16
```
Q01, avg:  14.7, detail: [14.3, 14.6, 14.5, 15.3]
Q02, avg:  46.9, detail: [50.8, 45.9, 45.6, 45.2]
Q03, avg:  76.5, detail: [75.6, 78.2, 78.1, 74.2]
Q05, avg: 154.8, detail: [216.5, 139.1, 129.6, 134.1]
Q06, avg:   7.8, detail: [6.8, 8.1, 7.9, 8.5]
Q07, avg: 102.8, detail: [106.6, 97.8, 98.9, 107.8]
Q08, avg: 177.8, detail: [223.6, 215.6, 136.4, 135.4]
Q09, avg: 177.5, detail: [174.7, 180.3]
Q10, avg:  63.1, detail: [60.8, 67.1, 61.0, 63.4]
Q11, avg:  19.9, detail: [19.9, 21.4, 18.8, 19.4]
Q12, avg:  46.1, detail: [50.6, 44.3, 43.4]
Q13, avg:  59.8, detail: [57.5, 62.5, 59.3]
Q14, avg:  25.7, detail: [22.3, 31.2, 23.7]
Q16, avg: 124.9, detail: [122.5, 121.0, 131.3]
Q19, avg:  56.0, detail: [55.6, 57.3, 55.1]
Q22, avg:  92.4, detail: [93.0, 94.0, 90.2]
```

## Environment

### Hardware
* Intel Xeon E312xx 16 cores 2.4Ghz.
* 16G Memory 1600MHz .
* SSD 2.0GB+/s.

### Software
* CentOS Linux release 7.2.1511
* Spark version 2.1.1
* CH server version 1.1.54310

### Spark
* SPARK EXECUTOR MEMORY=12G
* SPARK WORKER MEMORY=12G
* SPARK WORKER CORES=16

## TheFlash
* MutableMergeTree partition number: 16
* Pushdown=true
* Codegen=true
* Broadcast: tables bigger than 160k rows
* CH memory limit: 2G

### Data Set
* TPCH-100 100G data scala
