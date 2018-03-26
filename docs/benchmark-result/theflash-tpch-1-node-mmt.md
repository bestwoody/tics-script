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
        * MutableMergeTree engine, partitioning by `hash(primary key) / mod`
        * Use SelRaw instead of Select, allow duplicated primary key
    * Mutable
        * Spark + CH, Support Update/Delete
        * MutableMergeTree engine, partitioning by `hash(primary key) / mod`
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
        * Origin is IO bond (2G/s), MutableMergeTree is CPU bound (700-900M/s)

| Query    | Parquet | Origin  | SelRaw  | Mutable | Origin vs Parquet | Origin vs Mutable | Mutable vs Parquet |
| -------- | ------: | ------: | ------: | ------: | ----------------- | ----------------- | ------------------ |
| Q01      |  134.4  |   10.4  |    14.7 |    34.6 | ++++++++++++++++  | +++               | ++++++++++++++     |
| Q02      |   48.4  |   44.0  |    46.9 |    56.7 | +                 | +                 | -                  |
| Q03      |  223.0  |   70.1  |    76.5 |   117.6 | +++++++++++       | ++                | ++++               |
| Q04      |  335.0  |  299.1  |         |         | +                 | NO DATA           | NO DATA            |
| Q05      |  233.3  |  122.2  |   154.8 |   177.3 | ++++++++          | ++                | +++++++            |
| Q06      |  207.2  |    4.4  |     7.8 |    24.6 | +++++++++++++++++ | +++++             | +++++++++++++++++  |
| Q07      |  260.8  |   90.4  |   102.8 |   144.3 | +++++++++++       | +++               | +++++++++          |
| Q08      |  162.1  |  132.1  |   177.8 |   172.3 | ++++              | ++                | -                  |
| Q09      |  150.3  |  176.1  |   177.5 |   231.2 | -                 | ++                | --                 |
| Q10      |   99.2  |   56.9  |    63.1 |   110.7 | ++++++++          | ++                | -                  |
| Q11      |   35.0  |   18.2  |    19.9 |    29.5 | ++++++++          | ++                | +                  |
| Q12      |   45.7  |   36.8  |    46.1 |    93.9 | ++                | +++               | ---                |
| Q13      |   57.0  |   57.1  |    59.8 |    64.5 | EQUAL             | +                 | -                  |
| Q14      |  188.0  |   13.0  |    25.7 |    59.6 | +++++++++++++++++ | +++++             | ++++++++++         |
| Q19      |   29.2  |   53.2  |    56.0 |    92.5 | ----              | +++               | ----------         |
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
Q01, avg:  34.6, detail: [30.6, 31.5, 31.7, 33.7, 41.1, 40.8, 33.2, 40.3, 32.5, 33.5, 34.4, 34.5, 35.3, 34.1, 34.7, 32.6, 33.5]
Q02, avg:  56.7, detail: [55.7, 58.7, 58.0, 56.3, 56.1, 56.2, 57.6, 55.1, 55.6, 55.6, 57.8, 58.3, 56.9, 54.3, 56.2, 57.4, 57.7]
Q03, avg: 117.6, detail: [117.5, 122.7, 115.0, 118.9, 119.2, 118.2, 115.9, 117.3, 111.8, 117.3, 120.0, 117.2, 117.7, 118.4, 116.6, 119.1, 116.3]
Q05, avg: 177.3, detail: [166.6, 157.1, 173.2, 165.4, 169.1, 161.9, 293.0, 171.2, 167.6, 165.3, 169.8, 167.0]
Q06, avg:  24.6, detail: [22.0, 22.7, 28.2, 23.7, 23.0, 32.1, 29.8, 22.7, 24.1, 24.2, 23.5, 24.4, 23.9, 23.5, 23.5, 23.6, 22.7]
Q07, avg: 144.3, detail: [150.2, 142.7, 148.4, 143.8, 145.9, 143.7, 151.8, 144.9, 141.9, 138.6, 136.9, 150.7, 143.7, 140.4, 143.7, 141.6, 144.8]
Q08, avg: 172.3, detail: [170.5, 176.3, 167.8, 169.6, 169.2, 172.6, 165.8, 171.5, 176.5, 173.9, 179.6, 169.7, 171.1, 177.8]
Q09, avg: 231.2, detail: [231.3, 230.2, 230.3, 234.1, 229.9]
Q10, avg: 110.7, detail: [108.7, 113.7, 112.9, 112.6, 106.9, 104.3, 110.0, 108.8, 111.9, 115.5, 110.2, 110.7, 107.4, 113.3, 112.2, 112.3]
Q11, avg:  29.5, detail: [29.0, 29.5, 29.2, 29.9, 29.1, 29.5, 29.6, 29.6, 30.2, 29.6, 30.1, 28.1, 29.7, 30.3, 29.9, 29.3]
Q12, avg:  93.9, detail: [94.0, 95.2, 94.5, 93.4, 93.5, 94.0, 92.8, 92.6, 93.6, 92.4, 94.9, 92.8, 94.8, 93.3, 96.5, 94.3]
Q13, avg:  64.5, detail: [63.2, 65.5, 63.9, 67.0, 63.6, 64.8, 68.6, 62.7, 64.0, 64.4, 63.4, 64.2, 65.2, 66.0, 60.4, 64.3]
Q14, avg:  59.6, detail: [58.3, 61.8, 59.5, 60.4, 59.9, 58.8, 57.9, 61.9, 60.3, 58.5, 60.2, 59.6, 59.7, 58.5, 59.5, 58.4]
Q16, avg: 124.9, detail: [119.7, 132.8, 125.6, 136.0, 131.5, 124.0, 118.9, 122.6, 117.1, 121.1, 124.9]
Q19, avg:  92.5, detail: [96.8, 90.7, 92.3, 91.5, 93.5, 89.6, 93.1, 90.7, 92.4, 91.4, 92.4, 94.3, 91.2, 93.4, 94.1, 92.1]
Q22, avg: 102.6, detail: [109.1, 100.1, 101.1, 106.0, 94.5, 105.1, 103.2, 101.1, 104.3, 109.8, 96.7, 104.9, 100.4, 96.9, 103.0, 105.1]
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
* ClickHouse server version 1.1.54310

### Spark
* SPARK EXECUTOR MEMORY=12G
* SPARK WORKER MEMORY=12G
* SPARK WORKER CORES=16

## TheFlash
* MutableMergeTree partition number: 16
* Pushdown=true
* Codegen=true
* Broadcast: tables bigger than 160k rows

### Data Set
* TPCH-100 100G data scala
