# TheFlash TPCH on 1 node, pushdown + codegen + broadcast

## Result

* Time unit: sec
* Run 20 times of each query
    * Discard a few unstable result (cause by network retry)
    * Calculate avg result

| Test Type                      | Case    | Time   | Faster |
| --------                       | -----:  | -----: | :----- |
| Spark Parquet                  | Q1      | 122.00 |        |
| TheFlash                       | Q1      |   9.61 | ====== |
| Spark Parquet                  | Q2      |  53.50 |        |
| TheFlash                       | Q2      |  44.66 | ==     |
| Spark Parquet                  | Q3      | 177.00 |        |
| TheFlash                       | Q3      |  74.09 | =====  |
| Spark Parquet                  | Q4      | 332.00 |        |
| TheFlash                       | Q4      | 358.45 | SLOWER |
| Spark Parquet                  | Q5      | 227.50 |        |
| TheFlash                       | Q5      | 124.07 | ===    |
| Spark Parquet                  | Q6      |   3.50 |        |
| TheFlash                       | Q6      |   4.58 |        |
| Spark Parquet                  | Q7      | 305.00 |        |
| TheFlash                       | Q7      |  92.62 | =====  |
| Spark Parquet                  | Q8      | 159.00 |        |
| TheFlash                       | Q8      | 129.35 | =      |
| Spark Parquet                  | Q9      | 156.50 |        |
| TheFlash                       | Q9      | 180.02 | SLOWER |
| Spark Parquet                  | Q10     |  69.00 |        |
| TheFlash                       | Q10     |  62.03 | =      |
| Spark Parquet                  | Q11     |  32.50 |        |
| TheFlash                       | Q11     |  19.64 | ====   |
| Spark Parquet                  | Q12     |  45.00 |        |
| TheFlash                       | Q12     |  39.87 | =      |
| Spark Parquet                  | Q13     |  78.00 |        |
| TheFlash                       | Q13     |  56.82 | ==     |
| Spark Parquet                  | Q14     | 206.00 |        |
| TheFlash                       | Q14     |  13.94 | ====== |
| Spark Parquet                  | Q16     | 151.50 |        |
| TheFlash                       | Q16     | 138.44 | =      |
| Spark Parquet                  | Q19     |  36.50 |        |
| TheFlash                       | Q19     |  54.78 | SLOWER |
| Spark Parquet                  | Q20     | 267.00 |        |
| TheFlash                       | Q20     |        |        |
| Spark Parquet                  | Q22     |  72.5  |        |
| TheFlash                       | Q22     |        |        |


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
* Decoders=2
* Encoders=8
* Pushdown=true
* Codegen=true

### Data Set
* TPCH-100 100G data scala
